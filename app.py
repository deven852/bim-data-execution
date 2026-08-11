#!/usr/bin/env python3
"""
BIM Data Execution - web dashboard (v4, fully debugged)
"""

import os
import time
import uuid
import threading
import datetime
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

from flask import (Flask, request, jsonify, send_file, redirect, url_for,
                   render_template_string, session, abort)
from werkzeug.utils import secure_filename

import bim_data_execution as core

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", APP_DIR)
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
OUTPUT_DIR = os.path.join(DATA_DIR, "outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
app.secret_key = os.environ.get("APP_SECRET", "change-me-" + uuid.uuid4().hex)

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

def login_required(fn):
    @wraps(fn)
    def wrapper(*a, **k):
        if APP_PASSWORD and not session.get("authed"):
            if request.path.startswith("/status") or request.path.startswith("/master-stats"):
                return jsonify(error="Not authenticated"), 401
            return redirect(url_for("login"))
        return fn(*a, **k)
    return wrapper

JOBS = {}
JOBS_LOCK = threading.Lock()

def _run_with_timeout(fn, timeout_s, *args, **kwargs):
    """Run fn in a daemon thread and give up if it takes longer than timeout_s.
    Returns fn's result, or raises TimeoutError. Does NOT wait for the stuck
    thread to finish — abandons it as a daemon so the main flow can continue."""
    result = [None]
    error = [None]
    done_event = threading.Event()

    def _target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as e:
            error[0] = e
        finally:
            done_event.set()

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    finished = done_event.wait(timeout=timeout_s)
    if not finished:
        # Abandon the thread as daemon - it dies when process exits
        raise TimeoutError(f"{fn.__name__} exceeded {timeout_s}s (thread abandoned)")
    if error[0] is not None:
        raise error[0]
    return result[0]



# ---- Watched-folder worker ----
WATCH_LOG = []
WATCH_LOCK = threading.Lock()
WATCH_INTERVAL = int(os.environ.get("WATCH_INTERVAL_SECONDS", "60"))
_SCAN_LOCK = threading.Lock()
_LAST_SCAN = [0.0]
_WATCHER_THREAD = [None]

def _watch_log(msg):
    with WATCH_LOCK:
        WATCH_LOG.append(msg)
        del WATCH_LOG[:-200]
    print(msg, flush=True)

def run_scan(reason="timer"):
    api_key = os.environ.get("SEAMLESS_API_KEY", "")
    if not (core.watch_enabled() and api_key):
        return
    if not _SCAN_LOCK.acquire(blocking=False):
        return
    try:
        _LAST_SCAN[0] = time.time()
        found = core.scan_input_folder_once(api_key, logfn=_watch_log)
        found2 = core.scan_universal_once(api_key, logfn=_watch_log)
        if not found and not found2:
            _watch_log(f"[auto] checked Input folder ({reason}) - nothing new.")
    except Exception as e:
        _watch_log(f"[auto] scan error ({reason}): {e}")
    finally:
        _SCAN_LOCK.release()

def _watcher_loop():
    _watch_log(f"[auto] Watcher started. Checking Input folder every {WATCH_INTERVAL}s.")
    while True:
        run_scan(reason="timer")
        time.sleep(WATCH_INTERVAL)

def ensure_watcher():
    if not (core.watch_enabled() and os.environ.get("SEAMLESS_API_KEY")):
        return
    t = _WATCHER_THREAD[0]
    if t is None or not t.is_alive():
        nt = threading.Thread(target=_watcher_loop, daemon=True)
        nt.start()
        _WATCHER_THREAD[0] = nt

ensure_watcher()


# =============================================================================
# JOB RUNNER
# =============================================================================

def run_job(job_id, input_path, api_key, cfg):
    """Process a company file. ALWAYS sets status=done or status=error before returning."""

    def update(**kw):
        with JOBS_LOCK:
            JOBS[job_id].update(kw)

    def add_log(line):
        print(f"[job {job_id}] {line}", flush=True)
        with JOBS_LOCK:
            JOBS[job_id]["log"].append(line)

    out_path = os.path.join(OUTPUT_DIR, f"{'preview' if cfg['preview'] else 'results'}_{job_id}.xlsx")
    drive_link = None

    # Everything in one big try/finally so the job ALWAYS finishes
    try:
        # Step 1: build headers and load company list
        add_log("Starting job - loading company list...")
        try:
            headers = core.auth_headers(api_key)
            all_companies = core.load_companies(input_path)
        except Exception as e:
            add_log(f"ERROR loading file: {e}")
            update(status="error", error=str(e))
            return

        add_log(f"Loaded {len(all_companies)} companies from file.")

        # Enforce 1-200 companies per file (Starter plan safety cap)
        MIN_COMPANIES = 1
        MAX_COMPANIES = 200
        if len(all_companies) < MIN_COMPANIES:
            add_log(f"ERROR: File must contain at least {MIN_COMPANIES} company.")
            update(status="error", error=f"File must contain at least {MIN_COMPANIES} company.")
            return
        if len(all_companies) > MAX_COMPANIES:
            add_log(f"ERROR: File contains {len(all_companies)} companies. Maximum is {MAX_COMPANIES} per file.")
            update(status="error", error=f"File contains {len(all_companies)} companies. Split into batches of {MAX_COMPANIES} max.")
            return

        start = max(1, cfg["start"])
        companies = all_companies[start - 1:]
        if cfg["limit"]:
            companies = companies[:cfg["limit"]]
        preview = cfg["preview"]
        mode = "PREVIEW (free, no credits)" if preview else "RUN (uses credits)"

        if not companies:
            add_log("ERROR: No companies found in the uploaded file. "
                    "Make sure it has a column named 'Company'.")
            update(status="error", error="No companies found in file.")
            return

        update(status="running", total=len(companies), current=0,
               found=0, nomatch=0, skipped=0, errors=0,
               contacts=0, cached=0, preview=preview)

        add_log(f"{mode}. {len(companies)} companies, "
                f"up to {cfg['max_contacts']} contacts each, "
                f"{cfg['workers']} at a time.")

        # Step 2 & 3: Drive operations (skip ALL if any one fails/times out)
        drive_healthy = core.drive_enabled()
        if drive_healthy and not preview:
            try:
                pulled = _run_with_timeout(core.sync_master_before_run, 15)
                if pulled:
                    add_log(f"Loaded {pulled} contacts from permanent master in Drive.")
                else:
                    add_log("Drive master checked - no new contacts to pull.")
            except Exception as de:
                add_log(f"Drive master sync failed - skipping all Drive operations this run: {de}")
                drive_healthy = False

        if drive_healthy:
            try:
                stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                drive_name = f"INPUT_{stamp}_{os.path.basename(input_path)}"
                link = _run_with_timeout(core.drive_upload, 15, input_path, drive_name=drive_name)
                if link:
                    add_log(f"Input file saved to Drive: {link}")
            except Exception as de:
                add_log(f"Drive input upload failed - skipping remaining Drive ops: {de}")
                drive_healthy = False

        add_log(f"Starting parallel processing of {len(companies)} companies...")

        # Step 4: process companies in parallel
        results_by_company = {}
        counters = {"done": 0, "found": 0, "nomatch": 0,
                    "errors": 0, "contacts": 0, "cached": 0}

        def work(item):
            company, domain = item
            add_log(f"      [worker] START: {company} (domain={domain!r})")
            try:
                rows, kind = core.process_company(
                    company, headers,
                    search_limit=cfg["search_limit"],
                    poll_interval=cfg["poll_interval"],
                    poll_attempts=cfg["poll_attempts"],
                    min_rank=cfg["min_rank"],
                    max_contacts=cfg["max_contacts"],
                    preview=preview,
                    company_domain=domain)
                add_log(f"      [worker] DONE: {company} -> kind={kind}, rows={len(rows)}")
                return company, rows, kind
            except Exception as e:
                add_log(f"      [worker] FAIL: {company} -> {type(e).__name__}: {e}")
                return company, [core.note_row(company, f"ERROR: {e}")], "error"

        # 180s hard cap per company - a stuck one can never freeze the job
        PER_COMPANY_TIMEOUT = int(os.environ.get("PER_COMPANY_TIMEOUT", "180"))
        # NOTE: do NOT use `with ThreadPoolExecutor` - the `with` block waits for
        # all threads to finish on exit, which deadlocks if any thread is stuck.
        # We create the executor, use it, then abandon it (workers are daemons).
        ex = ThreadPoolExecutor(max_workers=cfg["workers"])
        try:
            futures = {ex.submit(work, item): item[0] for item in companies}
            for fut in as_completed(futures, timeout=None):
                company_name = futures[fut]
                try:
                    company, rows, kind = fut.result(timeout=PER_COMPANY_TIMEOUT)
                except FuturesTimeout:
                    add_log(f"[timeout] {company_name} took over {PER_COMPANY_TIMEOUT}s - skipped.")
                    company = company_name
                    rows = [core.note_row(company_name, f"TIMEOUT after {PER_COMPANY_TIMEOUT}s - Seamless did not respond.")]
                    kind = "error"
                except Exception as e:
                    add_log(f"[error] {company_name} failed: {e}")
                    company = company_name
                    rows = [core.note_row(company_name, f"ERROR: {e}")]
                    kind = "error"
                results_by_company[company] = rows
                counters["done"] += 1

                if kind == "found":
                    real = [r for r in rows if r.get("First Name") or r.get("Job Title")]
                    counters["found"] += 1
                    counters["contacts"] += len(real)
                elif kind == "cached":
                    real = [r for r in rows if r.get("First Name")]
                    counters["found"] += 1
                    counters["cached"] += len(real)
                elif kind == "error":
                    counters["errors"] += 1
                else:
                    counters["nomatch"] += 1

                update(current=counters["done"],
                       found=counters["found"],
                       nomatch=counters["nomatch"],
                       errors=counters["errors"],
                       contacts=counters["contacts"],
                       cached=counters["cached"],
                       company=company)

                n = len([r for r in rows if r.get("First Name")])
                src_tag = " (from cache - free)" if kind == "cached" else ""
                add_log(f"[{counters['done']}/{len(companies)}] {company} - "
                        f"{n if n else 'no'} contact(s) "
                        f"{'previewed' if preview else 'found'}{src_tag}")

                # Incremental save after each company
                ordered = []
                for c, _ in companies:
                    if c in results_by_company:
                        ordered.extend(results_by_company[c])
                try:
                    core.write_xlsx(ordered, out_path)
                except Exception:
                    pass
        finally:
            # Don't wait for stuck workers to finish - shutdown non-blocking
            ex.shutdown(wait=False)

        # Step 5: final ordered write
        ordered = []
        for c, _ in companies:
            ordered.extend(results_by_company.get(c, []))
        core.write_xlsx(ordered, out_path)

        # Step 6: upload output to Drive (only if Drive was healthy earlier)
        if drive_healthy:
            try:
                stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                tag = "PREVIEW" if preview else "RESULTS"
                drive_link = _run_with_timeout(core.drive_upload, 20, out_path, drive_name=f"{tag}_{stamp}.xlsx")
                if drive_link:
                    add_log(f"Output saved to Drive: {drive_link}")
            except Exception as de:
                add_log(f"Drive output upload skipped: {de}")
        elif core.drive_enabled():
            add_log("Skipping Drive output upload - Drive was unhealthy this run.")

        # Step 7: push master back to Drive (only if Drive was healthy earlier)
        if drive_healthy and not preview:
            try:
                mlink = _run_with_timeout(core.sync_master_after_run, 20)
                if mlink:
                    add_log(f"Permanent master updated in Drive: {mlink}")
            except Exception as de:
                add_log(f"Drive master push skipped: {de}")

        # Summary
        if preview:
            add_log(f"Preview complete. ~{counters['contacts']} contacts would be researched "
                    f"(est. ~{counters['contacts']} credits) across {counters['found']} companies. "
                    f"{counters['nomatch']} no match. NOTHING SPENT.")
        else:
            add_log(f"Finished. {counters['contacts']} newly researched + "
                    f"{counters['cached']} reused from cache "
                    f"(saved ~{counters['cached']} credits) across {counters['found']} companies. "
                    f"{counters['nomatch']} no match, {counters['errors']} errors.")

        update(status="done", output=out_path, company="", drive_link=drive_link)

    except Exception as e:
        # Catch any unexpected crash, log it clearly
        add_log(f"ERROR: Unexpected crash: {e}")
        update(status="error", error=str(e), company="")

    finally:
        # ABSOLUTE SAFETY NET: no matter what happened above,
        # if status is still "running" force it to "done" so the browser stops polling.
        with JOBS_LOCK:
            if JOBS[job_id]["status"] == "running":
                JOBS[job_id]["status"] = "done"
                JOBS[job_id]["company"] = ""
                JOBS[job_id]["output"] = out_path
                JOBS[job_id]["drive_link"] = drive_link


# =============================================================================
# AUTH PAGES
# =============================================================================

LOGIN_PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>BIM Data Execution - Sign in</title>
<style>body{font-family:system-ui,sans-serif;background:#eef1f4;display:flex;min-height:100vh;
align-items:center;justify-content:center;margin:0}.box{background:#fff;border:1px solid #d3d9e0;
border-radius:4px;padding:34px;width:320px;box-shadow:0 10px 30px -20px rgba(0,0,0,.4)}
h1{font-size:20px;margin:0 0 4px}p{color:#69727d;font-size:13px;margin:0 0 20px}
input{width:100%;padding:11px;border:1px solid #d3d9e0;border-radius:3px;font-size:14px;box-sizing:border-box}
button{width:100%;margin-top:14px;padding:12px;border:0;border-radius:3px;background:#141a20;color:#fff;
font-size:14px;cursor:pointer}.err{color:#c0392b;font-size:13px;margin-top:10px}</style></head>
<body><form class="box" method="post"><h1>BIM Data Execution</h1><p>Enter the team password to continue.</p>
<input type="password" name="password" placeholder="Team password" autofocus>
<button type="submit">Sign in</button>
{% if error %}<div class="err">{{ error }}</div>{% endif %}</form></body></html>"""


# =============================================================================
# ROUTES
# =============================================================================

@app.route("/diag")
@login_required
def diag():
    try:
        return jsonify(core.diagnose())
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route("/ping")
def ping():
    ensure_watcher()
    if core.watch_enabled() and os.environ.get("SEAMLESS_API_KEY"):
        threading.Thread(target=lambda: run_scan(reason="ping"), daemon=True).start()
    return jsonify(ok=True, watching=core.watch_enabled(),
                   last_scan=int(_LAST_SCAN[0]), ts=int(time.time()))

@app.route("/watch-status")
@login_required
def watch_status():
    ensure_watcher()
    with WATCH_LOCK:
        return jsonify(enabled=core.watch_enabled(), interval=WATCH_INTERVAL,
                       cap=core.AUTO_MAX_COMPANIES, last_scan=int(_LAST_SCAN[0]),
                       log=WATCH_LOG[-80:])

@app.route("/scan-now", methods=["POST"])
@login_required
def scan_now():
    if not (core.watch_enabled() and os.environ.get("SEAMLESS_API_KEY")):
        return jsonify(error="Watcher not configured."), 400
    threading.Thread(target=lambda: run_scan(reason="manual"), daemon=True).start()
    return jsonify(ok=True)

@app.route("/login", methods=["GET", "POST"])
def login():
    if not APP_PASSWORD:
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["authed"] = True
            return redirect(url_for("index"))
        error = "Incorrect password."
    return render_template_string(LOGIN_PAGE, error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_required
def index():
    return render_template_string(PAGE,
                                  api_key_set=bool(os.environ.get("SEAMLESS_API_KEY")),
                                  default_limit=core.SEARCH_LIMIT,
                                  default_interval=core.POLL_INTERVAL_SECONDS,
                                  default_attempts=core.POLL_MAX_ATTEMPTS,
                                  default_maxc=core.MAX_CONTACTS_PER_COMPANY)


def _int(form, name, default, lo, hi):
    try:
        v = int(form.get(name, default))
        return max(lo, min(hi, v))
    except (TypeError, ValueError):
        return default


@app.route("/upload", methods=["POST"])
@login_required
def upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(error="Choose an Excel (.xlsx) or CSV file first."), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in (".xlsx", ".xlsm", ".csv", ".tsv"):
        return jsonify(error="That file type isn't supported. Upload a .xlsx or .csv."), 400

    api_key = (request.form.get("api_key") or os.environ.get("SEAMLESS_API_KEY") or "").strip()
    if not api_key:
        return jsonify(error="No API key. Enter one in the Configuration panel, or set SEAMLESS_API_KEY."), 400

    cfg = {
        "search_limit":  _int(request.form, "search_limit",  core.SEARCH_LIMIT, 1, 25),
        "poll_interval": _int(request.form, "poll_interval", core.POLL_INTERVAL_SECONDS, 3, 120),
        "poll_attempts": _int(request.form, "poll_attempts", core.POLL_MAX_ATTEMPTS, 3, 60),
        "min_rank":      _int(request.form, "min_rank", 999, 1, 999),
        "max_contacts":  _int(request.form, "max_contacts", core.MAX_CONTACTS_PER_COMPANY, 1, 15),
        "workers":       _int(request.form, "workers", 6, 1, 12),
        "start":         _int(request.form, "start", 1, 1, 100000),
        "limit":         _int(request.form, "limit_companies", 0, 0, 100000),
        "preview":       request.form.get("preview") == "1",
    }

    job_id = uuid.uuid4().hex[:12]
    saved = os.path.join(UPLOAD_DIR, f"{job_id}_{secure_filename(f.filename)}")
    f.save(saved)

    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "queued", "total": 0, "current": 0, "company": "",
            "found": 0, "nomatch": 0, "skipped": 0, "errors": 0,
            "contacts": 0, "cached": 0, "preview": False,
            "drive_link": None, "log": [], "output": None, "error": None
        }

    threading.Thread(target=run_job, args=(job_id, saved, api_key, cfg), daemon=True).start()
    return jsonify(job_id=job_id)


@app.route("/export-master")
@login_required
def export_master():
    path = os.path.join(OUTPUT_DIR, "bim_master_export.xlsx")
    try:
        core.export_master_xlsx(path)
    except Exception as e:
        return jsonify(error=str(e)), 500
    return send_file(path, as_attachment=True, download_name="bim_master_export.xlsx")

@app.route("/master-stats")
@login_required
def master_stats():
    try:
        return jsonify(core.cache_stats())
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route("/purge-phoneless", methods=["POST", "GET"])
@login_required
def purge_phoneless():
    """One-shot admin action: delete all cached contacts missing phone AND email.
    Companies whose only cached rows are phoneless will be re-researched on the next run."""
    try:
        deleted, remaining = core.cache_purge_phoneless()
        return jsonify(deleted=deleted, remaining=remaining,
                       message=f"Removed {deleted} phoneless contact(s). "
                               f"{remaining} good contact(s) remain in the cache. "
                               f"Companies with no good cached rows will re-research on next run.")
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/test-seamless")
@login_required
def test_seamless():
    """Quick health check: send ONE Seamless search request and report the result.
    Use this to prove the API key + Seamless connectivity work before running a full job."""
    import time as _t
    api_key = os.environ.get("SEAMLESS_API_KEY", "").strip()
    if not api_key:
        return jsonify(ok=False, error="SEAMLESS_API_KEY env var is not set on Render."), 500
    try:
        headers = core.auth_headers(api_key)
        t0 = _t.time()
        cands = core.search_candidates("Microsoft", headers, limit=3)
        elapsed = _t.time() - t0
        return jsonify(
            ok=True,
            elapsed_seconds=round(elapsed, 2),
            candidates_returned=len(cands),
            sample=[{"name": (c.get("name") or ""), "title": c.get("title", "")} for c in cands[:3]],
            message="Seamless is reachable and returning data."
        )
    except Exception as e:
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}",
                       message="Seamless call failed. Check API key, credits, or network."), 500

@app.route("/status/<job_id>")
@login_required
def status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify(error="Unknown job."), 404
        return jsonify({
            "status":   job["status"],
            "total":    job["total"],
            "current":  job["current"],
            "company":  job["company"],
            "error":    job["error"],
            "found":    job["found"],
            "nomatch":  job["nomatch"],
            "skipped":  job["skipped"],
            "errors":   job["errors"],
            "contacts": job["contacts"],
            "cached":   job["cached"],
            "preview":  job["preview"],
            "log":      job["log"][-250:],
            "download": f"/download/{job_id}" if job["status"] == "done" else None,
            "drive_link": job.get("drive_link"),
        })


@app.route("/download/<job_id>")
@login_required
def download(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job or job["status"] != "done" or not job["output"]:
        abort(404)
    return send_file(job["output"], as_attachment=True,
                     download_name=f"contacts_{job_id}.xlsx")


# =============================================================================
# HTML PAGE
# =============================================================================

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BIM Data Execution</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=Inter:wght@300;400;500&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root{
    --bg-0:#050704; --bg-1:#0a0d08; --bg-2:#111614; --bg-3:#161c17;
    --olive-1:#6b8020; --olive-2:#8ca83c; --olive-3:#b8c95a; --olive-4:#d4e17a; --olive-5:#f5f9d4;
    --ink:#e8ecdc; --ink-2:#d4d8c8; --ink-dim:rgba(232,236,220,.55); --ink-muted:rgba(232,236,220,.4);
    --muted:#8a9070; --muted-2:#5a6045;
    --err:rgba(163,72,72,.9); --line:rgba(184,201,90,.12); --line-2:rgba(184,201,90,.22);
    --glass-bg:linear-gradient(145deg,rgba(255,255,255,0.04) 0%,rgba(255,255,255,0.01) 100%);
    --glass-border:rgba(184,201,90,0.15);
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0}
  body{
    background:var(--bg-0);
    color:var(--ink-2);
    font-family:"Inter",-apple-system,BlinkMacSystemFont,system-ui,sans-serif;
    font-weight:400;
    letter-spacing:-0.005em;
    line-height:1.55;
    -webkit-font-smoothing:antialiased;
    -moz-osx-font-smoothing:grayscale;
    min-height:100vh;
    overflow-x:hidden;
    position:relative;
  }

  /* Mesh gradient backdrop - no grid */
  body::before{
    content:"";
    position:fixed; inset:0;
    background:
      radial-gradient(ellipse 900px 600px at 15% 5%, rgba(140,168,60,0.18) 0%, transparent 55%),
      radial-gradient(ellipse 800px 700px at 85% 95%, rgba(184,201,90,0.14) 0%, transparent 55%),
      radial-gradient(ellipse 600px 500px at 60% 40%, rgba(212,225,122,0.07) 0%, transparent 60%);
    pointer-events:none; z-index:0;
  }
  body::after{
    content:"";
    position:fixed; inset:0;
    opacity:0.025;
    background-image:repeating-linear-gradient(45deg, transparent 0px, transparent 2px, rgba(255,255,255,0.5) 2px, rgba(255,255,255,0.5) 3px);
    pointer-events:none; z-index:0;
  }

  /* Holographic light beams */
  .beam{ position:fixed; width:600px; height:2px; background:linear-gradient(90deg, transparent, rgba(212,225,122,.28), transparent); filter:blur(1px); pointer-events:none; z-index:1; }
  .beam1{ top:12%; left:8%; animation:beamSweep1 22s ease-in-out infinite; }
  .beam2{ bottom:18%; right:5%; animation:beamSweep2 26s ease-in-out infinite; }

  .wrap{ max-width:820px; margin:0 auto; padding:56px 28px 90px; position:relative; z-index:2; }

  /* Glass card system */
  .glass{
    background:var(--glass-bg);
    backdrop-filter:blur(20px) saturate(1.4);
    -webkit-backdrop-filter:blur(20px) saturate(1.4);
    border:1px solid var(--glass-border);
    border-radius:16px;
    position:relative;
    overflow:hidden;
  }
  .glass::before{
    content:""; position:absolute; top:0; left:0; right:0; height:1px;
    background:linear-gradient(90deg, transparent, rgba(212,225,122,0.35), transparent);
    pointer-events:none;
  }
  .glass::after{
    content:""; position:absolute; inset:0; border-radius:16px; padding:1px;
    background:linear-gradient(145deg, rgba(212,225,122,0.15), transparent 50%, rgba(140,168,60,0.08));
    -webkit-mask:linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
    -webkit-mask-composite:xor; mask-composite:exclude;
    pointer-events:none;
  }

  /* HERO */
  .hero{ display:flex; align-items:center; gap:44px; margin-bottom:36px; }
  .hero-viz{ width:260px; height:280px; flex:0 0 auto; position:relative; animation:floatY 6s ease-in-out infinite; }
  .hero-viz-inner{ position:absolute; inset:0; animation:prismShift 8s ease-in-out infinite; }
  .hero-text{ flex:1; min-width:0; }

  .eyebrow{
    font-family:"JetBrains Mono",monospace;
    font-size:10.5px; letter-spacing:.4em; text-transform:uppercase;
    color:var(--olive-3); margin-bottom:16px;
  }
  .eyebrow .dot{
    display:inline-block; width:6px; height:6px; border-radius:50%;
    background:var(--olive-3); box-shadow:0 0 10px var(--olive-3);
    animation:livePulse 1.6s ease-in-out infinite;
    vertical-align:middle; margin-right:10px;
  }
  h1{
    font-family:"Space Grotesk",sans-serif;
    font-weight:500; font-size:52px; letter-spacing:-.035em;
    margin:0 0 14px; line-height:1;
    background:linear-gradient(120deg, #ffffff 0%, var(--olive-5) 30%, var(--olive-4) 60%, var(--olive-3) 100%);
    background-size:200% auto;
    -webkit-background-clip:text; background-clip:text;
    -webkit-text-fill-color:transparent; color:transparent;
    animation:shimmerText 8s ease-in-out infinite;
  }
  .sub{ color:var(--ink-dim); font-size:15px; margin:0 0 22px; max-width:38ch; line-height:1.55; font-weight:300; }

  .tiers{ display:flex; gap:8px; flex-wrap:wrap; }
  .tier-badge{
    display:inline-flex; align-items:center; gap:6px;
    padding:6px 12px; border-radius:99px;
    font-family:"JetBrains Mono",monospace; font-size:10.5px;
    backdrop-filter:blur(10px);
  }
  .tier-badge .bullet{ width:5px; height:5px; border-radius:50%; }
  .tier-1{ background:rgba(212,225,122,0.06); border:1px solid rgba(212,225,122,0.2); color:var(--olive-4); }
  .tier-1 .bullet{ background:var(--olive-4); box-shadow:0 0 6px var(--olive-4); }
  .tier-2{ background:rgba(184,201,90,0.06); border:1px solid rgba(184,201,90,0.2); color:var(--olive-3); }
  .tier-2 .bullet{ background:var(--olive-3); box-shadow:0 0 6px var(--olive-3); }
  .tier-3{ background:rgba(140,168,60,0.06); border:1px solid rgba(140,168,60,0.2); color:var(--olive-2); }
  .tier-3 .bullet{ background:var(--olive-2); box-shadow:0 0 6px var(--olive-2); }

  /* Ticker */
  .ticker-wrap{
    overflow:hidden; margin:0 0 32px; padding:14px 0;
    border-top:1px solid var(--line);
    border-bottom:1px solid var(--line);
    position:relative;
  }
  .ticker-wrap::before, .ticker-wrap::after{
    content:""; position:absolute; top:0; bottom:0; width:100px;
    z-index:2; pointer-events:none;
  }
  .ticker-wrap::before{ left:0; background:linear-gradient(90deg,var(--bg-0),transparent); }
  .ticker-wrap::after{ right:0; background:linear-gradient(270deg,var(--bg-0),transparent); }
  .ticker{
    display:flex; gap:56px; white-space:nowrap;
    animation:tickerFlow 45s linear infinite;
    font-family:"JetBrains Mono",monospace; font-size:12px;
    color:var(--ink-dim);
  }
  .ticker .num{ color:var(--olive-4); }
  .ticker .company{ color:var(--ink); }
  .ticker-sep{ color:rgba(184,201,90,0.22); }

  /* Master store */
  #masterbar{
    display:flex; align-items:center; gap:14px;
    margin:0 0 22px;
    font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-dim);
  }
  #masterbar .dot{
    width:6px; height:6px; border-radius:50%; background:var(--olive-3);
    box-shadow:0 0 10px var(--olive-3);
    animation:livePulse 1.6s ease-in-out infinite;
  }
  #masterbar a{
    margin-left:auto; padding:6px 14px;
    border:1px solid var(--line-2); border-radius:99px;
    color:var(--olive-3); text-decoration:none;
    transition:all .2s;
    backdrop-filter:blur(10px);
  }
  #masterbar a:hover{ background:rgba(184,201,90,.06); border-color:var(--olive-3); }

  /* Card header */
  .card{ margin-bottom:20px; }
  .card-h{ display:flex; align-items:center; gap:14px; padding:18px 24px; border-bottom:1px solid var(--line); }
  .card-h .n{
    font-family:"JetBrains Mono",monospace; font-size:10.5px;
    color:rgba(212,225,122,0.7);
    border:1px solid rgba(184,201,90,0.25); padding:4px 10px; border-radius:6px;
    background:rgba(212,225,122,0.04);
  }
  .card-h h2{
    font-family:"Space Grotesk",sans-serif; font-size:15px; font-weight:500;
    margin:0; color:var(--ink); letter-spacing:-0.01em;
  }
  .card-h .chev{
    margin-left:auto; color:var(--ink-muted);
    font-family:"JetBrains Mono",monospace; font-size:11.5px; cursor:pointer;
  }
  .card-b{ padding:24px; }
  .collapsed .card-b{ display:none; }
  .collapsible .card-h{ cursor:pointer; }

  .set{
    background:linear-gradient(135deg,rgba(212,225,122,0.15),rgba(184,201,90,0.08));
    border:1px solid rgba(212,225,122,0.3);
    color:var(--olive-4);
    font-family:"JetBrains Mono",monospace; font-size:10.5px;
    padding:4px 12px; border-radius:99px;
    display:inline-flex; align-items:center; gap:6px;
  }
  .set::before{ content:""; width:5px; height:5px; border-radius:50%; background:var(--olive-4); box-shadow:0 0 6px var(--olive-4); }

  /* Form fields */
  label{
    display:block;
    font-family:"JetBrains Mono",monospace;
    font-size:10.5px; letter-spacing:.12em; text-transform:uppercase;
    color:var(--muted); margin-bottom:10px;
  }
  .fld{ margin-bottom:22px; }
  .fld:last-child{ margin-bottom:0; }
  input[type=text],input[type=password],input[type=number],select{
    width:100%; padding:12px 16px;
    border:1px solid var(--line);
    border-radius:8px;
    font-family:"JetBrains Mono",monospace; font-size:13px;
    background:rgba(255,255,255,0.02);
    color:var(--ink);
    transition:all .2s;
    backdrop-filter:blur(10px);
  }
  input:focus,select:focus{
    outline:none; border-color:var(--olive-3);
    box-shadow:0 0 0 3px rgba(184,201,90,.12);
    background:rgba(255,255,255,0.04);
  }
  select{ cursor:pointer; }
  .keyrow{ display:flex; gap:8px; }
  .keyrow input{ flex:1; }
  .ghost{
    border:1px solid var(--line-2); background:transparent;
    padding:0 16px; border-radius:8px; cursor:pointer;
    font-family:"JetBrains Mono",monospace; font-size:11.5px;
    color:var(--ink-dim); transition:all .2s;
  }
  .ghost:hover{ border-color:var(--olive-3); color:var(--olive-3); background:rgba(184,201,90,0.04); }
  .hint{ font-size:12px; color:var(--ink-dim); margin-top:8px; font-weight:300; }
  .hint b{ color:var(--ink-2); font-weight:500; }
  .grid2{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }

  /* Drop zone */
  .drop{
    border:1.5px dashed var(--line-2); border-radius:12px;
    padding:42px 22px; text-align:center; cursor:pointer;
    background:rgba(184,201,90,.02); transition:all .3s;
    position:relative; overflow:hidden;
  }
  .drop::before{
    content:""; position:absolute; inset:0;
    background:radial-gradient(circle at var(--mx,50%) var(--my,50%), rgba(184,201,90,0.08) 0%, transparent 40%);
    opacity:0; transition:opacity .3s;
    pointer-events:none;
  }
  .drop:hover, .drop.hot{
    border-color:var(--olive-3);
    background:rgba(184,201,90,.04);
  }
  .drop:hover::before, .drop.hot::before{ opacity:1; }
  .drop strong{ color:var(--ink); font-weight:500; letter-spacing:-0.01em; }
  .drop .h2{ color:var(--ink-dim); font-size:12.5px; margin-top:10px; font-weight:300; }
  .drop .h2 b{ color:var(--olive-3); font-weight:500; }
  .fname{ margin-top:16px; font-family:"JetBrains Mono",monospace; font-size:13px; color:var(--olive-4); }
  input[type=file]{ display:none; }

  /* Buttons */
  button.run{
    padding:15px; border:0; border-radius:8px; cursor:pointer;
    font-family:"Space Grotesk",sans-serif; font-weight:500; font-size:14.5px;
    transition:all .3s cubic-bezier(.4,0,.2,1);
    position:relative; overflow:hidden;
    letter-spacing:-0.01em;
  }
  button.run.primary{
    background:linear-gradient(135deg,var(--olive-2) 0%,var(--olive-3) 50%,var(--olive-4) 100%);
    color:var(--bg-0);
    box-shadow:0 4px 24px rgba(184,201,90,.25), inset 0 1px 0 rgba(255,255,255,0.15);
  }
  button.run.primary::before{
    content:""; position:absolute; top:0; left:-100%; width:100%; height:100%;
    background:linear-gradient(90deg, transparent, rgba(255,255,255,0.25), transparent);
    transition:left .5s;
  }
  button.run.primary:hover:not(:disabled){
    transform:translateY(-2px);
    box-shadow:0 8px 32px rgba(184,201,90,.4), inset 0 1px 0 rgba(255,255,255,0.2);
  }
  button.run.primary:hover:not(:disabled)::before{ left:100%; }
  button.run.ghost-btn{
    background:rgba(255,255,255,0.02); color:var(--olive-3);
    border:1px solid var(--line-2);
    backdrop-filter:blur(10px);
  }
  button.run.ghost-btn:hover:not(:disabled){
    background:rgba(184,201,90,.06); border-color:var(--olive-3);
    transform:translateY(-1px);
  }
  button.run:disabled{ opacity:.3; cursor:not-allowed; }
  .msg{ margin-top:14px; font-size:13px; color:var(--err); min-height:1em; font-weight:300; }

  /* Stats */
  .stats{ display:grid; grid-template-columns:repeat(5,1fr); gap:10px; margin-bottom:22px; }
  .stat{
    padding:18px 16px;
    transition:all .4s cubic-bezier(.34,1.56,.64,1);
  }
  .stat:hover{
    transform:translateY(-4px);
    border-color:rgba(184,201,90,.35);
  }
  .stat .v{
    font-family:"Space Grotesk",sans-serif; font-weight:500; font-size:34px; line-height:1;
    color:var(--ink); letter-spacing:-0.02em;
    transition:all .3s;
  }
  .stat .k{
    font-family:"JetBrains Mono",monospace; font-size:9.5px;
    letter-spacing:.18em; text-transform:uppercase;
    color:var(--ink-muted); margin-top:10px;
  }
  .stat.total .v{ color:var(--olive-4); text-shadow:0 0 30px rgba(212,225,122,.4); }
  .stat.found .v{ color:var(--olive-5); text-shadow:0 0 30px rgba(245,249,212,.5); }
  .stat.err .v{ color:var(--err); }
  .stat .v.bump{ animation:numberPop .7s cubic-bezier(.34,1.56,.64,1); }

  /* Progress */
  .barwrap{
    height:2px;
    background:rgba(184,201,90,0.08);
    border-radius:99px; overflow:hidden;
    position:relative;
  }
  .bar{
    height:100%; width:0;
    background:linear-gradient(90deg, var(--olive-1), var(--olive-3) 40%, var(--olive-5) 50%, var(--olive-3) 60%, var(--olive-1));
    background-size:200% 100%;
    animation:shimmerFlow 2.5s linear infinite;
    box-shadow:0 0 20px rgba(212,225,122,0.6);
    border-radius:99px;
    transition:width .5s cubic-bezier(.16,1,.3,1);
  }
  .meta{ display:flex; justify-content:space-between; margin:14px 0 20px; font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-dim); }
  .meta .stage-dot{ display:inline-block; width:6px; height:6px; border-radius:50%; background:var(--olive-4); margin-right:8px; box-shadow:0 0 8px var(--olive-4); animation:livePulse 1.6s ease-in-out infinite; vertical-align:middle; }
  .meta .companyname{ color:var(--ink); }
  .meta .count{ color:var(--olive-4); letter-spacing:0.05em; }

  /* Console */
  .console{
    background:rgba(5,7,4,0.5);
    backdrop-filter:blur(10px);
    border:1px solid var(--line);
    color:#a8b090; border-radius:12px;
    padding:20px; height:320px; overflow:auto;
    font-family:"JetBrains Mono",monospace; font-size:12.5px; line-height:1.8;
    white-space:pre-wrap; word-break:break-word;
    box-shadow:inset 0 0 40px rgba(184,201,90,.02);
  }
  .console::-webkit-scrollbar{ width:6px; }
  .console::-webkit-scrollbar-track{ background:transparent; }
  .console::-webkit-scrollbar-thumb{ background:rgba(184,201,90,0.2); border-radius:3px; }
  .console .ok{ color:var(--olive-4); }
  .console .cache{ color:var(--olive-1); }
  .console .er{ color:var(--err); }
  .console .dim{ color:var(--muted-2); }
  .console .hi{ color:var(--olive-5); }
  .console > div{ animation:fadeSlide .5s cubic-bezier(.16,1,.3,1); }

  /* Done */
  .done{
    margin-top:22px; padding:24px;
    display:none; align-items:center; justify-content:space-between; gap:16px;
    background:linear-gradient(145deg,rgba(212,225,122,0.1),rgba(140,168,60,0.04));
    border:1px solid rgba(212,225,122,0.3);
    border-radius:16px;
    box-shadow:0 0 60px rgba(212,225,122,0.15);
    backdrop-filter:blur(20px);
  }
  .done.show{ display:flex; }
  .done a{
    background:linear-gradient(135deg,var(--olive-3),var(--olive-4));
    color:var(--bg-0); text-decoration:none;
    padding:12px 22px; border-radius:8px;
    font-family:"Space Grotesk",sans-serif; font-size:14px; font-weight:500;
    letter-spacing:-0.01em; white-space:nowrap;
    box-shadow:0 4px 24px rgba(212,225,122,.3);
    transition:all .3s;
  }
  .done a:hover{ transform:translateY(-2px); box-shadow:0 8px 32px rgba(212,225,122,.5); }
  .hidden{ display:none; }

  /* Animations */
  @keyframes beamSweep1 { 0%,100%{transform:translate(-30%,-20%) rotate(15deg);opacity:.3;} 50%{transform:translate(30%,20%) rotate(15deg);opacity:.6;} }
  @keyframes beamSweep2 { 0%,100%{transform:translate(20%,30%) rotate(-25deg);opacity:.2;} 50%{transform:translate(-20%,-30%) rotate(-25deg);opacity:.5;} }
  @keyframes floatY { 0%,100%{transform:translateY(0px);} 50%{transform:translateY(-8px);} }
  @keyframes rotate360 { from{transform:rotate(0deg);} to{transform:rotate(360deg);} }
  @keyframes counterRotate { from{transform:rotate(0deg);} to{transform:rotate(-360deg);} }
  @keyframes prismShift { 0%,100%{filter:hue-rotate(0deg) drop-shadow(0 0 30px rgba(184,201,90,.4));} 33%{filter:hue-rotate(-12deg) drop-shadow(0 0 30px rgba(212,225,122,.5));} 66%{filter:hue-rotate(8deg) drop-shadow(0 0 30px rgba(140,168,60,.4));} }
  @keyframes ringGlow { 0%,100%{opacity:.7;} 50%{opacity:1;} }
  @keyframes packet { 0%{offset-distance:0%;opacity:0;} 15%{opacity:1;} 85%{opacity:1;} 100%{offset-distance:100%;opacity:0;} }
  @keyframes shimmerText { 0%,100%{background-position:0% 50%;} 50%{background-position:100% 50%;} }
  @keyframes shimmerFlow { 0%{background-position:-200% 0;} 100%{background-position:200% 0;} }
  @keyframes numberPop { 0%{transform:translateY(15px) scale(.8);opacity:.4;} 60%{transform:translateY(-3px) scale(1.08);} 100%{transform:translateY(0) scale(1);opacity:1;} }
  @keyframes fadeSlide { from{opacity:0;transform:translateX(-15px);} to{opacity:1;transform:translateX(0);} }
  @keyframes livePulse { 0%,100%{opacity:1;transform:scale(1);} 50%{opacity:.5;transform:scale(1.15);} }
  @keyframes tickerFlow { from{transform:translateX(0);} to{transform:translateX(-50%);} }

  @media (max-width:720px){
    .hero{ flex-direction:column; text-align:center; }
    .stats{ grid-template-columns:repeat(3,1fr); }
    .grid2{ grid-template-columns:1fr; }
    h1{ font-size:38px; }
    .wrap{ padding:36px 20px 60px; }
    .hero-viz{ width:220px; height:240px; }
  }
  @media (prefers-reduced-motion:reduce){ *,*::before,*::after{ animation-duration:.01ms !important; animation-iteration-count:1 !important; transition-duration:.01ms !important; } }
</style>
</head>
<body>

<div class="beam beam1"></div>
<div class="beam beam2"></div>

<div class="wrap">

  <!-- HERO -->
  <div class="hero">
    <div class="hero-viz" aria-hidden="true">
      <div class="hero-viz-inner">
        <svg width="260" height="280" viewBox="0 0 260 280" style="overflow:visible;">
          <defs>
            <radialGradient id="coreGrad" cx="30%" cy="30%">
              <stop offset="0%" stop-color="#f5f9d4"/>
              <stop offset="30%" stop-color="#d4e17a"/>
              <stop offset="70%" stop-color="#b8c95a"/>
              <stop offset="100%" stop-color="#6b8020"/>
            </radialGradient>
            <radialGradient id="nodeGrad" cx="30%" cy="30%">
              <stop offset="0%" stop-color="#e8f0a8"/>
              <stop offset="60%" stop-color="#b8c95a"/>
              <stop offset="100%" stop-color="#6b8020"/>
            </radialGradient>
            <radialGradient id="nodeGradDim" cx="30%" cy="30%">
              <stop offset="0%" stop-color="#a8c04a"/>
              <stop offset="100%" stop-color="#3d4a12"/>
            </radialGradient>
          </defs>

          <g style="transform-origin:130px 140px; animation:rotate360 40s linear infinite;">
            <circle cx="130" cy="140" r="115" fill="none" stroke="rgba(184,201,90,0.15)" stroke-width="0.5" stroke-dasharray="1,3"/>
          </g>
          <g style="transform-origin:130px 140px; animation:counterRotate 30s linear infinite;">
            <circle cx="130" cy="140" r="85" fill="none" stroke="rgba(212,225,122,0.2)" stroke-width="0.5" stroke-dasharray="2,4"/>
          </g>

          <g stroke="rgba(184,201,90,0.4)" stroke-width="0.75" fill="none">
            <line x1="130" y1="140" x2="70" y2="55" stroke-dasharray="2,3"/>
            <line x1="130" y1="140" x2="190" y2="55" stroke-dasharray="2,3"/>
            <line x1="130" y1="140" x2="35" y2="140" stroke-dasharray="2,3"/>
            <line x1="130" y1="140" x2="225" y2="140" stroke-dasharray="2,3"/>
            <line x1="130" y1="140" x2="80" y2="235" stroke-dasharray="2,3"/>
            <line x1="130" y1="140" x2="180" y2="235" stroke-dasharray="2,3"/>
          </g>

          <circle r="2.5" fill="#f5f9d4" style="offset-path:path('M 130 140 L 70 55'); animation:packet 3s ease-in-out infinite; filter:drop-shadow(0 0 8px #d4e17a);"/>
          <circle r="2.5" fill="#f5f9d4" style="offset-path:path('M 130 140 L 190 55'); animation:packet 3s ease-in-out infinite .5s; filter:drop-shadow(0 0 8px #d4e17a);"/>
          <circle r="2" fill="#d4e17a" style="offset-path:path('M 130 140 L 35 140'); animation:packet 3.5s ease-in-out infinite 1s; filter:drop-shadow(0 0 6px #b8c95a);"/>
          <circle r="2" fill="#d4e17a" style="offset-path:path('M 130 140 L 225 140'); animation:packet 3.5s ease-in-out infinite 1.5s; filter:drop-shadow(0 0 6px #b8c95a);"/>
          <circle r="1.5" fill="#b8c95a" style="offset-path:path('M 130 140 L 80 235'); animation:packet 4s ease-in-out infinite 2s; filter:drop-shadow(0 0 5px #8ca83c);"/>
          <circle r="1.5" fill="#b8c95a" style="offset-path:path('M 130 140 L 180 235'); animation:packet 4s ease-in-out infinite 2.5s; filter:drop-shadow(0 0 5px #8ca83c);"/>

          <g style="animation:ringGlow 3s ease-in-out infinite;">
            <circle cx="70" cy="55" r="16" fill="url(#nodeGrad)" stroke="#f5f9d4" stroke-width="0.75"/>
            <text x="70" y="33" text-anchor="middle" fill="#d4e17a" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">CEO / VP</text>
          </g>
          <g style="animation:ringGlow 3s ease-in-out infinite .3s;">
            <circle cx="190" cy="55" r="16" fill="url(#nodeGrad)" stroke="#f5f9d4" stroke-width="0.75"/>
            <text x="190" y="33" text-anchor="middle" fill="#d4e17a" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">President</text>
          </g>

          <g style="animation:ringGlow 3s ease-in-out infinite .6s;">
            <circle cx="35" cy="140" r="15" fill="url(#nodeGrad)" stroke="#b8c95a" stroke-width="0.75"/>
            <text x="35" y="172" text-anchor="middle" fill="#b8c95a" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">Estimator</text>
          </g>
          <g style="animation:ringGlow 3s ease-in-out infinite .9s;">
            <circle cx="225" cy="140" r="15" fill="url(#nodeGrad)" stroke="#b8c95a" stroke-width="0.75"/>
            <text x="225" y="172" text-anchor="middle" fill="#b8c95a" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">PM</text>
          </g>

          <g style="animation:ringGlow 3s ease-in-out infinite 1.2s;">
            <circle cx="80" cy="235" r="13" fill="url(#nodeGradDim)" stroke="#8ca83c" stroke-width="0.75"/>
            <text x="80" y="262" text-anchor="middle" fill="#8ca83c" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">BIM Lead</text>
          </g>
          <g style="animation:ringGlow 3s ease-in-out infinite 1.5s;">
            <circle cx="180" cy="235" r="13" fill="url(#nodeGradDim)" stroke="#8ca83c" stroke-width="0.75"/>
            <text x="180" y="262" text-anchor="middle" fill="#8ca83c" font-family="JetBrains Mono, monospace" font-size="8.5" letter-spacing="0.5">VDC / CAD</text>
          </g>

          <g>
            <circle cx="130" cy="140" r="34" fill="url(#coreGrad)" stroke="#f5f9d4" stroke-width="1"/>
            <circle cx="130" cy="140" r="28" fill="none" stroke="rgba(255,255,255,0.2)" stroke-width="0.5"/>
            <text x="130" y="137" text-anchor="middle" fill="#050704" font-family="JetBrains Mono, monospace" font-size="7.5" font-weight="600" letter-spacing="1.5">COMPANY</text>
            <text x="130" y="150" text-anchor="middle" fill="#050704" font-family="JetBrains Mono, monospace" font-size="6.5" opacity="0.7">domain.com</text>
          </g>
        </svg>
      </div>
    </div>

    <div class="hero-text">
      <div class="eyebrow"><span class="dot"></span>BMSI &middot; Lead Generation Engine</div>
      <h1>BIM Data<br/>Execution.</h1>
      <p class="sub">Precision lead intelligence. Every executive, estimator, and BIM lead - locked to the exact company you specify.</p>
      <div class="tiers">
        <span class="tier-badge tier-1"><span class="bullet"></span>Executive</span>
        <span class="tier-badge tier-2"><span class="bullet"></span>Estimator &middot; PM</span>
        <span class="tier-badge tier-3"><span class="bullet"></span>BIM &middot; VDC</span>
      </div>
    </div>
  </div>

  <!-- Ticker -->
  <div class="ticker-wrap">
    <div class="ticker" id="ticker">
      <span>Loading activity&hellip;</span>
    </div>
  </div>

  <div id="masterbar">
    <span class="dot"></span>
    <span id="masterinfo">Master store: loading&hellip;</span>
    <a href="/export-master">Export master &rarr;</a>
  </div>

  <!-- CONFIGURATION -->
  <div class="glass card collapsible" id="cfgCard">
    <div class="card-h" id="cfgToggle">
      <span class="n">01</span><h2>Configuration</h2>
      {% if api_key_set %}<span class="set">Server key active</span>{% endif %}
      <span class="chev" id="cfgChev">Hide &darr;</span>
    </div>
    <div class="card-b">
      <div class="fld">
        <label for="apikey">Seamless API key</label>
        <div class="keyrow">
          <input type="password" id="apikey" autocomplete="off"
            placeholder="{% if api_key_set %}Using server key - leave blank, or paste to override{% else %}Paste your Seamless API key{% endif %}">
          <button type="button" class="ghost" id="toggleKey">show</button>
        </div>
        <div class="hint">Sent in the <b>token</b> header. Not stored - lives only in this browser session.</div>
      </div>
      <div class="grid2">
        <div class="fld">
          <label for="limit">Candidates to rank</label>
          <input type="number" id="limit" min="1" max="25" value="{{ default_limit }}">
          <div class="hint">Free search size. Keep at 25.</div>
        </div>
        <div class="fld">
          <label for="maxc">Max contacts per company</label>
          <input type="number" id="maxc" min="1" max="15" value="{{ default_maxc }}">
          <div class="hint">Contacts to <b>research</b>. <b>~1 credit each.</b></div>
        </div>
        <div class="fld">
          <label for="workers">Parallel companies</label>
          <input type="number" id="workers" min="1" max="12" value="6">
          <div class="hint">Companies at once. 6 is safe.</div>
        </div>
        <div class="fld">
          <label for="pollint">Poll interval (sec)</label>
          <input type="number" id="pollint" min="5" max="120" value="{{ default_interval }}">
          <div class="hint">Wait between research checks.</div>
        </div>
      </div>
      <div class="fld" style="margin-top:2px">
        <label for="pollatt">Max poll attempts</label>
        <input type="number" id="pollatt" min="3" max="60" value="{{ default_attempts }}">
        <div class="hint">Give up on research after this many checks.</div>
      </div>
      <div style="height:1px;background:var(--line);margin:26px 0"></div>
      <div class="fld">
        <label for="minrank">Credit saver &middot; match quality</label>
        <select id="minrank">
          <option value="999">Any match (research everyone)</option>
          <option value="45">Skip executives (no VP/President/CEO)</option>
          <option value="37">Estimators &amp; Project roles &amp; above</option>
          <option value="27">Estimators &amp; above</option>
          <option value="8">BIM/VDC/CAD roles only</option>
        </select>
        <div class="hint">Weaker matches skipped <b>free</b>.</div>
      </div>
      <div class="grid2">
        <div class="fld">
          <label for="startrow">Start at company #</label>
          <input type="number" id="startrow" min="1" value="1">
          <div class="hint">For batching, e.g. start at 101.</div>
        </div>
        <div class="fld">
          <label for="maxco">Max companies this run</label>
          <input type="number" id="maxco" min="0" max="200" value="0" placeholder="0 = all">
          <div class="hint">0 = all remaining (up to 200 total).</div>
        </div>
      </div>
    </div>
  </div>

  <!-- UPLOAD -->
  <div class="glass card" id="uploadCard">
    <div class="card-h"><span class="n">02</span><h2>Upload &amp; Run</h2></div>
    <div class="card-b">
      <div class="drop" id="drop">
        <strong>Choose a file</strong> or drag it here
        <div class="h2">Excel (.xlsx) or CSV &middot; 1-200 companies &middot; columns: <b>Company</b> + optional <b>Website</b> or <b>Email</b> (for exact company matching)</div>
        <div class="fname" id="fname"></div>
      </div>
      <input type="file" id="file" accept=".xlsx,.xlsm,.csv,.tsv">
      <div style="display:flex;gap:10px;margin-top:24px">
        <button class="run ghost-btn" id="preview" disabled style="flex:1">Preview &middot; free</button>
        <button class="run primary" id="go" disabled style="flex:1">Run &middot; uses credits</button>
      </div>
      <div class="msg" id="msg"></div>
    </div>
  </div>

  <!-- RUN / RESULTS -->
  <div class="hidden" id="run">
    <div class="stats">
      <div class="glass stat total"><div class="v" id="s_done">0</div><div class="k">Companies</div></div>
      <div class="glass stat found"><div class="v" id="s_contacts">0</div><div class="k" id="k_found">New contacts</div></div>
      <div class="glass stat"><div class="v" id="s_cached">0</div><div class="k">Reused free</div></div>
      <div class="glass stat"><div class="v" id="s_nomatch">0</div><div class="k">No match</div></div>
      <div class="glass stat err"><div class="v" id="s_err">0</div><div class="k">Errors</div></div>
    </div>
    <div class="barwrap"><div class="bar" id="bar"></div></div>
    <div class="meta">
      <span><span class="stage-dot"></span><span id="stage">Starting&hellip;</span></span>
      <span class="count" id="count"></span>
    </div>
    <div class="console" id="console"></div>
    <div class="done" id="done">
      <span>Your contacts file is ready.</span>
      <a id="dl" href="#">Download Excel</a>
    </div>
  </div>
</div>

<script>
  // Interactive drop hover
  var dropEl = document.getElementById('drop');
  dropEl.addEventListener('mousemove', function(e){
    var rect = dropEl.getBoundingClientRect();
    dropEl.style.setProperty('--mx', ((e.clientX - rect.left) / rect.width * 100) + '%');
    dropEl.style.setProperty('--my', ((e.clientY - rect.top) / rect.height * 100) + '%');
  });

  var cfgCard = document.getElementById('cfgCard');
  var cfgChev = document.getElementById('cfgChev');
  document.getElementById('cfgToggle').addEventListener('click', function(){
    cfgCard.classList.toggle('collapsed');
    cfgChev.innerHTML = cfgCard.classList.contains('collapsed') ? 'Edit &rarr;' : 'Hide &darr;';
  });
  var apikeyEl = document.getElementById('apikey');
  var tk = document.getElementById('toggleKey');
  tk.addEventListener('click', function(){
    var p = apikeyEl.type === 'password';
    apikeyEl.type = p ? 'text' : 'password';
    tk.textContent = p ? 'hide' : 'show';
  });

  var fileInput = document.getElementById('file'),
      fname = document.getElementById('fname'), go = document.getElementById('go'), msg = document.getElementById('msg');
  dropEl.addEventListener('click', function(){ fileInput.click(); });
  dropEl.addEventListener('dragover', function(e){ e.preventDefault(); dropEl.classList.add('hot'); });
  dropEl.addEventListener('dragleave', function(){ dropEl.classList.remove('hot'); });
  dropEl.addEventListener('drop', function(e){
    e.preventDefault(); dropEl.classList.remove('hot');
    if(e.dataTransfer.files.length){ fileInput.files = e.dataTransfer.files; onFile(); }
  });
  fileInput.addEventListener('change', onFile);
  function onFile(){
    if(fileInput.files.length){
      fname.textContent = fileInput.files[0].name;
      go.disabled = false;
      document.getElementById('preview').disabled = false;
    }
  }

  var preBtn = document.getElementById('preview');
  function collect(previewFlag){
    var fd = new FormData();
    fd.append('file', fileInput.files[0]);
    if(apikeyEl.value) fd.append('api_key', apikeyEl.value);
    fd.append('search_limit', document.getElementById('limit').value);
    fd.append('max_contacts', document.getElementById('maxc').value);
    fd.append('workers', document.getElementById('workers').value);
    fd.append('poll_interval', document.getElementById('pollint').value);
    fd.append('poll_attempts', document.getElementById('pollatt').value);
    fd.append('min_rank', document.getElementById('minrank').value);
    fd.append('start', document.getElementById('startrow').value || '1');
    fd.append('limit_companies', document.getElementById('maxco').value || '0');
    fd.append('preview', previewFlag ? '1' : '0');
    return fd;
  }
  function submit(previewFlag){
    msg.textContent = '';
    if(!fileInput.files.length){ msg.textContent = 'Choose a file first.'; return; }
    go.disabled = true; preBtn.disabled = true;
    (previewFlag ? preBtn : go).textContent = 'Uploading...';
    fetch('/upload', { method: 'POST', body: collect(previewFlag) })
      .then(function(r){ return r.json().then(function(d){ return { ok: r.ok, data: d }; }); })
      .then(function(res){
        if(!res.ok) throw new Error(res.data.error || 'Upload failed.');
        document.getElementById('uploadCard').style.display = 'none';
        cfgCard.classList.add('collapsed'); cfgChev.innerHTML = 'Edit &rarr;';
        document.getElementById('run').classList.remove('hidden');
        poll(res.data.job_id);
      })
      .catch(function(e){
        msg.textContent = e.message;
        go.disabled = false; preBtn.disabled = false;
        go.textContent = 'Run · uses credits';
        preBtn.textContent = 'Preview · free';
      });
  }
  go.addEventListener('click', function(){ submit(false); });
  preBtn.addEventListener('click', function(){ submit(true); });

  var bar = document.getElementById('bar'), stage = document.getElementById('stage'),
      count = document.getElementById('count'), cons = document.getElementById('console'),
      doneBox = document.getElementById('done'), dl = document.getElementById('dl');
  var S = {
    done: document.getElementById('s_done'),
    contacts: document.getElementById('s_contacts'),
    cached: document.getElementById('s_cached'),
    nomatch: document.getElementById('s_nomatch'),
    err: document.getElementById('s_err')
  };
  var lastValues = { done: 0, contacts: 0, cached: 0, nomatch: 0, err: 0 };

  fetch('/master-stats').then(function(r){ return r.json(); }).then(function(d){
    if(d && d.contacts != null){
      document.getElementById('masterinfo').innerHTML =
        'Master store &middot; <span class="num" style="color:var(--olive-4)">' + d.contacts.toLocaleString() + '</span> contacts across <span class="num" style="color:var(--olive-4)">' + d.companies.toLocaleString() + '</span> companies';
      var items = [
        '<span><span class="num">' + d.contacts.toLocaleString() + '</span> contacts cached</span>',
        '<span class="ticker-sep">&#9670;</span>',
        '<span><span class="num">' + d.companies.toLocaleString() + '</span> companies mapped</span>',
        '<span class="ticker-sep">&#9670;</span>',
        '<span>Domain-locked matching</span>',
        '<span class="ticker-sep">&#9670;</span>',
        '<span>Executive &middot; Estimator &middot; PM &middot; BIM tiers</span>',
        '<span class="ticker-sep">&#9670;</span>',
        '<span>Master synced to Drive</span>',
        '<span class="ticker-sep">&#9670;</span>'
      ];
      document.getElementById('ticker').innerHTML = items.join('') + items.join('');
    }
  }).catch(function(){
    document.getElementById('masterinfo').textContent = 'Master store: empty (builds as you run).';
  });

  function bumpStat(el, newVal, key){
    if(lastValues[key] !== newVal){
      lastValues[key] = newVal;
      el.textContent = newVal;
      el.classList.remove('bump');
      void el.offsetWidth;
      el.classList.add('bump');
    }
  }

  function render(log){
    cons.innerHTML = log.map(function(l){
      var cls = '';
      if(/ERROR|error|FAILED/.test(l)) cls = 'er';
      else if(/from cache/.test(l)) cls = 'cache';
      else if(/Finished|complete|ready|NOTHING SPENT/.test(l)) cls = 'ok';
      else if(/PREVIEW|Loaded|RUN|Starting|Input file|Output|master/.test(l)) cls = 'dim';
      else if(/\[search\]|\[research\]|\[poll\]|Processing/.test(l)) cls = 'hi';
      return '<div class="' + cls + '">' + l.replace(/</g,'&lt;') + '</div>';
    }).join('');
    cons.scrollTop = cons.scrollHeight;
  }

  function poll(jobId){
    fetch('/status/' + jobId).then(function(r){
      if(!r.ok){ stage.textContent = 'Job not found - server may have restarted. Please re-upload.'; return null; }
      return r.json();
    }).then(function(s){
      if(!s) return;
      render(s.log || []);
      bumpStat(S.done, s.current || 0, 'done');
      bumpStat(S.contacts, s.contacts || 0, 'contacts');
      bumpStat(S.cached, s.cached || 0, 'cached');
      bumpStat(S.nomatch, s.nomatch || 0, 'nomatch');
      bumpStat(S.err, s.errors || 0, 'err');
      document.getElementById('k_found').textContent = s.preview ? 'Would research' : 'New contacts';
      if(s.total){
        bar.style.width = Math.round((s.current / s.total) * 100) + '%';
        count.textContent = s.current + ' · ' + s.total;
      }
      if(s.status === 'running'){
        stage.innerHTML = (s.preview ? 'Previewing · ' : 'Processing · ') + '<span class="companyname">' + (s.company || '…') + '</span>';
      } else if(s.status === 'queued'){
        stage.textContent = 'Queued…';
      } else if(s.status === 'error'){
        stage.textContent = 'Stopped: ' + (s.error || 'error');
        return;
      } else if(s.status === 'done'){
        stage.textContent = s.preview ? 'Preview complete' : 'Complete';
        bar.style.width = '100%';
        dl.href = s.download;
        var baseMsg = s.preview
          ? 'Estimated cost: ~' + (s.contacts || 0) + ' credits for ' + (s.contacts || 0) + ' contacts. Nothing spent - Run when ready.'
          : 'Your contacts file is ready.';
        if(s.drive_link){ baseMsg += '  Also saved to shared Drive.'; }
        doneBox.querySelector('span').innerHTML = baseMsg +
          (s.drive_link ? ' <a href="' + s.drive_link + '" target="_blank" style="color:var(--olive-4); background:transparent; padding:0; box-shadow:none;">Open in Drive</a>' : '');
        dl.textContent = s.preview ? 'Download preview' : 'Download Excel';
        doneBox.classList.add('show');
        return;
      }
      setTimeout(function(){ poll(jobId); }, 2000);
    }).catch(function(){
      stage.textContent = 'Lost connection. Retrying...';
      setTimeout(function(){ poll(jobId); }, 4000);
    });
  }
</script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    try:
        from waitress import serve
        print(f"BIM Data Execution (production server) on http://0.0.0.0:{port}")
        serve(app, host="0.0.0.0", port=port, threads=8)
    except ImportError:
        print(f"BIM Data Execution (dev server) on http://0.0.0.0:{port}")
        app.run(host="0.0.0.0", port=port, threaded=True)