#!/usr/bin/env python3
"""
BIM Data Execution - web dashboard (v2, with editable API configuration + stats)

A local web page so non-technical users can upload a company file, tune settings,
and download the contacts file - without touching the command line. Reuses all
logic (and the hierarchy) from bim_data_execution.py.

SETUP (one time):
    pip install flask requests openpyxl
RUN:
    python app.py
Then open http://localhost:5000

The API key can be set on the page (Configuration panel), or via the
SEAMLESS_API_KEY environment variable as a default.
"""

import os
import time
import uuid
import threading
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import (Flask, request, jsonify, send_file, redirect, url_for,
                   render_template_string, session, abort)
from werkzeug.utils import secure_filename

import bim_data_execution as core   # must sit next to this file

APP_DIR = os.path.dirname(os.path.abspath(__file__))
# On a host, point these at a persistent disk via DATA_DIR; else use the app folder.
DATA_DIR = os.environ.get("DATA_DIR", APP_DIR)
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
OUTPUT_DIR = os.path.join(DATA_DIR, "outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
app.secret_key = os.environ.get("APP_SECRET", "change-me-" + uuid.uuid4().hex)

# Optional shared team password. Set APP_PASSWORD env var to enable the login gate.
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

# ---- Watched-folder worker (ping-driven + self-healing timer) ----
WATCH_LOG = []
WATCH_LOCK = threading.Lock()
WATCH_INTERVAL = int(os.environ.get("WATCH_INTERVAL_SECONDS", "60"))
_SCAN_LOCK = threading.Lock()     # prevents two scans overlapping
_LAST_SCAN = [0.0]                # timestamp of last scan start
_WATCHER_THREAD = [None]

def _watch_log(msg):
    with WATCH_LOCK:
        WATCH_LOG.append(msg)
        del WATCH_LOG[:-200]
    print(msg, flush=True)

def run_scan(reason="timer"):
    """Do one scan of the Input folder. Guarded so scans never overlap. Safe to call
    from the timer thread OR from the /ping and /scan-now endpoints."""
    api_key = os.environ.get("SEAMLESS_API_KEY", "")
    if not (core.watch_enabled() and api_key):
        return
    if not _SCAN_LOCK.acquire(blocking=False):
        return  # a scan is already running; skip
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
    """Start the timer thread if it isn't alive. Called at boot AND on every /ping,
    so even if Gunicorn recycles the worker and kills the thread, the next ping revives it."""
    if not (core.watch_enabled() and os.environ.get("SEAMLESS_API_KEY")):
        return
    t = _WATCHER_THREAD[0]
    if t is None or not t.is_alive():
        nt = threading.Thread(target=_watcher_loop, daemon=True)
        nt.start()
        _WATCHER_THREAD[0] = nt

ensure_watcher()


def classify(row):
    note = (row.get("Note") or "")
    if note.startswith("ERROR"):
        return "error"
    if note.startswith("No contact") or note.startswith("No contacts"):
        return "nomatch"
    if row.get("First Name") or row.get("Email"):
        return "found"
    return "nomatch"


def _classify_rows(rows):
    note = (rows[0].get("Note") or "") if rows else ""
    if note.startswith("ERROR"): return "error"
    if note.startswith("No contacts"): return "nocontacts"
    if note.startswith("No contact matched"): return "nomatch"
    return "found"

def run_job(job_id, input_path, api_key, cfg):
    def update(**kw):
        with JOBS_LOCK: JOBS[job_id].update(kw)
    def add_log(line):
        with JOBS_LOCK: JOBS[job_id]["log"].append(line)

    try:
        headers = core.auth_headers(api_key)
        all_companies = core.load_companies(input_path)
    except Exception as e:
        update(status="error", error=str(e)); return

    start = max(1, cfg["start"])
    companies = all_companies[start - 1:]
    if cfg["limit"]:
        companies = companies[:cfg["limit"]]
    preview = cfg["preview"]

    update(status="running", total=len(companies), current=0,
           found=0, nomatch=0, skipped=0, errors=0, contacts=0, preview=preview)
    mode = "PREVIEW (free, no credits)" if preview else "RUN (uses credits)"

    out_path = os.path.join(OUTPUT_DIR, f"{'preview' if preview else 'results'}_{job_id}.xlsx")
    results_by_company = {}
    counters = {"done": 0, "found": 0, "nomatch": 0, "errors": 0, "contacts": 0, "cached": 0}
    drive_link = None

    try:
        # Pull the PERMANENT master from Drive into the local cache first, so reuse
        # reflects everything the team has ever researched (survives free-tier resets).
        if core.drive_enabled() and not preview:
            pulled = core.sync_master_before_run()
            if pulled:
                add_log(f"Loaded {pulled} contacts from the permanent master in Drive.")

        # Save the uploaded INPUT to the shared Drive folder (if Drive is configured)
        if core.drive_enabled():
            import datetime as _dt
            stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M")
            link = core.drive_upload(input_path, drive_name=f"INPUT_{stamp}_{os.path.basename(input_path)}")
            if link:
                add_log(f"Input file saved to shared Drive: {link}")
        add_log(f"{mode}. {len(companies)} companies, up to {cfg['max_contacts']} contacts each, "
                f"{cfg['workers']} at a time.")

        def work(item):
            company, domain = item
            try:
                rows, kind = core.process_company(company, headers,
                                                  search_limit=cfg["search_limit"],
                                                  poll_interval=cfg["poll_interval"],
                                                  poll_attempts=cfg["poll_attempts"],
                                                  min_rank=cfg["min_rank"],
                                                  max_contacts=cfg["max_contacts"],
                                                  preview=preview,
                                                  company_domain=domain)
                return company, rows, kind
            except Exception as e:
                return company, [core.note_row(company, f"ERROR: {e}")], "error"

        with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
            futures = {ex.submit(work, item): item[0] for item in companies}
            for fut in as_completed(futures):
                company, rows, kind = fut.result()
                results_by_company[company] = rows
                counters["done"] += 1
                if kind == "found":
                    real = [r for r in rows if r.get("First Name") or r.get("Job Title")]
                    counters["found"] += 1; counters["contacts"] += len(real)
                elif kind == "cached":
                    real = [r for r in rows if r.get("First Name")]
                    counters["found"] += 1; counters["cached"] += len(real)
                elif kind == "error": counters["errors"] += 1
                else: counters["nomatch"] += 1
                update(current=counters["done"], found=counters["found"],
                       nomatch=counters["nomatch"], errors=counters["errors"],
                       contacts=counters["contacts"], cached=counters["cached"], company=company)
                n = len([r for r in rows if r.get('First Name')])
                src_tag = ' (from cache - free)' if kind == 'cached' else ''
                add_log(f"[{counters['done']}/{len(companies)}] {company} - "
                        f"{n if n else 'no'} contact(s) {'previewed' if preview else 'found'}{src_tag}")
                # incremental save in input order (companies is list of (name,domain) tuples)
                ordered = []
                for c, _ in companies:
                    if c in results_by_company:
                        ordered.extend(results_by_company[c])
                try: core.write_xlsx(ordered, out_path)
                except Exception: pass

        ordered = []
        for c, _ in companies:
            ordered.extend(results_by_company.get(c, []))
        core.write_xlsx(ordered, out_path)

        # Save the OUTPUT to the shared Drive folder (if Drive is configured)
        if core.drive_enabled():
            import datetime as _dt
            stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M")
            tag = "PREVIEW" if preview else "RESULTS"
            drive_link = core.drive_upload(out_path, drive_name=f"{tag}_{stamp}.xlsx")
            if drive_link:
                add_log(f"Output saved to shared Drive: {drive_link}")

        # Push the updated cache back to the PERMANENT master in Drive (accumulates forever)
        if core.drive_enabled() and not preview:
            mlink = core.sync_master_after_run()
            if mlink:
                add_log(f"Permanent master updated in Drive: {mlink}")

        if preview:
            add_log(f"Preview complete. ~{counters['contacts']} contacts would be researched "
                    f"(est. ~{counters['contacts']} credits) across {counters['found']} companies. "
                    f"{counters['nomatch']} no match. NOTHING SPENT.")
        else:
            add_log(f"Finished. {counters['contacts']} newly researched + {counters['cached']} reused "
                    f"from cache (saved ~{counters['cached']} credits) across {counters['found']} "
                    f"companies. {counters['nomatch']} no match, {counters['errors']} errors.")

    except Exception as e:
        add_log(f"ERROR: Job failed unexpectedly: {e}")
        update(status="error", error=str(e), company="")
        return

    finally:
        # ALWAYS mark the job as done so the browser stops polling.
        # This runs whether the job succeeded, partially completed, or hit an exception.
        with JOBS_LOCK:
            if JOBS[job_id]["status"] == "running":
                JOBS[job_id]["status"] = "done"
                JOBS[job_id]["company"] = ""
                JOBS[job_id]["drive_link"] = drive_link

    update(status="done", output=out_path, company="", drive_link=drive_link)

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

@app.route("/diag")
@login_required
def diag():
    try:
        return jsonify(core.diagnose())
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route("/ping")
def ping():
    # public endpoint the free cron pinger hits. Besides keeping the app awake, it
    # revives the watcher thread if Gunicorn recycled it, and kicks off a scan in the
    # background - so processing happens even if the long-lived thread died.
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
    # manual trigger to process the input folder immediately (doesn't wait for the timer)
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
        JOBS[job_id] = {"status": "queued", "total": 0, "current": 0, "company": "",
                        "found": 0, "nomatch": 0, "skipped": 0, "errors": 0, "contacts": 0, "cached": 0, "preview": False, "drive_link": None,
                        "log": [], "output": None, "error": None}

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
    # Instant response - just report the local cache count. The scheduled watcher
    # already keeps the local cache synced with the Drive master, so we don't need
    # to redo that expensive Drive round trip on every page load (doing so used to
    # timeout the Gunicorn worker when Google was slow).
    try:
        return jsonify(core.cache_stats())
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route("/status/<job_id>")
@login_required
def status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify(error="Unknown job."), 404
        return jsonify({
            "status": job["status"], "total": job["total"], "current": job["current"],
            "company": job["company"], "error": job["error"],
            "found": job["found"], "nomatch": job["nomatch"],
            "skipped": job["skipped"], "errors": job["errors"], "contacts": job["contacts"], "cached": job["cached"], "preview": job["preview"],
            "log": job["log"][-250:],
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


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BIM Data Execution</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root{
    --ink:#141a20; --ink2:#2a333d; --paper:#eef1f4; --panel:#ffffff; --line:#d3d9e0;
    --muted:#69727d; --cyan:#0996ab; --cyan-d:#0a7686; --grid:rgba(9,150,171,.09);
    --ok:#1f9d57; --okbg:#eef9f2; --warn:#b26a00; --err:#c0392b; --errbg:#fdeeec;
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{
    background:
      linear-gradient(var(--grid) 1px,transparent 1px) 0 0/26px 26px,
      linear-gradient(90deg,var(--grid) 1px,transparent 1px) 0 0/26px 26px,
      radial-gradient(1200px 500px at 70% -8%, #fff, var(--paper));
    color:var(--ink); font-family:"Inter",system-ui,sans-serif; line-height:1.5;
    -webkit-font-smoothing:antialiased; min-height:100vh;
  }
  .wrap{max-width:820px;margin:0 auto;padding:44px 22px 90px}

  header{display:flex;align-items:center;gap:18px;border-bottom:1px solid var(--line);
    padding-bottom:22px;margin-bottom:30px}
  .mark{flex:0 0 auto}
  .mark svg{display:block}
  .cube path{stroke:var(--cyan);stroke-width:1.6;fill:none;stroke-linejoin:round}
  .cube .face{fill:var(--cyan);opacity:.06}
  .htext .eyebrow{font-family:"IBM Plex Mono",monospace;font-size:11.5px;letter-spacing:.22em;
    text-transform:uppercase;color:var(--cyan-d)}
  h1{font-family:"Space Grotesk",sans-serif;font-weight:700;font-size:32px;letter-spacing:-.01em;margin:5px 0 3px}
  .sub{color:var(--muted);font-size:14.5px;max-width:56ch}

  .card{background:var(--panel);border:1px solid var(--line);border-radius:3px;
    box-shadow:0 1px 0 rgba(20,26,32,.03),0 10px 30px -24px rgba(20,26,32,.4);margin-bottom:20px}
  .card-h{display:flex;align-items:center;gap:10px;padding:15px 20px;border-bottom:1px solid var(--line);
    cursor:default}
  .card-h .n{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--cyan-d);
    border:1px solid var(--line);border-radius:2px;padding:2px 7px}
  .card-h h2{font-family:"Space Grotesk",sans-serif;font-size:15px;font-weight:600;margin:0}
  .card-h .chev{margin-left:auto;color:var(--muted);font-size:13px;user-select:none;cursor:pointer;
    font-family:"IBM Plex Mono",monospace}
  .card-b{padding:20px}
  .collapsible .card-h{cursor:pointer}
  .collapsed .card-b{display:none}

  label{display:block;font-family:"IBM Plex Mono",monospace;font-size:11px;letter-spacing:.09em;
    text-transform:uppercase;color:var(--muted);margin-bottom:7px}
  .fld{margin-bottom:18px}
  .fld:last-child{margin-bottom:0}
  input[type=text],input[type=password],input[type=number]{width:100%;padding:11px 12px;border:1px solid var(--line);
    border-radius:2px;font-family:"IBM Plex Mono",monospace;font-size:13px;background:#fff;color:var(--ink)}
  input:focus{outline:2px solid var(--cyan);outline-offset:1px;border-color:var(--cyan)}
  .keyrow{display:flex;gap:8px}
  .keyrow input{flex:1}
  .ghost{border:1px solid var(--line);background:#fff;border-radius:2px;padding:0 13px;cursor:pointer;
    font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--muted)}
  .ghost:hover{border-color:var(--cyan);color:var(--cyan-d)}
  .hint{font-size:12.5px;color:var(--muted);margin-top:7px}
  .hint b{color:var(--ink2);font-weight:600}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .set{background:#f0fbfc;border:1px solid #cfeef2;color:var(--cyan-d);font-size:12px;
    font-family:"IBM Plex Mono",monospace;padding:3px 8px;border-radius:2px}

  .drop{border:1.5px dashed var(--line);border-radius:3px;padding:30px 22px;text-align:center;cursor:pointer;
    transition:border-color .15s,background .15s;background:#fafcfd}
  .drop:hover,.drop.hot{border-color:var(--cyan);background:#f0fbfc}
  .drop strong{font-weight:600}
  .drop .h2{color:var(--muted);font-size:13px;margin-top:6px}
  .fname{margin-top:12px;font-family:"IBM Plex Mono",monospace;font-size:13px;color:var(--ink)}
  input[type=file]{display:none}

  button.run{width:100%;padding:15px;border:0;border-radius:3px;cursor:pointer;background:var(--ink);color:#fff;
    font-family:"Space Grotesk",sans-serif;font-weight:600;font-size:15px;letter-spacing:.01em;transition:background .15s}
  button.run:hover:not(:disabled){background:#000}
  button.run:disabled{opacity:.4;cursor:not-allowed}
  button.run.ghost-btn{background:#fff;color:var(--ink);border:1px solid var(--ink)}
  button.run.ghost-btn:hover:not(:disabled){background:#f3f6f9}
  .msg{margin-top:14px;font-size:13.5px;color:var(--err);min-height:1em}

  .stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px}
  .stat{background:var(--panel);border:1px solid var(--line);border-radius:3px;padding:13px 14px}
  .stat .v{font-family:"Space Grotesk",sans-serif;font-weight:700;font-size:26px;line-height:1}
  .stat .k{font-family:"IBM Plex Mono",monospace;font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;
    color:var(--muted);margin-top:6px}
  .stat.found .v{color:var(--ok)} .stat.err .v{color:var(--err)} .stat.total .v{color:var(--cyan-d)}

  .barwrap{height:9px;background:#dde3ea;border-radius:99px;overflow:hidden}
  .bar{height:100%;width:0;background:linear-gradient(90deg,var(--cyan-d),var(--cyan));transition:width .3s ease}
  .meta{display:flex;justify-content:space-between;margin:10px 0 16px;font-family:"IBM Plex Mono",monospace;
    font-size:12px;color:var(--muted)}
  .console{background:#0f141a;color:#c6d2df;border-radius:3px;padding:16px;height:300px;overflow:auto;
    font-family:"IBM Plex Mono",monospace;font-size:12.5px;line-height:1.7;white-space:pre-wrap;word-break:break-word;
    border:1px solid #0b0f14}
  .console .ok{color:#63d18c} .console .er{color:#ff8f7a} .console .dim{color:#7d8b99}
  .done{margin-top:20px;border:1px solid var(--ok);border-radius:3px;padding:18px;display:none;
    align-items:center;justify-content:space-between;gap:16px;background:var(--okbg)}
  .done.show{display:flex}
  .done a{background:var(--ok);color:#fff;text-decoration:none;padding:11px 18px;border-radius:2px;
    font-family:"Space Grotesk",sans-serif;font-size:14px;white-space:nowrap}
  .hidden{display:none}
  @media (max-width:620px){.grid2,.stats{grid-template-columns:1fr 1fr}}
  @media (prefers-reduced-motion:reduce){*{transition:none!important;animation:none!important}}
  .cube .spin{transform-origin:32px 34px;animation:spin 22s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="mark">
      <svg class="cube" width="64" height="68" viewBox="0 0 64 68" aria-hidden="true">
        <g class="spin">
          <path class="face" d="M32 6 L56 20 L32 34 L8 20 Z"/>
          <path class="face" d="M8 20 L32 34 L32 62 L8 48 Z"/>
          <path class="face" d="M56 20 L32 34 L32 62 L56 48 Z"/>
          <path d="M32 6 L56 20 L32 34 L8 20 Z M8 20 L8 48 L32 62 L56 48 L56 20 M32 34 L32 62"/>
        </g>
      </svg>
    </div>
    <div class="htext">
      <div class="eyebrow">BMSI &middot; Lead Generation Engine</div>
      <h1>BIM Data Execution</h1>
      <p class="sub">Upload a spreadsheet of companies. For each one we find <b>every contact in the BMSI hierarchy</b>,
        grouped by tier, with name, title, email, phone and LinkedIn - returned as a clean Excel.</p>
    </div>
  </header>

  <div id="masterbar" style="display:flex;align-items:center;gap:12px;margin:-14px 0 26px;
       font-family:'IBM Plex Mono',monospace;font-size:12.5px;color:var(--muted)">
    <span id="masterinfo">Master store: loading&hellip;</span>
    <a href="/export-master" style="color:var(--cyan-d);text-decoration:none;border:1px solid var(--line);
       padding:4px 10px;border-radius:2px">Export master to Excel</a>
  </div>

  <!-- CONFIGURATION -->
  <div class="card collapsible" id="cfgCard">
    <div class="card-h" id="cfgToggle">
      <span class="n">01</span><h2>Configuration</h2>
      {% if api_key_set %}<span class="set">server key loaded</span>{% endif %}
      <span class="chev" id="cfgChev">[ hide ]</span>
    </div>
    <div class="card-b">
      <div class="fld">
        <label for="apikey">Seamless API key</label>
        <div class="keyrow">
          <input type="password" id="apikey" autocomplete="off"
            placeholder="{% if api_key_set %}Using server key - leave blank, or paste to override{% else %}Paste your Seamless API key{% endif %}">
          <button type="button" class="ghost" id="toggleKey">show</button>
        </div>
        <div class="hint">Sent in the <b>token</b> header. Not stored - it lives only in this browser session.</div>
      </div>
      <div class="grid2">
        <div class="fld">
          <label for="limit">Candidates to rank</label>
          <input type="number" id="limit" min="1" max="25" value="{{ default_limit }}">
          <div class="hint">How many people to pull &amp; rank per company (from the free search). Keep at 25.</div>
        </div>
        <div class="fld">
          <label for="maxc">Max contacts per company</label>
          <input type="number" id="maxc" min="1" max="15" value="{{ default_maxc }}">
          <div class="hint">How many hierarchy contacts to <b>research</b> per company. <b>Each one costs ~1 credit.</b> 5 = up to 5 people per company.</div>
        </div>
        <div class="fld">
          <label for="workers">Parallel companies (speed)</label>
          <input type="number" id="workers" min="1" max="12" value="6">
          <div class="hint">How many companies to process at once. Higher = faster. 6 is a safe default.</div>
        </div>
        <div class="fld">
          <label for="pollint">Poll interval (sec)</label>
          <input type="number" id="pollint" min="5" max="120" value="{{ default_interval }}">
          <div class="hint">How long to wait between research checks.</div>
        </div>
      </div>
      <div class="fld" style="margin-top:2px">
        <label for="pollatt">Max poll attempts</label>
        <input type="number" id="pollatt" min="3" max="60" value="{{ default_attempts }}">
        <div class="hint">Give up on a company's research after this many checks.</div>
      </div>
      <div style="height:1px;background:var(--line);margin:22px 0"></div>
      <div class="fld">
        <label for="minrank">Credit saver &middot; match quality to research</label>
        <select id="minrank">
          <option value="999">Any match (research everyone in the hierarchy)</option>
          <option value="45">Skip executives (no VP/President/CEO/Owner)</option>
          <option value="37">Estimators &amp; Project roles &amp; above</option>
          <option value="27">Estimators &amp; above (skip PM/engineering/ops/exec)</option>
          <option value="8">BIM/VDC/CAD roles only</option>
        </select>
        <div class="hint">Companies whose best contact is weaker than this are <b>skipped for free</b> - no credit spent.</div>
      </div>
      <div class="grid2">
        <div class="fld">
          <label for="startrow">Start at company #</label>
          <input type="number" id="startrow" min="1" value="1">
          <div class="hint">For batching, e.g. start at 101 for the second batch.</div>
        </div>
        <div class="fld">
          <label for="maxco">Max companies this run</label>
          <input type="number" id="maxco" min="0" value="0" placeholder="0 = all">
          <div class="hint">0 = all remaining. Set 100 to cap a batch.</div>
        </div>
      </div>
    </div>
  </div>

  <!-- UPLOAD -->
  <div class="card" id="uploadCard">
    <div class="card-h"><span class="n">02</span><h2>Upload &amp; Run</h2></div>
    <div class="card-b">
      <div class="drop" id="drop">
        <strong>Choose a file</strong> or drag it here
        <div class="h2">Excel (.xlsx) or CSV &middot; must have a column named "Company"</div>
        <div class="fname" id="fname"></div>
      </div>
      <input type="file" id="file" accept=".xlsx,.xlsm,.csv,.tsv">
      <div style="display:flex;gap:10px;margin-top:22px">
        <button class="run ghost-btn" id="preview" disabled style="flex:1">Preview &middot; free</button>
        <button class="run" id="go" disabled style="flex:1">Run &middot; uses credits</button>
      </div>
      <div class="msg" id="msg"></div>
    </div>
  </div>

  <!-- RUN / RESULTS -->
  <div class="hidden" id="run">
    <div class="stats">
      <div class="stat total"><div class="v" id="s_done">0</div><div class="k">Companies</div></div>
      <div class="stat found"><div class="v" id="s_contacts">0</div><div class="k" id="k_found">New contacts</div></div>
      <div class="stat"><div class="v" id="s_cached">0</div><div class="k">Reused free</div></div>
      <div class="stat"><div class="v" id="s_nomatch">0</div><div class="k">No match</div></div>
      <div class="stat err"><div class="v" id="s_err">0</div><div class="k">Errors</div></div>
    </div>
    <div class="barwrap"><div class="bar" id="bar"></div></div>
    <div class="meta"><span id="stage">Starting&hellip;</span><span id="count"></span></div>
    <div class="console" id="console"></div>
    <div class="done" id="done">
      <span>Your contacts file is ready.</span>
      <a id="dl" href="#">Download Excel</a>
    </div>
  </div>
</div>

<script>
  // collapse config
  const cfgCard=document.getElementById('cfgCard'), cfgChev=document.getElementById('cfgChev');
  document.getElementById('cfgToggle').addEventListener('click',()=>{
    cfgCard.classList.toggle('collapsed');
    cfgChev.textContent = cfgCard.classList.contains('collapsed') ? '[ edit ]' : '[ hide ]';
  });
  // show/hide key
  const apikeyEl=document.getElementById('apikey'), tk=document.getElementById('toggleKey');
  tk.addEventListener('click',()=>{const p=apikeyEl.type==='password';apikeyEl.type=p?'text':'password';tk.textContent=p?'hide':'show';});

  const fileInput=document.getElementById('file'), drop=document.getElementById('drop'),
        fname=document.getElementById('fname'), go=document.getElementById('go'), msg=document.getElementById('msg');
  drop.addEventListener('click',()=>fileInput.click());
  drop.addEventListener('dragover',e=>{e.preventDefault();drop.classList.add('hot');});
  drop.addEventListener('dragleave',()=>drop.classList.remove('hot'));
  drop.addEventListener('drop',e=>{e.preventDefault();drop.classList.remove('hot');
    if(e.dataTransfer.files.length){fileInput.files=e.dataTransfer.files;onFile();}});
  fileInput.addEventListener('change',onFile);
  function onFile(){if(fileInput.files.length){fname.textContent=fileInput.files[0].name;go.disabled=false;document.getElementById('preview').disabled=false;}}

  const preBtn=document.getElementById('preview');
  function collect(previewFlag){
    const fd=new FormData();
    fd.append('file',fileInput.files[0]);
    if(apikeyEl.value) fd.append('api_key',apikeyEl.value);
    fd.append('search_limit',document.getElementById('limit').value);
    fd.append('max_contacts',document.getElementById('maxc').value);
    fd.append('workers',document.getElementById('workers').value);
    fd.append('poll_interval',document.getElementById('pollint').value);
    fd.append('poll_attempts',document.getElementById('pollatt').value);
    fd.append('min_rank',document.getElementById('minrank').value);
    fd.append('start',document.getElementById('startrow').value||'1');
    fd.append('limit_companies',document.getElementById('maxco').value||'0');
    fd.append('preview',previewFlag?'1':'0');
    return fd;
  }
  async function submit(previewFlag){
    msg.textContent='';
    if(!fileInput.files.length){msg.textContent='Choose a file first.';return;}
    go.disabled=true; preBtn.disabled=true;
    (previewFlag?preBtn:go).textContent='Uploading\u2026';
    try{
      const r=await fetch('/upload',{method:'POST',body:collect(previewFlag)});
      const data=await r.json();
      if(!r.ok) throw new Error(data.error||'Upload failed.');
      document.getElementById('uploadCard').style.display='none';
      cfgCard.classList.add('collapsed'); cfgChev.textContent='[ edit ]';
      document.getElementById('run').classList.remove('hidden');
      poll(data.job_id);
    }catch(e){msg.textContent=e.message;go.disabled=false;preBtn.disabled=false;
      go.textContent='Run \u00b7 uses credits';preBtn.textContent='Preview \u00b7 free';}
  }
  go.addEventListener('click',()=>submit(false));
  preBtn.addEventListener('click',()=>submit(true));

  const bar=document.getElementById('bar'),stage=document.getElementById('stage'),count=document.getElementById('count'),
        cons=document.getElementById('console'),doneBox=document.getElementById('done'),dl=document.getElementById('dl');
  const S={done:document.getElementById('s_done'),contacts:document.getElementById('s_contacts'),
           cached:document.getElementById('s_cached'),nomatch:document.getElementById('s_nomatch'),
           err:document.getElementById('s_err')};
  // show master store size
  fetch('/master-stats').then(r=>r.json()).then(d=>{
    if(d && d.contacts!=null){document.getElementById('masterinfo').textContent=
      'Master store: '+d.contacts+' contacts across '+d.companies+' companies (reused free).';}
  }).catch(()=>{document.getElementById('masterinfo').textContent='Master store: empty (builds as you run).';});

  function render(log){
    cons.innerHTML=log.map(l=>{
      const cls=/ERROR|error/.test(l)?'er':(/Finished|complete|ready|NOTHING SPENT/.test(l)?'ok':(/no contacts|no match|Skipped|PREVIEW|Loaded|RUN|Quality/.test(l)?'dim':''));
      return '<div class="'+cls+'">'+l.replace(/</g,'&lt;')+'</div>';
    }).join('');
    cons.scrollTop=cons.scrollHeight;
  }
  async function poll(jobId){
    try{
      const r=await fetch('/status/'+jobId); const s=await r.json();
      render(s.log||[]);
      S.done.textContent=s.current||0; S.contacts.textContent=s.contacts||0;
      S.cached.textContent=s.cached||0; S.nomatch.textContent=s.nomatch||0; S.err.textContent=s.errors||0;
      document.getElementById('k_found').textContent = s.preview ? 'Would research' : 'New contacts';
      if(s.total){bar.style.width=Math.round((s.current/s.total)*100)+'%';count.textContent=s.current+' / '+s.total;}
      if(s.status==='running') stage.textContent=(s.preview?'Previewing: ':'Processing: ')+(s.company||'\u2026');
      else if(s.status==='queued') stage.textContent='Queued\u2026';
      else if(s.status==='error'){stage.textContent='Stopped: '+(s.error||'error');return;}
      else if(s.status==='done'){
        stage.textContent=s.preview?'Preview complete':'Complete'; bar.style.width='100%';
        dl.href=s.download;
        var baseMsg = s.preview
          ? ('Estimated cost: ~'+(s.contacts||0)+' credits for '+(s.contacts||0)+' contacts. Nothing spent - Run when ready.')
          : 'Your contacts file is ready.';
        if(s.drive_link){ baseMsg += '  Also saved to the shared Drive.'; }
        doneBox.querySelector('span').innerHTML = baseMsg +
          (s.drive_link ? ' <a href="'+s.drive_link+'" target="_blank" style="color:#0a7686">Open in Drive</a>' : '');
        dl.textContent = s.preview ? 'Download preview' : 'Download Excel';
        doneBox.classList.add('show'); return;
      }
      setTimeout(()=>poll(jobId),2000);
    }catch(e){stage.textContent='Lost connection to the server. Is it still running?';}
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