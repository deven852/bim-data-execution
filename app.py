#!/usr/bin/env python3
"""BIM Data Execution - Web App (clean rewrite v5)"""

import os, uuid, threading, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from functools import wraps
from flask import Flask, request, jsonify, send_file, redirect, url_for, render_template_string, session, abort
from werkzeug.utils import secure_filename
import bim_data_execution as core

APP_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.environ.get("DATA_DIR", APP_DIR)
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
OUTPUT_DIR = os.path.join(DATA_DIR, "outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
app.secret_key = os.environ.get("APP_SECRET", "bim-secret-" + uuid.uuid4().hex)
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

def login_required(fn):
    @wraps(fn)
    def wrapper(*a, **k):
        if APP_PASSWORD and not session.get("authed"):
            if request.path.startswith(("/status", "/master-stats")):
                return jsonify(error="Not authenticated"), 401
            return redirect(url_for("login"))
        return fn(*a, **k)
    return wrapper

JOBS = {}
JOBS_LOCK = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# Timeout helper: daemon thread + Event so a hung call never blocks the caller
# ──────────────────────────────────────────────────────────────────────────────
def _with_timeout(fn, secs, *args, **kwargs):
    result = [None]; error = [None]; done = threading.Event()
    def _run():
        try: result[0] = fn(*args, **kwargs)
        except Exception as e: error[0] = e
        finally: done.set()
    threading.Thread(target=_run, daemon=True).start()
    if not done.wait(secs):
        raise TimeoutError(f"{fn.__name__} timed out after {secs}s")
    if error[0]: raise error[0]
    return result[0]

# ──────────────────────────────────────────────────────────────────────────────
# Job runner
# ──────────────────────────────────────────────────────────────────────────────
def run_job(job_id, input_path, api_key, cfg):
    def log(msg):
        print(f"[job {job_id}] {msg}", flush=True)
        with JOBS_LOCK:
            JOBS[job_id]["log"].append(msg)

    def upd(**kw):
        with JOBS_LOCK:
            JOBS[job_id].update(kw)

    out_path = os.path.join(OUTPUT_DIR, f"results_{job_id}.xlsx")
    drive_link = None

    try:
        # ── key fingerprint so we can verify in logs ──────────────────────────
        fp = f"{api_key[:4]}...{api_key[-4:]} (len={len(api_key)})" if api_key else "EMPTY"
        log(f"API key: {fp}")

        # ── load companies ─────────────────────────────────────────────────────
        log("Loading company list...")
        try:
            all_companies = core.load_companies(input_path)
        except Exception as e:
            log(f"ERROR loading file: {e}")
            upd(status="error", error=str(e))
            return

        start = max(1, cfg["start"])
        companies = all_companies[start - 1:]
        if cfg["limit"]:
            companies = companies[:cfg["limit"]]

        log(f"Loaded {len(companies)} companies.")
        upd(status="running", total=len(companies), current=0,
            found=0, nomatch=0, errors=0, contacts=0, cached=0)

        log(f"Processing {len(companies)} companies 1 at a time "
            f"(up to {cfg['max_contacts']} contacts each)...")

        headers = core.auth_headers(api_key)

        # ── Drive: try once, skip all Drive if it fails ────────────────────────
        drive_ok = core.drive_enabled()
        if drive_ok:
            try:
                pulled = _with_timeout(core.sync_master_before_run, 12)
                if pulled:
                    log(f"Drive: loaded {pulled} cached contacts.")
            except Exception as e:
                log(f"Drive unavailable this run: {e}")
                drive_ok = False

        # ── process companies sequentially ─────────────────────────────────────
        results_by_company = {}
        counts = {"done": 0, "found": 0, "nomatch": 0, "errors": 0, "contacts": 0, "cached": 0}

        for company, domain in companies:
            upd(company=company)
            log(f"[{counts['done']+1}/{len(companies)}] {company} ...")
            try:
                rows, kind = core.process_company(
                    company, headers,
                    search_limit   = cfg["search_limit"],
                    poll_interval  = cfg["poll_interval"],
                    poll_attempts  = cfg["poll_attempts"],
                    min_rank       = cfg["min_rank"],
                    max_contacts   = cfg["max_contacts"],
                    preview        = False,
                    company_domain = domain)
            except Exception as e:
                log(f"  ERROR: {e}")
                rows = [core.note_row(company, f"ERROR: {e}")]
                kind = "error"

            results_by_company[company] = rows
            counts["done"] += 1

            real = [r for r in rows if r.get("First Name")]
            n = len(real)
            if kind == "cached":
                counts["found"] += 1; counts["cached"] += n
                log(f"  {n} contact(s) from cache (free)")
            elif kind == "found":
                counts["found"] += 1; counts["contacts"] += n
                log(f"  {n} contact(s) researched")
            elif kind == "error":
                counts["errors"] += 1
            else:
                counts["nomatch"] += 1
                log(f"  No match")

            upd(current=counts["done"], found=counts["found"],
                nomatch=counts["nomatch"], errors=counts["errors"],
                contacts=counts["contacts"], cached=counts["cached"])

            # incremental save
            ordered = []
            for c, _ in companies:
                if c in results_by_company:
                    ordered.extend(results_by_company[c])
            try: core.write_xlsx(ordered, out_path)
            except Exception: pass

        # ── final write ────────────────────────────────────────────────────────
        ordered = []
        for c, _ in companies:
            ordered.extend(results_by_company.get(c, []))
        core.write_xlsx(ordered, out_path)

        # ── Drive: save output ─────────────────────────────────────────────────
        if drive_ok:
            try:
                stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                drive_link = _with_timeout(core.drive_upload, 20, out_path,
                                           drive_name=f"RESULTS_{stamp}.xlsx")
                if drive_link:
                    log(f"Output saved to Drive: {drive_link}")
            except Exception as e:
                log(f"Drive output upload skipped: {e}")

            try:
                mlink = _with_timeout(core.sync_master_after_run, 20)
                if mlink:
                    log(f"Drive master updated: {mlink}")
            except Exception as e:
                log(f"Drive master push skipped: {e}")

        log(f"Done. {counts['contacts']} new + {counts['cached']} cached contacts "
            f"across {counts['found']} companies. "
            f"{counts['nomatch']} no-match, {counts['errors']} errors.")
        upd(status="done", output=out_path, company="", drive_link=drive_link)

    except Exception as e:
        log(f"FATAL: {e}")
        upd(status="error", error=str(e), company="")
    finally:
        with JOBS_LOCK:
            if JOBS[job_id]["status"] == "running":
                JOBS[job_id]["status"] = "done"
                JOBS[job_id]["company"] = ""
                JOBS[job_id]["output"] = out_path
                JOBS[job_id]["drive_link"] = drive_link


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
LOGIN_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>BIM Data Execution</title>
<style>body{font-family:system-ui;background:#050704;display:flex;min-height:100vh;
align-items:center;justify-content:center;margin:0;color:#d4d8c8}
.box{background:#111;border:1px solid rgba(184,201,90,.2);border-radius:12px;
padding:36px;width:340px}h1{font-size:22px;margin:0 0 6px;color:#d4e17a}
p{color:#8a9070;font-size:14px;margin:0 0 20px}
input{width:100%;padding:12px;border:1px solid rgba(184,201,90,.2);border-radius:8px;
background:#1a2016;color:#d4d8c8;font-size:14px;box-sizing:border-box}
button{width:100%;margin-top:14px;padding:13px;border:0;border-radius:8px;
background:linear-gradient(135deg,#8ca83c,#b8c95a);color:#050704;font-size:15px;
font-weight:600;cursor:pointer}.err{color:#a34848;font-size:13px;margin-top:10px}</style>
</head><body><form class="box" method="post">
<h1>BIM Data Execution</h1><p>Enter the team password to continue.</p>
<input type="password" name="password" placeholder="Password" autofocus>
<button>Sign in</button>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
</form></body></html>"""

@app.route("/login", methods=["GET","POST"])
def login():
    if not APP_PASSWORD: return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["authed"] = True; return redirect(url_for("index"))
        error = "Incorrect password."
    return render_template_string(LOGIN_HTML, error=error)

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))

@app.route("/ping")
def ping():
    return jsonify(ok=True, ts=int(__import__("time").time()))

@app.route("/test-seamless")
@login_required
def test_seamless():
    import time as _t
    api_key = (os.environ.get("SEAMLESS_API_KEY") or "").strip()
    if not api_key:
        return jsonify(ok=False, error="SEAMLESS_API_KEY not set on Render."), 500
    try:
        headers = core.auth_headers(api_key)
        t0 = _t.time()
        cands = core.search_candidates("Microsoft", headers, limit=3)
        elapsed = round(_t.time() - t0, 2)
        return jsonify(ok=True, elapsed_seconds=elapsed, candidates_returned=len(cands),
                       message="Seamless is working.")
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/debug-research")
@login_required
def debug_research():
    """Runs a full search + research on ONE company and returns the RAW Seamless
    responses so we can see exactly what fields Seamless is using for phone/email.
    Uses ~5 credits."""
    import time as _t
    company = request.args.get("company", "Microsoft")
    api_key = (os.environ.get("SEAMLESS_API_KEY") or "").strip()
    if not api_key:
        return jsonify(error="SEAMLESS_API_KEY not set"), 500
    try:
        headers = core.auth_headers(api_key)
        # 1) SEARCH
        cands = core.search_candidates(company, headers, limit=5)
        if not cands:
            return jsonify(step="search", got=0, note="No candidates returned")
        first = cands[0]
        # 2) RESEARCH one candidate
        cid = first.get("searchResultId") or first.get("searchResultID") or first.get("id")
        if not cid:
            return jsonify(step="search_returned", candidate_raw=first,
                           note="No id on candidate to research")
        req_ids = core.research_ids([cid], headers)
        # 3) POLL - do our own poll and return the raw response
        _t.sleep(3)
        import requests
        params = {"requestIds": ",".join(str(x) for x in req_ids)}
        r = requests.get(core.EP_POLL, headers=headers, params=params, timeout=30)
        raw = r.json()
        # Try a couple more polls if not done
        for _ in range(4):
            done = False
            data = raw.get("data") or []
            if data and all(d.get("status") in ("done","error","missing","duplicate")
                            for d in data):
                done = True; break
            _t.sleep(4)
            r = requests.get(core.EP_POLL, headers=headers, params=params, timeout=30)
            raw = r.json()

        return jsonify(
            step="research_complete",
            search_candidate=first,
            research_raw=raw,
            what_we_extract={
                "email": core._find_email(raw.get("data",[{}])[0] if raw.get("data") else {}),
                "phone": core._find_phone(raw.get("data",[{}])[0] if raw.get("data") else {}),
                "linkedin": core._find_linkedin(raw.get("data",[{}])[0] if raw.get("data") else {}),
            },
            note="Look at 'research_raw' to see all fields Seamless returned. If email/phone are hiding under a field name we don't check, that's what we need to add."
        )
    except Exception as e:
        return jsonify(step="error", error=str(e)), 500


@app.route("/master-stats")
@login_required
def master_stats():
    try: return jsonify(core.cache_stats())
    except Exception as e: return jsonify(error=str(e)), 500

@app.route("/export-master")
@login_required
def export_master():
    path = os.path.join(OUTPUT_DIR, "bim_master_export.xlsx")
    try: core.export_master_xlsx(path)
    except Exception as e: return jsonify(error=str(e)), 500
    return send_file(path, as_attachment=True, download_name="bim_master_export.xlsx")

@app.route("/purge-phoneless", methods=["GET","POST"])
@login_required
def purge_phoneless():
    try:
        deleted, remaining = core.cache_purge_phoneless()
        return jsonify(deleted=deleted, remaining=remaining,
                       message=f"Removed {deleted} phoneless row(s). {remaining} good row(s) remain.")
    except Exception as e: return jsonify(error=str(e)), 500

@app.route("/status/<job_id>")
@login_required
def status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job: return jsonify(error="Unknown job."), 404
        return jsonify({
            "status":   job["status"],  "total":    job["total"],
            "current":  job["current"], "company":  job["company"],
            "error":    job["error"],   "found":    job["found"],
            "nomatch":  job["nomatch"], "errors":   job["errors"],
            "contacts": job["contacts"],"cached":   job["cached"],
            "log":      job["log"][-300:],
            "download": f"/download/{job_id}" if job["status"] == "done" and job["output"] else None,
            "drive_link": job.get("drive_link"),
        })

@app.route("/download/<job_id>")
@login_required
def download(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job or not job.get("output"): abort(404)
    return send_file(job["output"], as_attachment=True,
                     download_name=f"contacts_{job_id}.xlsx")

def _int(form, name, default, lo=1, hi=999999):
    try: return max(lo, min(hi, int(form.get(name, default))))
    except: return default

@app.route("/upload", methods=["POST"])
@login_required
def upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(error="Choose an Excel (.xlsx) or CSV file first."), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in (".xlsx", ".xlsm", ".csv", ".tsv"):
        return jsonify(error="Unsupported file type. Use .xlsx or .csv"), 400

    # ALWAYS prefer the server-side env var — never use a browser-supplied key
    api_key = (os.environ.get("SEAMLESS_API_KEY") or "").strip()
    if not api_key:
        return jsonify(error="SEAMLESS_API_KEY is not set on Render. "
                             "Add it in Render → Environment."), 400

    cfg = {
        "search_limit":  _int(request.form, "search_limit",  core.SEARCH_LIMIT, 1, 25),
        "poll_interval": _int(request.form, "poll_interval", core.POLL_INTERVAL_SECONDS, 3, 120),
        "poll_attempts": _int(request.form, "poll_attempts", core.POLL_MAX_ATTEMPTS, 3, 60),
        "min_rank":      _int(request.form, "min_rank", 999, 1, 999),
        "max_contacts":  _int(request.form, "max_contacts", core.MAX_CONTACTS_PER_COMPANY, 1, 10),
        "start":         _int(request.form, "start", 1, 1, 100000),
        "limit":         _int(request.form, "limit_companies", 0, 0, 200),
    }

    job_id = uuid.uuid4().hex[:12]
    saved = os.path.join(UPLOAD_DIR, f"{job_id}_{secure_filename(f.filename)}")
    f.save(saved)

    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "queued", "total": 0, "current": 0, "company": "",
            "found": 0, "nomatch": 0, "errors": 0, "contacts": 0, "cached": 0,
            "log": [], "output": None, "error": None, "drive_link": None,
        }

    threading.Thread(target=run_job, args=(job_id, saved, api_key, cfg), daemon=True).start()
    return jsonify(job_id=job_id)

@app.route("/")
@login_required
def index():
    return render_template_string(PAGE,
        api_key_set=bool(os.environ.get("SEAMLESS_API_KEY")),
        default_limit=core.SEARCH_LIMIT,
        default_interval=core.POLL_INTERVAL_SECONDS,
        default_attempts=core.POLL_MAX_ATTEMPTS,
        default_maxc=core.MAX_CONTACTS_PER_COMPANY)


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BIM Data Execution</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#050704; --bg1:#0a0d08; --bg2:#111610; --bg3:#1a2016;
  --o1:#6b8020; --o2:#8ca83c; --o3:#b8c95a; --o4:#d4e17a; --o5:#f5f9d4;
  --ink:#e8ecdc; --ink2:#d4d8c8; --dim:rgba(232,236,220,.55);
  --muted:#8a9070; --err:#a34848;
  --line:rgba(184,201,90,.14); --line2:rgba(184,201,90,.25);
}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{
  background:var(--bg);
  color:var(--ink2);
  font-family:"Space Grotesk",system-ui,sans-serif;
  font-weight:400;
  line-height:1.55;
  -webkit-font-smoothing:antialiased;
  min-height:100vh;
  overflow-x:hidden;
}
/* Aurora background */
body::before,body::after{
  content:"";position:fixed;border-radius:50%;pointer-events:none;z-index:0;filter:blur(80px);
}
body::before{top:-150px;left:-150px;width:550px;height:550px;
  background:radial-gradient(circle,rgba(140,168,60,.22) 0%,transparent 65%);
  animation:orb1 20s ease-in-out infinite;}
body::after{bottom:-200px;right:-150px;width:650px;height:650px;
  background:radial-gradient(circle,rgba(184,201,90,.16) 0%,transparent 65%);
  animation:orb2 25s ease-in-out infinite;}
.particles{position:fixed;inset:0;pointer-events:none;z-index:0;}
.pt{position:absolute;border-radius:50%;background:var(--o3);
  box-shadow:0 0 10px var(--o3),0 0 20px rgba(184,201,90,.3);}
.pt:nth-child(1){width:3px;height:3px;top:12%;left:8%;animation:drift 14s ease-in-out infinite;}
.pt:nth-child(2){width:4px;height:4px;top:30%;left:90%;animation:drift2 17s ease-in-out infinite;}
.pt:nth-child(3){width:3px;height:3px;top:68%;left:14%;animation:drift 11s ease-in-out infinite reverse;}
.pt:nth-child(4){width:2px;height:2px;top:22%;left:68%;animation:drift2 16s ease-in-out infinite;}
.pt:nth-child(5){width:4px;height:4px;top:82%;left:58%;animation:drift 19s ease-in-out infinite;}
.pt:nth-child(6){width:3px;height:3px;top:55%;left:86%;animation:drift2 15s ease-in-out infinite;}
.wrap{max-width:820px;margin:0 auto;padding:48px 24px 80px;position:relative;z-index:2;}

/* Glass panel */
.glass{
  background:linear-gradient(145deg,rgba(255,255,255,.04),rgba(255,255,255,.01));
  backdrop-filter:blur(20px) saturate(1.3);
  -webkit-backdrop-filter:blur(20px) saturate(1.3);
  border:1px solid var(--line2);border-radius:14px;
  position:relative;overflow:hidden;
}
.glass::before{
  content:"";position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(212,225,122,.35),transparent);
}

/* Hero */
.hero{display:flex;align-items:center;gap:40px;margin-bottom:32px;}
.hero-svg{width:260px;height:280px;flex:0 0 auto;
  animation:float 6s ease-in-out infinite,glowPulse 4s ease-in-out infinite;}
.eyebrow{font-family:"JetBrains Mono",monospace;font-size:10.5px;
  letter-spacing:.4em;text-transform:uppercase;color:var(--o3);margin-bottom:14px;}
.eyebrow .dot{display:inline-block;width:6px;height:6px;border-radius:50%;
  background:var(--o3);box-shadow:0 0 10px var(--o3);vertical-align:middle;
  margin-right:9px;animation:pulse 1.6s ease-in-out infinite;}
h1{font-weight:600;font-size:48px;letter-spacing:-.03em;margin:0 0 12px;line-height:1;
  background:linear-gradient(120deg,#fff 0%,var(--o5) 30%,var(--o4) 60%,var(--o3) 100%);
  background-size:200% auto;-webkit-background-clip:text;background-clip:text;
  -webkit-text-fill-color:transparent;color:transparent;
  animation:shimmer 8s ease-in-out infinite;}
.sub{color:var(--dim);font-size:14px;margin:0 0 18px;max-width:44ch;line-height:1.6;font-weight:400;}
.sub b{color:var(--o3);font-weight:500;}
.tiers{display:flex;gap:8px;flex-wrap:wrap;}
.tier{display:inline-flex;align-items:center;gap:6px;padding:5px 12px;border-radius:99px;
  font-family:"JetBrains Mono",monospace;font-size:10.5px;}
.t1{background:rgba(212,225,122,.06);border:1px solid rgba(212,225,122,.2);color:var(--o4);}
.t2{background:rgba(184,201,90,.06);border:1px solid rgba(184,201,90,.2);color:var(--o3);}
.t3{background:rgba(140,168,60,.06);border:1px solid rgba(140,168,60,.2);color:var(--o2);}
.tier .b{width:5px;height:5px;border-radius:50%;}
.t1 .b{background:var(--o4);box-shadow:0 0 6px var(--o4);}
.t2 .b{background:var(--o3);box-shadow:0 0 6px var(--o3);}
.t3 .b{background:var(--o2);box-shadow:0 0 6px var(--o2);}

/* Ticker */
.ticker-wrap{overflow:hidden;margin:0 0 28px;padding:13px 0;
  border-top:1px solid var(--line);border-bottom:1px solid var(--line);position:relative;}
.ticker-wrap::before,.ticker-wrap::after{content:"";position:absolute;top:0;bottom:0;width:80px;
  z-index:2;pointer-events:none;}
.ticker-wrap::before{left:0;background:linear-gradient(90deg,var(--bg),transparent);}
.ticker-wrap::after{right:0;background:linear-gradient(270deg,var(--bg),transparent);}
.ticker{display:flex;gap:48px;white-space:nowrap;animation:tick 40s linear infinite;
  font-family:"JetBrains Mono",monospace;font-size:12px;color:var(--dim);}
.ticker .n{color:var(--o4);}
.ticker .sep{color:rgba(184,201,90,.22);}

/* Master bar */
#masterbar{display:flex;align-items:center;gap:12px;margin:0 0 20px;
  font-family:"JetBrains Mono",monospace;font-size:12px;color:var(--dim);}
#masterbar .d{width:6px;height:6px;border-radius:50%;background:var(--o3);
  box-shadow:0 0 10px var(--o3);animation:pulse 1.6s ease-in-out infinite;}
#masterbar a{margin-left:auto;padding:6px 14px;border:1px solid var(--line2);
  border-radius:99px;color:var(--o3);text-decoration:none;transition:all .2s;}
#masterbar a:hover{background:rgba(184,201,90,.06);}

/* Config card */
.card{margin-bottom:18px;}
.card-h{display:flex;align-items:center;gap:12px;padding:16px 22px;
  border-bottom:1px solid var(--line);cursor:pointer;}
.card-h .num{font-family:"JetBrains Mono",monospace;font-size:10.5px;color:var(--o4);
  border:1px solid rgba(212,225,122,.25);padding:3px 9px;border-radius:6px;
  background:rgba(212,225,122,.04);}
.card-h h2{font-size:15px;font-weight:500;margin:0;color:var(--ink);}
.card-h .chev{margin-left:auto;color:var(--muted);font-family:"JetBrains Mono",monospace;
  font-size:11.5px;}
.card-b{padding:22px;}
.collapsed .card-b{display:none;}
.set{display:inline-flex;align-items:center;gap:6px;
  background:linear-gradient(135deg,rgba(212,225,122,.15),rgba(184,201,90,.08));
  border:1px solid rgba(212,225,122,.3);color:var(--o4);
  font-family:"JetBrains Mono",monospace;font-size:10.5px;padding:4px 12px;border-radius:99px;}
.set::before{content:"";width:5px;height:5px;border-radius:50%;background:var(--o4);
  box-shadow:0 0 6px var(--o4);}
label{display:block;font-family:"JetBrains Mono",monospace;font-size:10.5px;
  letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:9px;}
.fld{margin-bottom:20px;}
.fld:last-child{margin-bottom:0;}
input[type=number],select{width:100%;padding:11px 14px;
  border:1px solid var(--line);border-radius:8px;
  background:rgba(255,255,255,.02);color:var(--ink);
  font-family:"JetBrains Mono",monospace;font-size:13px;transition:all .2s;}
input:focus,select:focus{outline:none;border-color:var(--o3);
  box-shadow:0 0 0 3px rgba(184,201,90,.12);background:rgba(255,255,255,.04);}
select{cursor:pointer;}
.hint{font-size:12px;color:var(--dim);margin-top:7px;font-weight:400;}
.hint b{color:var(--ink2);font-weight:500;}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:18px;}
.sep{height:1px;background:var(--line);margin:22px 0;}

/* Drop zone */
.drop{border:1.5px dashed var(--line2);border-radius:12px;padding:40px 22px;
  text-align:center;cursor:pointer;background:rgba(184,201,90,.02);transition:all .3s;}
.drop:hover,.drop.hot{border-color:var(--o3);background:rgba(184,201,90,.04);
  box-shadow:inset 0 0 40px rgba(184,201,90,.04);}
.drop strong{color:var(--ink);font-weight:500;}
.drop .h2{color:var(--dim);font-size:13px;margin-top:10px;font-weight:400;}
.drop .h2 b{color:var(--o3);font-weight:500;}
.fname{margin-top:14px;font-family:"JetBrains Mono",monospace;font-size:13px;color:var(--o4);}
input[type=file]{display:none;}

/* Buttons */
.btns{display:flex;gap:10px;margin-top:22px;}
button.run{flex:1;padding:15px;border:0;border-radius:8px;cursor:pointer;
  font-family:"Space Grotesk",sans-serif;font-weight:500;font-size:14.5px;
  transition:all .3s;letter-spacing:-.01em;position:relative;overflow:hidden;}
button.primary{background:linear-gradient(135deg,var(--o2),var(--o3),var(--o4));
  color:var(--bg);box-shadow:0 4px 20px rgba(184,201,90,.25);}
button.primary::after{content:"";position:absolute;top:0;left:-100%;width:100%;height:100%;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.2),transparent);transition:left .5s;}
button.primary:hover:not(:disabled){transform:translateY(-2px);
  box-shadow:0 8px 28px rgba(184,201,90,.4);}
button.primary:hover:not(:disabled)::after{left:100%;}
button.ghost{background:rgba(255,255,255,.02);color:var(--o3);
  border:1px solid var(--line2);backdrop-filter:blur(10px);}
button.ghost:hover:not(:disabled){background:rgba(184,201,90,.06);border-color:var(--o3);}
button.run:disabled{opacity:.3;cursor:not-allowed;}
.msg{margin-top:12px;font-size:13px;color:var(--err);min-height:1em;}

/* Stats */
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:18px;}
.stat{padding:16px;transition:all .4s cubic-bezier(.34,1.56,.64,1);}
.stat:hover{transform:translateY(-4px);border-color:rgba(184,201,90,.35);}
.stat .v{font-size:32px;font-weight:600;letter-spacing:-.025em;line-height:1;
  color:var(--ink);transition:all .3s;}
.stat .k{font-family:"JetBrains Mono",monospace;font-size:9.5px;letter-spacing:.18em;
  text-transform:uppercase;color:var(--muted);margin-top:10px;}
.stat.total .v{color:var(--o4);text-shadow:0 0 24px rgba(212,225,122,.4);}
.stat.found .v{color:var(--o5);text-shadow:0 0 24px rgba(245,249,212,.5);}
.stat.err .v{color:var(--err);}
.stat .v.bump{animation:pop .7s cubic-bezier(.34,1.56,.64,1);}

/* Progress */
.barwrap{height:2px;background:rgba(184,201,90,.08);border-radius:99px;
  overflow:hidden;margin-bottom:14px;}
.bar{height:100%;width:0;
  background:linear-gradient(90deg,var(--o1),var(--o3) 40%,var(--o5) 50%,var(--o3) 60%,var(--o1));
  background-size:200% 100%;animation:shimbar 2.5s linear infinite;
  box-shadow:0 0 16px rgba(212,225,122,.6);border-radius:99px;
  transition:width .5s cubic-bezier(.16,1,.3,1);}
.meta{display:flex;justify-content:space-between;margin:0 0 18px;
  font-family:"JetBrains Mono",monospace;font-size:12px;color:var(--dim);}
.meta .d2{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--o4);
  margin-right:8px;box-shadow:0 0 8px var(--o4);animation:pulse 1.6s ease-in-out infinite;
  vertical-align:middle;}
.meta .co{color:var(--ink);}
.meta .ct{color:var(--o4);letter-spacing:.05em;}

/* Console */
.console{background:rgba(5,7,4,.6);backdrop-filter:blur(10px);
  border:1px solid var(--line);color:#a8b090;border-radius:12px;
  padding:18px;height:320px;overflow:auto;
  font-family:"JetBrains Mono",monospace;font-size:12.5px;line-height:1.75;
  white-space:pre-wrap;word-break:break-word;}
.console::-webkit-scrollbar{width:5px;}
.console::-webkit-scrollbar-thumb{background:rgba(184,201,90,.2);border-radius:3px;}
.console .ok{color:var(--o4);}
.console .ca{color:var(--o1);}
.console .er{color:var(--err);}
.console .di{color:#5a6045;}
.console .hi{color:var(--o5);}
.console>div{animation:slide .4s cubic-bezier(.16,1,.3,1);}

/* Done */
.done{margin-top:20px;border:1px solid rgba(212,225,122,.3);border-radius:14px;
  padding:22px;display:none;align-items:center;justify-content:space-between;gap:16px;
  background:linear-gradient(145deg,rgba(212,225,122,.1),rgba(140,168,60,.04));
  box-shadow:0 0 50px rgba(212,225,122,.12);}
.done.show{display:flex;}
.done a{background:linear-gradient(135deg,var(--o3),var(--o4));color:var(--bg);
  text-decoration:none;padding:12px 22px;border-radius:8px;
  font-weight:500;font-size:14px;white-space:nowrap;
  box-shadow:0 4px 20px rgba(212,225,122,.28);transition:all .3s;}
.done a:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(212,225,122,.45);}
.hidden{display:none;}

/* Animations */
@keyframes orb1{0%,100%{transform:translate(0,0) scale(1);}50%{transform:translate(80px,60px) scale(1.15);}}
@keyframes orb2{0%,100%{transform:translate(0,0) scale(1);}50%{transform:translate(-70px,-50px) scale(1.1);}}
@keyframes float{0%,100%{transform:translateY(0);}50%{transform:translateY(-8px);}}
@keyframes glowPulse{0%,100%{filter:drop-shadow(0 0 20px rgba(184,201,90,.4));}
  50%{filter:drop-shadow(0 0 35px rgba(184,201,90,.7)) drop-shadow(0 0 60px rgba(140,168,60,.35));}}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1);}50%{opacity:.5;transform:scale(1.15);}}
@keyframes shimmer{0%,100%{background-position:0% 50%;}50%{background-position:100% 50%;}}
@keyframes shimbar{0%{background-position:-200% 0;}100%{background-position:200% 0;}}
@keyframes tick{from{transform:translateX(0);}to{transform:translateX(-50%);}}
@keyframes drift{0%,100%{transform:translate(0,0);opacity:.4;}50%{transform:translate(30px,-40px);opacity:.8;}}
@keyframes drift2{0%,100%{transform:translate(0,0);opacity:.3;}50%{transform:translate(-40px,-30px);opacity:.7;}}
@keyframes pop{0%{transform:translateY(14px) scale(.8);opacity:.4;}60%{transform:translateY(-3px) scale(1.08);}100%{transform:translateY(0) scale(1);opacity:1;}}
@keyframes slide{from{opacity:0;transform:translateX(-14px);}to{opacity:1;transform:translateX(0);}}
@keyframes ringSpin{from{transform-origin:130px 140px;transform:rotate(0deg);}to{transform-origin:130px 140px;transform:rotate(360deg);}}
@keyframes ringSpinR{from{transform-origin:130px 140px;transform:rotate(0deg);}to{transform-origin:130px 140px;transform:rotate(-360deg);}}
@keyframes pkt{0%{offset-distance:0%;opacity:0;}15%{opacity:1;}85%{opacity:1;}100%{offset-distance:100%;opacity:0;}}
@keyframes nGlow{0%,100%{opacity:.7;}50%{opacity:1;}}

@media(max-width:720px){.hero{flex-direction:column;text-align:center;}
  .stats{grid-template-columns:1fr 1fr 1fr;}.grid2{grid-template-columns:1fr;}
  h1{font-size:36px;}.hero-svg{width:200px;height:220px;}}
@media(prefers-reduced-motion:reduce){*{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important;}}
</style>
</head>
<body>
<div class="particles"><div class="pt"></div><div class="pt"></div><div class="pt"></div>
<div class="pt"></div><div class="pt"></div><div class="pt"></div></div>

<div class="wrap">
  <!-- HERO -->
  <div class="hero">
    <div class="hero-svg" aria-hidden="true">
      <svg width="260" height="280" viewBox="0 0 260 280" style="overflow:visible">
        <defs>
          <radialGradient id="cg" cx="30%" cy="30%">
            <stop offset="0%" stop-color="#f5f9d4"/><stop offset="35%" stop-color="#d4e17a"/>
            <stop offset="70%" stop-color="#b8c95a"/><stop offset="100%" stop-color="#6b8020"/>
          </radialGradient>
          <radialGradient id="ng" cx="30%" cy="30%">
            <stop offset="0%" stop-color="#e8f0a8"/><stop offset="60%" stop-color="#b8c95a"/>
            <stop offset="100%" stop-color="#6b8020"/>
          </radialGradient>
          <radialGradient id="nd" cx="30%" cy="30%">
            <stop offset="0%" stop-color="#a8c04a"/><stop offset="100%" stop-color="#3d4a12"/>
          </radialGradient>
        </defs>
        <!-- Rotating rings -->
        <circle cx="130" cy="140" r="115" fill="none" stroke="rgba(184,201,90,.15)" stroke-width=".5" stroke-dasharray="1,3" style="transform-origin:130px 140px;animation:ringSpin 40s linear infinite;"/>
        <circle cx="130" cy="140" r="85" fill="none" stroke="rgba(212,225,122,.2)" stroke-width=".5" stroke-dasharray="2,4" style="transform-origin:130px 140px;animation:ringSpinR 28s linear infinite;"/>
        <!-- Lines -->
        <g stroke="rgba(184,201,90,.4)" stroke-width=".75" fill="none">
          <line x1="130" y1="140" x2="70" y2="55" stroke-dasharray="2,3"/>
          <line x1="130" y1="140" x2="190" y2="55" stroke-dasharray="2,3"/>
          <line x1="130" y1="140" x2="35" y2="140" stroke-dasharray="2,3"/>
          <line x1="130" y1="140" x2="225" y2="140" stroke-dasharray="2,3"/>
          <line x1="130" y1="140" x2="80" y2="235" stroke-dasharray="2,3"/>
          <line x1="130" y1="140" x2="180" y2="235" stroke-dasharray="2,3"/>
        </g>
        <!-- Data packets -->
        <circle r="2.5" fill="#f5f9d4" style="offset-path:path('M 130 140 L 70 55');animation:pkt 3s ease-in-out infinite;filter:drop-shadow(0 0 8px #d4e17a);"/>
        <circle r="2.5" fill="#f5f9d4" style="offset-path:path('M 130 140 L 190 55');animation:pkt 3s ease-in-out infinite .5s;filter:drop-shadow(0 0 8px #d4e17a);"/>
        <circle r="2" fill="#d4e17a" style="offset-path:path('M 130 140 L 35 140');animation:pkt 3.5s ease-in-out infinite 1s;filter:drop-shadow(0 0 6px #b8c95a);"/>
        <circle r="2" fill="#d4e17a" style="offset-path:path('M 130 140 L 225 140');animation:pkt 3.5s ease-in-out infinite 1.5s;filter:drop-shadow(0 0 6px #b8c95a);"/>
        <circle r="1.5" fill="#b8c95a" style="offset-path:path('M 130 140 L 80 235');animation:pkt 4s ease-in-out infinite 2s;filter:drop-shadow(0 0 5px #8ca83c);"/>
        <circle r="1.5" fill="#b8c95a" style="offset-path:path('M 130 140 L 180 235');animation:pkt 4s ease-in-out infinite 2.5s;filter:drop-shadow(0 0 5px #8ca83c);"/>
        <!-- Tier 1 nodes -->
        <g style="animation:nGlow 3s ease-in-out infinite;"><circle cx="70" cy="55" r="16" fill="url(#ng)" stroke="#f5f9d4" stroke-width=".75"/><text x="70" y="33" text-anchor="middle" fill="#d4e17a" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">CEO/VP</text></g>
        <g style="animation:nGlow 3s ease-in-out infinite .3s;"><circle cx="190" cy="55" r="16" fill="url(#ng)" stroke="#f5f9d4" stroke-width=".75"/><text x="190" y="33" text-anchor="middle" fill="#d4e17a" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">President</text></g>
        <!-- Tier 2 nodes -->
        <g style="animation:nGlow 3s ease-in-out infinite .6s;"><circle cx="35" cy="140" r="15" fill="url(#ng)" stroke="#b8c95a" stroke-width=".75"/><text x="35" y="172" text-anchor="middle" fill="#b8c95a" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">Estimator</text></g>
        <g style="animation:nGlow 3s ease-in-out infinite .9s;"><circle cx="225" cy="140" r="15" fill="url(#ng)" stroke="#b8c95a" stroke-width=".75"/><text x="225" y="172" text-anchor="middle" fill="#b8c95a" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">PM</text></g>
        <!-- Tier 3 nodes -->
        <g style="animation:nGlow 3s ease-in-out infinite 1.2s;"><circle cx="80" cy="235" r="13" fill="url(#nd)" stroke="#8ca83c" stroke-width=".75"/><text x="80" y="262" text-anchor="middle" fill="#8ca83c" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">BIM Lead</text></g>
        <g style="animation:nGlow 3s ease-in-out infinite 1.5s;"><circle cx="180" cy="235" r="13" fill="url(#nd)" stroke="#8ca83c" stroke-width=".75"/><text x="180" y="262" text-anchor="middle" fill="#8ca83c" font-family="JetBrains Mono" font-size="8.5" letter-spacing=".5">VDC/CAD</text></g>
        <!-- Core -->
        <circle cx="130" cy="140" r="34" fill="url(#cg)" stroke="#f5f9d4" stroke-width="1"/>
        <circle cx="130" cy="140" r="28" fill="none" stroke="rgba(255,255,255,.18)" stroke-width=".5"/>
        <text x="130" y="137" text-anchor="middle" fill="#050704" font-family="JetBrains Mono" font-size="7.5" font-weight="600" letter-spacing="1.5">COMPANY</text>
        <text x="130" y="150" text-anchor="middle" fill="#050704" font-family="JetBrains Mono" font-size="6.5" opacity=".7">domain.com</text>
      </svg>
    </div>
    <div>
      <div class="eyebrow"><span class="dot"></span>BMSI &middot; Lead Generation Engine</div>
      <h1>BIM Data<br>Execution.</h1>
      <p class="sub">Upload a spreadsheet. We find <b>every contact in the BMSI hierarchy</b> for each company — name, title, email, phone, LinkedIn.</p>
      <div class="tiers">
        <span class="tier t1"><span class="b"></span>Executive</span>
        <span class="tier t2"><span class="b"></span>Estimator &middot; PM</span>
        <span class="tier t3"><span class="b"></span>BIM &middot; VDC</span>
      </div>
    </div>
  </div>

  <!-- Ticker -->
  <div class="ticker-wrap">
    <div class="ticker" id="ticker">
      <span>Loading&hellip;</span>
    </div>
  </div>

  <div id="masterbar">
    <span class="d"></span>
    <span id="masterinfo">Master store: loading&hellip;</span>
    <a href="/export-master">Export master &rarr;</a>
  </div>

  <!-- Config -->
  <div class="glass card collapsible" id="cfg">
    <div class="card-h" id="cfgH">
      <span class="num">01</span><h2>Configuration</h2>
      {% if api_key_set %}<span class="set">Server key active</span>{% endif %}
      <span class="chev" id="cfgC">Hide &darr;</span>
    </div>
    <div class="card-b">
      <div class="grid2">
        <div class="fld">
          <label for="limit">Candidates to rank</label>
          <input type="number" id="limit" min="5" max="25" value="{{ default_limit }}">
          <div class="hint">Free search per company. Keep at 25 for best results.</div>
        </div>
        <div class="fld">
          <label for="maxc">Max contacts per company</label>
          <input type="number" id="maxc" min="1" max="10" value="{{ default_maxc }}">
          <div class="hint">Researches the <b>top N by hierarchy</b> (President &rarr; Estimator &rarr; BIM). <b>Each = ~1 credit.</b> Lower = cheaper. Default 3.</div>
        </div>
      </div>
      <div class="sep"></div>
      <div class="fld">
        <label for="minrank">Credit saver &middot; minimum match quality</label>
        <select id="minrank">
          <option value="999">Any match (research everyone found)</option>
          <option value="45">Skip pure executives (no CEO/VP/President)</option>
          <option value="37">Estimators, PMs, and Project roles only</option>
          <option value="27">Estimators and above only</option>
          <option value="8">BIM / VDC / CAD roles only</option>
        </select>
        <div class="hint">Weaker matches skipped at <b>zero cost</b>.</div>
      </div>
      <div class="grid2">
        <div class="fld">
          <label for="startrow">Start at company #</label>
          <input type="number" id="startrow" min="1" value="1">
          <div class="hint">For batching — e.g. start at 51 for second batch.</div>
        </div>
        <div class="fld">
          <label for="maxco">Max companies this run</label>
          <input type="number" id="maxco" min="0" max="200" value="0">
          <div class="hint">0 = all (up to 200 total per file).</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Upload -->
  <div class="glass card" id="uploadCard">
    <div class="card-h"><span class="num">02</span><h2>Upload &amp; Run</h2></div>
    <div class="card-b">
      <div class="drop" id="drop">
        <strong>Choose a file</strong> or drag it here
        <div class="h2">Excel (.xlsx) or CSV &middot; 1&ndash;200 companies<br>
          Columns: <b>Company</b> (required) + <b>Website</b> or <b>Email</b> (for exact matching)</div>
        <div class="fname" id="fname"></div>
      </div>
      <input type="file" id="file" accept=".xlsx,.xlsm,.csv,.tsv">
      <div class="btns">
        <button class="run ghost" id="prev" disabled>Preview &middot; free</button>
        <button class="run primary" id="go" disabled>Run &middot; uses credits</button>
      </div>
      <div class="msg" id="msg"></div>
    </div>
  </div>

  <!-- Results -->
  <div class="hidden" id="run">
    <div class="stats">
      <div class="glass stat total"><div class="v" id="s0">0</div><div class="k">Companies</div></div>
      <div class="glass stat found"><div class="v" id="s1">0</div><div class="k" id="k1">New contacts</div></div>
      <div class="glass stat"><div class="v" id="s2">0</div><div class="k">Reused free</div></div>
      <div class="glass stat"><div class="v" id="s3">0</div><div class="k">No match</div></div>
      <div class="glass stat err"><div class="v" id="s4">0</div><div class="k">Errors</div></div>
    </div>
    <div class="barwrap"><div class="bar" id="bar"></div></div>
    <div class="meta">
      <span><span class="d2"></span><span id="stage">Starting&hellip;</span></span>
      <span class="ct" id="count"></span>
    </div>
    <div class="console" id="con"></div>
    <div class="done" id="done">
      <span id="doneMsg">Your contacts file is ready.</span>
      <a id="dl" href="#">Download Excel</a>
    </div>
  </div>
</div>

<script>
// Config toggle
var cfg=document.getElementById('cfg'),cfgC=document.getElementById('cfgC');
document.getElementById('cfgH').addEventListener('click',function(){
  cfg.classList.toggle('collapsed');
  cfgC.innerHTML=cfg.classList.contains('collapsed')?'Edit &rarr;':'Hide &darr;';
});

// File drop
var fi=document.getElementById('file'),drop=document.getElementById('drop'),
    fname=document.getElementById('fname'),go=document.getElementById('go'),
    prev=document.getElementById('prev'),msg=document.getElementById('msg');
drop.addEventListener('click',function(){fi.click();});
drop.addEventListener('dragover',function(e){e.preventDefault();drop.classList.add('hot');});
drop.addEventListener('dragleave',function(){drop.classList.remove('hot');});
drop.addEventListener('drop',function(e){e.preventDefault();drop.classList.remove('hot');
  if(e.dataTransfer.files.length){fi.files=e.dataTransfer.files;onFile();}});
fi.addEventListener('change',onFile);
function onFile(){if(fi.files.length){fname.textContent=fi.files[0].name;
  go.disabled=false;prev.disabled=false;}}

function collect(isPreview){
  var fd=new FormData();
  fd.append('file',fi.files[0]);
  fd.append('search_limit',document.getElementById('limit').value);
  fd.append('max_contacts',document.getElementById('maxc').value);
  fd.append('min_rank',document.getElementById('minrank').value);
  fd.append('start',document.getElementById('startrow').value||'1');
  fd.append('limit_companies',document.getElementById('maxco').value||'0');
  fd.append('preview',isPreview?'1':'0');
  return fd;
}
function submit(isPreview){
  msg.textContent='';
  if(!fi.files.length){msg.textContent='Choose a file first.';return;}
  go.disabled=true;prev.disabled=true;
  (isPreview?prev:go).textContent='Uploading...';
  fetch('/upload',{method:'POST',body:collect(isPreview)})
    .then(function(r){return r.json().then(function(d){return{ok:r.ok,d:d};});})
    .then(function(res){
      if(!res.ok)throw new Error(res.d.error||'Upload failed.');
      document.getElementById('uploadCard').style.display='none';
      cfg.classList.add('collapsed');cfgC.innerHTML='Edit &rarr;';
      document.getElementById('run').classList.remove('hidden');
      poll(res.d.job_id);
    })
    .catch(function(e){
      msg.textContent=e.message;
      go.disabled=false;prev.disabled=false;
      go.textContent='Run \u00b7 uses credits';prev.textContent='Preview \u00b7 free';
    });
}
go.addEventListener('click',function(){submit(false);});
prev.addEventListener('click',function(){submit(true);});

// Master stats + ticker
fetch('/master-stats').then(function(r){return r.json();}).then(function(d){
  if(d&&d.contacts!=null){
    document.getElementById('masterinfo').innerHTML=
      'Master store &middot; <span style="color:var(--o4)">'+(d.contacts||0).toLocaleString()+
      '</span> contacts across <span style="color:var(--o4)">'+(d.companies||0).toLocaleString()+
      '</span> companies (reused free)';
    var items=[
      '<span><span class="n">'+(d.contacts||0).toLocaleString()+'</span> contacts cached</span>',
      '<span class="sep">&#9670;</span>',
      '<span><span class="n">'+(d.companies||0).toLocaleString()+'</span> companies mapped</span>',
      '<span class="sep">&#9670;</span>',
      '<span>Domain-locked matching</span>',
      '<span class="sep">&#9670;</span>',
      '<span>Executive &middot; Estimator &middot; PM &middot; BIM tiers</span>',
      '<span class="sep">&#9670;</span>',
      '<span>Cached contacts reused free</span>',
      '<span class="sep">&#9670;</span>'
    ];
    document.getElementById('ticker').innerHTML=items.join('')+items.join('');
  }
}).catch(function(){
  document.getElementById('masterinfo').textContent='Master store: empty (builds as you run).';
});

// Polling
var bar=document.getElementById('bar'),stage=document.getElementById('stage'),
    count=document.getElementById('count'),con=document.getElementById('con'),
    doneBox=document.getElementById('done'),dl=document.getElementById('dl'),
    doneMsg=document.getElementById('doneMsg');
var Sv={s0:0,s1:0,s2:0,s3:0,s4:0};
function bumpStat(id,val){
  var el=document.getElementById(id);
  if(Sv[id]!==val){Sv[id]=val;el.textContent=val;
    el.classList.remove('bump');void el.offsetWidth;el.classList.add('bump');}
}
function renderLog(log){
  con.innerHTML=log.map(function(l){
    var c='';
    if(/ERROR|FATAL|error/.test(l))c='er';
    else if(/from cache|cached/.test(l))c='ca';
    else if(/Done\.|contact\(s\) researched/.test(l))c='ok';
    else if(/\[http\]|\[poll\]|\[search\]|\[research\]/.test(l))c='hi';
    else if(/API key|Loading|Drive|Starting|Processing/.test(l))c='di';
    return '<div class="'+c+'">'+l.replace(/</g,'&lt;')+'</div>';
  }).join('');
  con.scrollTop=con.scrollHeight;
}
function poll(id){
  fetch('/status/'+id).then(function(r){
    if(!r.ok){stage.textContent='Job not found. Please re-upload.';return null;}
    return r.json();
  }).then(function(s){
    if(!s)return;
    renderLog(s.log||[]);
    bumpStat('s0',s.current||0);bumpStat('s1',s.contacts||0);
    bumpStat('s2',s.cached||0);bumpStat('s3',s.nomatch||0);bumpStat('s4',s.errors||0);
    if(s.total){bar.style.width=Math.round((s.current/s.total)*100)+'%';
      count.textContent=s.current+' \u00b7 '+s.total;}
    if(s.status==='running'){
      stage.innerHTML='Processing \u00b7 <span class="co">'+(s.company||'\u2026')+'</span>';
    }else if(s.status==='queued'){stage.textContent='Queued\u2026';
    }else if(s.status==='error'){stage.textContent='Stopped: '+(s.error||'error');return;
    }else if(s.status==='done'){
      stage.textContent='Complete';bar.style.width='100%';
      dl.href=s.download;
      var m='Your contacts file is ready.';
      if(s.drive_link)m+=' <a href="'+s.drive_link+'" target="_blank" style="color:var(--o4)">Open in Drive</a>';
      doneMsg.innerHTML=m;
      doneBox.classList.add('show');return;
    }
    setTimeout(function(){poll(id);},2000);
  }).catch(function(){
    stage.textContent='Lost connection. Retrying...';
    setTimeout(function(){poll(id);},4000);
  });
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT","5000"))
    try:
        from waitress import serve
        print(f"BIM Data Execution on http://0.0.0.0:{port}")
        serve(app, host="0.0.0.0", port=port, threads=8)
    except ImportError:
        app.run(host="0.0.0.0", port=port, threaded=True)