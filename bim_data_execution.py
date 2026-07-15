#!/usr/bin/env python3
"""
BIM Data Execution - Bulk Company Lead Generation with Hierarchy (v4)

For each company:
  1. SEARCH wide (free) -> candidates with titles.
  2. RANK every candidate into hierarchy tiers.
  3. RESEARCH the top N distinct people (paid) -> emails + phones.
Output: ONE ROW PER CONTACT, grouped by company, ordered by tier, with Tier,
Hierarchy Role, name, title, email, phone, LinkedIn.

Speed: companies are processed in parallel by the web app (app.py).

REQUIREMENTS:  pip install requests openpyxl
"""

import argparse, csv, os, re, sys, time, json, sqlite3, threading, requests
from datetime import datetime, timezone
from openpyxl import load_workbook, Workbook

# ============================================================================
# MASTER STORE (SQLite cache)  - avoids paying to re-research known contacts.
# Key = company name + person name + hierarchy role (all lowercased).
# ============================================================================
MASTER_DB = os.environ.get("MASTER_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "bim_master.db")
_DB_LOCK = threading.Lock()

def _norm(s):
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _cache_key(company, first, last, role):
    return f"{_norm(company)}|{_norm(first)} {_norm(last)}|{_norm(role)}"

def db_connect():
    conn = sqlite3.connect(MASTER_DB, timeout=30, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS contacts (
        cache_key TEXT PRIMARY KEY,
        company TEXT, tier TEXT, role TEXT,
        first_name TEXT, last_name TEXT, job_title TEXT,
        email TEXT, phone TEXT, linkedin TEXT, domain TEXT,
        saved_at TEXT )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_company ON contacts(company)")
    return conn

def cache_lookup_company(company):
    """Return list of saved contact rows for a company (empty if none)."""
    with _DB_LOCK:
        conn = db_connect()
        try:
            cur = conn.execute("SELECT company,tier,role,first_name,last_name,job_title,"
                               "email,phone,linkedin,domain,saved_at FROM contacts "
                               "WHERE company=?", (_norm(company),))
            out = []
            for r in cur.fetchall():
                out.append({"Company Name": company, "Tier": r[1], "Hierarchy Role": r[2],
                            "First Name": r[3], "Last Name": r[4], "Job Title": r[5],
                            "Email": r[6], "Phone Number": r[7], "LinkedIn Profile": r[8],
                            "Company Domain": r[9],
                            "Source": f"From cache (saved {(r[10] or '')[:10]})", "Note": ""})
            return out
        finally:
            conn.close()

def cache_save(rows):
    """Upsert freshly-researched contact rows into the master store."""
    now = datetime.now(timezone.utc).isoformat()
    with _DB_LOCK:
        conn = db_connect()
        try:
            for r in rows:
                if not (r.get("First Name") or r.get("Last Name")):
                    continue  # skip note-only / no-match rows
                key = _cache_key(r["Company Name"], r.get("First Name"),
                                 r.get("Last Name"), r.get("Hierarchy Role"))
                conn.execute("""INSERT INTO contacts
                    (cache_key,company,tier,role,first_name,last_name,job_title,
                     email,phone,linkedin,domain,saved_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                     email=excluded.email, phone=excluded.phone, linkedin=excluded.linkedin,
                     domain=excluded.domain, job_title=excluded.job_title,
                     tier=excluded.tier, saved_at=excluded.saved_at""",
                    (key, _norm(r["Company Name"]), str(r.get("Tier","")), r.get("Hierarchy Role",""),
                     r.get("First Name",""), r.get("Last Name",""), r.get("Job Title",""),
                     r.get("Email",""), r.get("Phone Number",""), r.get("LinkedIn Profile",""),
                     r.get("Company Domain",""), now))
            conn.commit()
        finally:
            conn.close()

def cache_stats():
    with _DB_LOCK:
        conn = db_connect()
        try:
            n = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
            c = conn.execute("SELECT COUNT(DISTINCT company) FROM contacts").fetchone()[0]
            return {"contacts": n, "companies": c}
        finally:
            conn.close()

def export_master_xlsx(path):
    with _DB_LOCK:
        conn = db_connect()
        try:
            cur = conn.execute("SELECT company,tier,role,first_name,last_name,job_title,"
                               "email,phone,linkedin,domain,saved_at FROM contacts "
                               "ORDER BY company,tier")
            wb = Workbook(); ws = wb.active; ws.title = "Master"
            ws.append(["Company","Tier","Hierarchy Role","First Name","Last Name","Job Title",
                       "Email","Phone Number","LinkedIn Profile","Company Domain","Saved At"])
            for r in cur.fetchall():
                ws.append(list(r))
            wb.save(path)
        finally:
            conn.close()

# ============================================================================
# CONFIG  (Seamless auth: header named "token", raw key)
# ============================================================================
SEAMLESS_BASE = "https://api.seamless.ai/api/client/v1"
EP_SEARCH   = f"{SEAMLESS_BASE}/search/contacts"
EP_RESEARCH = f"{SEAMLESS_BASE}/contacts/research"
EP_POLL     = f"{SEAMLESS_BASE}/contacts/research/poll"

AUTH_HEADER_NAME  = "token"
AUTH_HEADER_VALUE = "{key}"

SEARCH_LIMIT              = 25        # candidates to pull & rank per company
MAX_CONTACTS_PER_COMPANY  = 5         # how many people to RESEARCH per company (credit driver)
SKIP_DEDUP                = False
HTTP_TIMEOUT              = 45
HTTP_RETRIES              = 2         # retry on timeout / transient error
POLL_INTERVAL_SECONDS     = 8
POLL_MAX_ATTEMPTS         = 20
DELAY_BETWEEN_COMPANIES   = 0
DONE_STATUSES             = {"done", "error", "missing", "duplicate"}
USABLE_STATUSES           = {"done", "duplicate"}

COMPANY_COLUMN_CANDIDATES = ["company", "company name", "companyname", "firm", "firm name"]

# ============================================================================
# HIERARCHY  (user-defined, priority order: index 1 = highest priority)
#   ONLY titles in this list match. Anything not listed is not selected.
#   Ordered patterns; the FIRST pattern a title matches wins its tier.
#   More specific titles are listed before broader ones so they win correctly.
# ============================================================================
HIERARCHY_TITLES = [
    # --- BIM / VDC / CAD ---
    ("BIM Director",                    r"bim director|director of bim"),
    ("VDC Director",                    r"vdc director|director of vdc"),
    ("Virtual Design Manager",          r"virtual design (and construction )?manager"),
    ("VDC Manager",                     r"vdc manager"),
    ("BIM Manager",                     r"bim manager"),
    ("CAD Manager",                     r"cad manager"),
    ("BIM Coordinator",                 r"bim coordinator"),
    ("CAD Coordinator",                 r"cad coordinator"),
    # --- Pre-Construction ---
    ("VP of Pre-Construction",          r"(vice president|vp).*(pre-?con(struction)?)|(pre-?con(struction)?).*(vice president|vp)"),
    ("Director of Pre-Construction",    r"(director of pre-?con(struction)?)|(pre-?con(struction)?\s*director)"),
    ("Pre-Construction Executive",      r"pre-?con(struction)?\s*executive"),
    ("Pre-Construction Lead",           r"pre-?con(struction)?\s*lead"),
    ("Pre-Construction Operations Mgr", r"pre-?con(struction)?\s*operations\s*manager"),
    ("Pre-Construction Manager",        r"pre-?con(struction)?\s*manager"),
    ("Pre-Construction Coordinator",    r"pre-?con(struction)?\s*coordinator"),
    # --- Division / General Management ---
    ("VP - Division",                   r"(vice president|vp)\s*[-\u2013]?\s*division"),
    ("VP - Region",                     r"(vice president|vp)\s*[-\u2013]?\s*region"),
    ("Regional Division Manager",       r"regional division manager"),
    ("Mechanical Division Manager",     r"mechanical division manager"),
    ("Electrical Division Manager",     r"electrical division manager"),
    ("Plumbing Division Manager",       r"plumbing division manager"),
    ("Division Director",               r"division director"),
    ("Division Manager",                r"division manager"),
    ("Vertical Manager",                r"vertical manager"),
    ("General Manager",                 r"general manager"),
    # --- Estimating ---
    ("Chief Estimating Officer",        r"chief estimating officer"),
    ("Chief Estimator",                 r"chief estimator"),
    ("Director of Estimating",          r"(director of estimating)|(estimating director)"),
    ("Estimating Manager",              r"estimating manager"),
    ("Senior Estimator",               r"(senior|sr\.?) estimator"),
    ("Lead Estimator",                  r"lead estimator"),
    ("Mechanical Estimator",            r"mechanical estimator"),
    ("HVAC Estimator",                  r"hvac estimator"),
    ("Electrical Estimator",            r"electrical estimator"),
    ("Plumbing Estimator",              r"plumbing estimator"),
    ("Estimator",                       r"\bestimator\b"),
    # --- Project Management ---
    ("Director of Project Management",  r"director of project management"),
    ("Project Executive",               r"project executive"),
    ("Senior Project Manager",         r"(senior|sr\.?) project manager"),
    ("Project Manager",                 r"\bproject manager\b"),
    ("Assistant Project Manager",       r"assistant project manager"),
    ("Senior Construction Manager",     r"(senior|sr\.?) construction manager"),
    ("Construction Manager",            r"construction manager"),
    ("Senior Project Engineer",         r"(senior|sr\.?) project engineer"),
    ("Project Engineer",                r"project engineer"),
    ("Project Coordinator",             r"project coordinator"),
    # --- Engineering ---
    ("Director of Engineering",         r"director of engineering"),
    ("Chief Engineer",                  r"chief engineer"),
    ("Engineering Manager",             r"engineering manager"),
    ("Senior Engineer",                 r"(senior|sr\.?) engineer"),
    ("Mechanical Engineer",             r"mechanical engineer"),
    ("Electrical Engineer",             r"electrical engineer"),
    ("Plumbing Engineer",               r"plumbing engineer"),
    ("Design Engineer",                 r"design engineer"),
    # --- Operations ---
    ("VP of Operations",                r"(vice president|vp).*operations|operations.*(vice president|vp)"),
    ("Chief Operating Officer",         r"chief operating officer|\bcoo\b"),
    ("Director of Operations",          r"(director of operations)|(operations director)"),
    ("Senior Operations Manager",       r"(senior|sr\.?) operations manager"),
    ("Operations Manager",              r"operations manager"),
    # --- Executive / President ---
    ("Executive Vice President",        r"executive vice president|\bevp\b"),
    ("Senior Vice President",           r"senior vice president|\bsvp\b"),
    ("Regional President",              r"regional president"),
    ("Company President",               r"company president"),
    ("President",                       r"\bpresident\b"),
    ("Vice President",                  r"vice president|\bvp\b"),
    # --- Owner / Founder / CEO (+ assistants to them, listed before CEO/Owner so they win) ---
    ("Executive Assistant to CEO",      r"executive assistant to (the )?ceo"),
    ("Assistant to CEO",                r"assistant to (the )?ceo"),
    ("Assistant to Owner",              r"assistant to (the )?owner"),
    ("Chief Executive Officer",         r"chief executive officer|\bceo\b"),
    ("Owner",                           r"\bowner\b"),
    ("Founder",                         r"\bfounder\b"),
]
# Compile into tiered rules (tier = position in the list, 1-based)
HIERARCHY = [(i + 1, label, re.compile(pat, re.I))
             for i, (label, pat) in enumerate(HIERARCHY_TITLES)]
TIER_LABEL = {i + 1: label for i, (label, _) in enumerate(HIERARCHY_TITLES)}
LAST_RESORT_TIER = 10_000   # unused now (kept so other code references don't break)
BLACKLIST = []              # no exclusions - only listed titles match
LAST_RESORT = []

OUTPUT_COLUMNS = ["Company Name", "Tier", "Hierarchy Role", "First Name", "Last Name",
                  "Job Title", "Email", "Phone Number", "LinkedIn Profile",
                  "Company Domain", "Source", "Note"]


def log(msg): print(msg, flush=True)

def auth_headers(api_key):
    return {AUTH_HEADER_NAME: AUTH_HEADER_VALUE.format(key=api_key), "Content-Type": "application/json"}

def load_companies(path):
    ext = os.path.splitext(path)[1].lower()
    rows = []
    if ext in (".xlsx", ".xlsm"):
        wb = load_workbook(path, read_only=True, data_only=True); ws = wb.active
        rows = [list(r) for r in ws.iter_rows(values_only=True)]
    elif ext in (".csv", ".tsv"):
        delim = "\t" if ext == ".tsv" else ","
        with open(path, newline="", encoding="utf-8-sig") as f:
            rows = [r for r in csv.reader(f, delimiter=delim)]
    else:
        raise ValueError(f"Unsupported file type: {ext} (use .xlsx or .csv)")
    if not rows: raise ValueError("File is empty.")
    header = [("" if c is None else str(c)).strip() for c in rows[0]]
    col_idx = None
    for i, h in enumerate(header):
        if h.strip().lower() in COMPANY_COLUMN_CANDIDATES: col_idx = i; break
    if col_idx is None:
        raise ValueError(f'No company column found. Headers: {header}. Expected one of: {COMPANY_COLUMN_CANDIDATES}')
    companies = []
    seen = set()
    for r in rows[1:]:
        if col_idx < len(r) and r[col_idx] is not None:
            name = str(r[col_idx]).strip()
            if name and name.lower() not in seen:
                seen.add(name.lower()); companies.append(name)
    if not companies: raise ValueError("Company column found, but no non-empty company names.")
    return companies


# ---------------------------------------------------------------------------
# Ranking (free, on search titles)
# ---------------------------------------------------------------------------
def _match_text(contact):
    return f"{contact.get('title','') or ''} {contact.get('department','') or ''}".strip()

def tier_of(contact):
    """Return the hierarchy tier for a contact's title, or None if the title isn't
    in the defined hierarchy. First matching pattern (highest priority) wins."""
    title = (contact.get("title") or "").strip()
    if not title:
        return None
    for tier, label, pat in HIERARCHY:
        if pat.search(title):
            return tier
    return None

def rank_candidates(contacts):
    """Return list of (tier, contact) for all contacts whose title is in the hierarchy,
    best tier first, de-duplicated by person."""
    ranked = []
    seen = set()
    for c in contacts:
        t = tier_of(c)
        if t is None:
            continue
        key = (c.get("name") or "").strip().lower() or (c.get("liUrl") or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        ranked.append((t, c))
    ranked.sort(key=lambda x: x[0])
    return ranked


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
def _split_name(full):
    parts = (full or "").split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], parts[-1]

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"(?:\+?\d[\d\-\.\s\(\)]{6,}\d)")

def _flat_str(v):
    """Pull a usable string out of a value that may be a str, list, or dict."""
    if v is None: return ""
    if isinstance(v, str): return v.strip()
    if isinstance(v, (int, float)): return str(v)
    if isinstance(v, list):
        for item in v:
            s = _flat_str(item)
            if s: return s
        return ""
    if isinstance(v, dict):
        for item in v.values():
            s = _flat_str(item)
            if s: return s
        return ""
    return str(v).strip()

def _find_email(*objs):
    for o in objs:
        if not isinstance(o, dict): continue
        for k in ("email", "email1", "emailAddress", "workEmail", "primaryEmail", "email2", "emails"):
            s = _flat_str(o.get(k))
            if s and _EMAIL_RE.search(s): return _EMAIL_RE.search(s).group(0)
    for o in objs:
        if not isinstance(o, dict): continue
        for k, v in o.items():
            if "email" in k.lower():
                s = _flat_str(v)
                if s and _EMAIL_RE.search(s): return _EMAIL_RE.search(s).group(0)
    for o in objs:
        if not isinstance(o, dict): continue
        for v in o.values():
            s = _flat_str(v)
            if s and _EMAIL_RE.search(s): return _EMAIL_RE.search(s).group(0)
    return ""

def _find_phone(*objs):
    for o in objs:
        if not isinstance(o, dict): continue
        for k in ("contactPhone1", "contactPhone2", "contactPhone3", "phone", "phoneNumber",
                  "mobilePhone", "directPhone", "workPhone", "companyPhone", "phones"):
            s = _flat_str(o.get(k))
            if s: return s
    for o in objs:
        if not isinstance(o, dict): continue
        for k, v in o.items():
            if "phone" in k.lower():
                s = _flat_str(v)
                if s: return s
    return ""

def _find_linkedin(*objs):
    for o in objs:
        if not isinstance(o, dict): continue
        for k in ("lIProfileUrl", "liUrl", "linkedin", "linkedInUrl", "linkedinUrl"):
            s = _flat_str(o.get(k))
            if s and "linkedin" in s.lower(): return s
    for o in objs:
        if not isinstance(o, dict): continue
        for v in o.values():
            s = _flat_str(v)
            if s and "linkedin.com/in/" in s.lower(): return s
    return ""

def contact_row(company, tier, search_c, enriched, note=""):
    enriched = enriched or {}
    fn = enriched.get("firstName") or search_c.get("firstName")
    ln = enriched.get("lastName") or search_c.get("lastName")
    if not fn and not ln:
        fn, ln = _split_name(search_c.get("name") or enriched.get("name"))
    # scan BOTH the enriched (research) and search objects, any field name
    email = _find_email(enriched, search_c)
    phone = _find_phone(enriched, search_c)
    linkedin = _find_linkedin(enriched, search_c)
    return {
        "Company Name": company,
        "Tier": tier,
        "Hierarchy Role": TIER_LABEL.get(tier, ""),
        "First Name": fn or "", "Last Name": ln or "",
        "Job Title": enriched.get("title") or search_c.get("title") or "",
        "Email": email, "Phone Number": phone,
        "LinkedIn Profile": linkedin,
        "Company Domain": enriched.get("companyDomain") or search_c.get("domain") or "",
        "Source": "Researched now",
        "Note": note or ("" if email else "No email returned by research."),
    }

def note_row(company, note):
    row = {k: "" for k in OUTPUT_COLUMNS}
    row["Company Name"] = company; row["Note"] = note
    return row


# ---------------------------------------------------------------------------
# Seamless API (with retry on timeout)
# ---------------------------------------------------------------------------
def _request(method, url, headers, **kw):
    last = None
    for attempt in range(1, HTTP_RETRIES + 2):
        try:
            r = requests.request(method, url, headers=headers, timeout=HTTP_TIMEOUT, **kw)
            if r.status_code >= 500:
                last = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(2 * attempt); continue
            return r
        except (requests.Timeout, requests.ConnectionError) as e:
            last = e; time.sleep(2 * attempt)
    raise last

def safe_json(resp, label):
    if resp.status_code >= 400:
        raise RuntimeError(f"{label} HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"{label}: non-JSON response: {resp.text[:300]}")

def search_candidates(company, headers, limit=None):
    limit = limit or SEARCH_LIMIT
    body = {"companyName": company, "companyDomain": "", "limit": limit}
    data = safe_json(_request("POST", EP_SEARCH, headers, json=body), "search")
    return data.get("data") or data.get("contacts") or data.get("results") or []

def research_ids(search_result_ids, headers):
    body = {"searchResultIds": list(search_result_ids), "skipDeduplicationCheck": SKIP_DEDUP}
    data = safe_json(_request("POST", EP_RESEARCH, headers, json=body), "research-submit")
    request_ids = data.get("requestIds") or (data.get("data") or {}).get("requestIds") or []
    if not request_ids:
        err = data.get("message") or data.get("error") or json.dumps(data)[:200]
        raise RuntimeError(f"research submit returned no requestIds. Raw: {err}")
    return request_ids

def poll_research(request_ids, headers, interval=None, attempts=None):
    interval = interval or POLL_INTERVAL_SECONDS
    attempts = attempts or POLL_MAX_ATTEMPTS
    params = {"requestIds": ",".join(str(x) for x in request_ids)}
    results = []
    for _ in range(attempts):
        data = safe_json(_request("GET", EP_POLL, headers, params=params), "poll")
        results = data.get("data") or []
        if isinstance(results, list) and results and all(r.get("status") in DONE_STATUSES for r in results):
            return results
        time.sleep(interval)
    return results if isinstance(results, list) else []


def _id_of(c):
    return (c.get("searchResultId") or c.get("searchResultID")
            or c.get("id") or c.get("contactId"))

def process_company(company, headers, search_limit=None, poll_interval=None,
                    poll_attempts=None, min_rank=None, max_contacts=None,
                    preview=False, use_cache=True):
    """Return (rows, kind). rows is a list (one per contact) or a single note row.
    kind: nocontacts | nomatch | would | found | cached | error(handled by caller)"""
    max_contacts = max_contacts or MAX_CONTACTS_PER_COMPANY

    # 0) MASTER STORE: if we already researched this company, reuse it for free.
    if use_cache and not preview:
        cached = cache_lookup_company(company)
        if cached:
            return cached, "cached"

    candidates = search_candidates(company, headers, limit=search_limit)
    if not candidates:
        return [note_row(company, "No contacts found for this company in Seamless search.")], "nocontacts"

    ranked = rank_candidates(candidates)
    if min_rank is not None:
        ranked = [(t, c) for t, c in ranked if t <= min_rank]
    if not ranked:
        return [note_row(company, "No contact matched the hierarchy (no one at this company "
                         "held a title in your hierarchy list).")], "nomatch"

    ranked = ranked[:max_contacts]

    if preview:
        rows = []
        # in preview, show whether each would be free (cached) or paid
        cached = {(_norm(r["First Name"]) + " " + _norm(r["Last Name"])).strip()
                  for r in (cache_lookup_company(company) if use_cache else [])}
        for t, c in ranked:
            name = (c.get("name") or f"{c.get('firstName','')} {c.get('lastName','')}").strip()
            fn, ln = _split_name(c.get("name")) if not c.get("firstName") else (c.get("firstName"), c.get("lastName"))
            is_cached = (_norm(fn) + " " + _norm(ln)).strip() in cached
            row = contact_row(company, t, c, {},
                    note=(f"PREVIEW: already saved - would reuse FREE."
                          if is_cached else f"PREVIEW: would research {name} ({TIER_LABEL.get(t)}). ~1 credit."))
            row["Source"] = "From cache (free)" if is_cached else "Would research"
            rows.append(row)
        return rows, "would"

    # research all selected in ONE batch call (fast) then poll once
    ranked_with_id = [(t, c) for t, c in ranked if _id_of(c)]
    ids = [_id_of(c) for _, c in ranked_with_id]
    enriched_by_key = {}
    ordered_results = []
    if ids:
        req = research_ids(ids, headers)
        results = poll_research(req, headers, interval=poll_interval, attempts=poll_attempts)
        for r in results:
            if not isinstance(r, dict):
                continue
            status = r.get("status")
            c = r.get("contact") if isinstance(r.get("contact"), dict) else r
            # Only skip explicit failures that carry no data.
            if status in ("error", "missing", "researching") and not (
                    c.get("email") or c.get("email1") or c.get("firstName")):
                continue
            ordered_results.append(c)
            # The poll item's TOP-LEVEL searchResultId matches the search id we submitted.
            top_id = r.get("searchResultId") or r.get("searchResultID")
            if top_id:
                enriched_by_key[str(top_id)] = c
            # also register any ids inside the contact object, and the name
            for k in (c.get("searchResultId"), c.get("id"), c.get("contactId")):
                if k:
                    enriched_by_key[str(k)] = c
            nm = _norm(f"{c.get('firstName','')} {c.get('lastName','')}") or _norm(c.get("name"))
            if nm.strip():
                enriched_by_key["name:" + nm] = c

    rows = []
    for idx, (t, c) in enumerate(ranked_with_id):
        cid = _id_of(c)
        nm = _norm(f"{c.get('firstName','')} {c.get('lastName','')}") or _norm(c.get("name"))
        enr = (enriched_by_key.get(str(cid))
               or enriched_by_key.get("name:" + nm)
               or (ordered_results[idx] if idx < len(ordered_results) else None)  # order fallback
               or {})
        rows.append(contact_row(company, t, c, enr))
    # include any ranked contacts that had no id (rare) so nothing is dropped
    for t, c in ranked:
        if _id_of(c) is None:
            rows.append(contact_row(company, t, c, {}))

    # SAVE newly researched contacts to the master store for next time
    if use_cache:
        try: cache_save(rows)
        except Exception: pass
    return rows, "found"


def write_xlsx(rows, path):
    wb = Workbook(); ws = wb.active; ws.title = "Contacts"; ws.append(OUTPUT_COLUMNS)
    for row in rows:
        ws.append([row.get(k, "") for k in OUTPUT_COLUMNS])
    wb.save(path)


# ============================================================================
# GOOGLE DRIVE upload (optional). Enabled when these env vars are set:
#   GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN, GDRIVE_FOLDER_ID
# Uploads a local file into the shared Drive folder. Uses only the refresh token
# (no browser needed on the server). Fails soft: never breaks a run.
# ============================================================================
GDRIVE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GDRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true"

def drive_enabled():
    return all(os.environ.get(k) for k in
               ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN", "GDRIVE_FOLDER_ID"))

def _drive_access_token():
    r = requests.post(GDRIVE_TOKEN_URI, data={
        "client_id": os.environ["GDRIVE_CLIENT_ID"],
        "client_secret": os.environ["GDRIVE_CLIENT_SECRET"],
        "refresh_token": os.environ["GDRIVE_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def drive_upload(local_path, drive_name=None, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
    """Upload a file to the configured Drive folder. Returns the Drive file link, or None."""
    if not drive_enabled():
        return None
    try:
        token = _drive_access_token()
        meta = {"name": drive_name or os.path.basename(local_path),
                "parents": [os.environ["GDRIVE_FOLDER_ID"]]}
        with open(local_path, "rb") as f:
            data = f.read()
        boundary = "----bimboundary7hf83n"
        body = (
            (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
             f"{json.dumps(meta)}\r\n").encode()
            + f"--{boundary}\r\nContent-Type: {mime}\r\n\r\n".encode()
            + data + f"\r\n--{boundary}--".encode()
        )
        r = requests.post(GDRIVE_UPLOAD_URL, headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        }, data=body, timeout=120)
        r.raise_for_status()
        fid = r.json().get("id")
        return f"https://drive.google.com/file/d/{fid}/view" if fid else None
    except Exception as e:
        log(f"      [drive] upload failed: {e}")
        return None


# ---------------------------------------------------------------------------
# PERMANENT MASTER stored as a single Excel file in Drive (survives free-tier
# restarts). Flow each run: pull master from Drive -> merge into local SQLite ->
# push updated master back to the same Drive file.
# ---------------------------------------------------------------------------
MASTER_DRIVE_NAME = "BIM_MASTER_CONTACTS.xlsx"
GDRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
GDRIVE_UPDATE_URL = "https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=media&supportsAllDrives=true"
_MASTER_SYNC_LOCK = threading.Lock()

def _drive_find_master(token):
    """Return the fileId of the master Excel in the folder, or None."""
    folder = os.environ["GDRIVE_FOLDER_ID"]
    q = f"name='{MASTER_DRIVE_NAME}' and '{folder}' in parents and trashed=false"
    r = requests.get(GDRIVE_FILES_URL, headers={"Authorization": f"Bearer {token}"},
                     params={"q": q, "fields": "files(id,name)",
                             "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"},
                     timeout=30)
    r.raise_for_status()
    files = r.json().get("files", [])
    return files[0]["id"] if files else None

def _drive_download(token, fid, dest_path):
    r = requests.get(f"{GDRIVE_FILES_URL}/{fid}",
                     headers={"Authorization": f"Bearer {token}"},
                     params={"alt": "media", "supportsAllDrives": "true"}, timeout=120)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)

def _import_master_xlsx_into_db(path):
    """Load a master Excel (exported by export_master_xlsx) back into the SQLite cache."""
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return 0
    hdr = [str(h).strip() if h else "" for h in rows[0]]
    idx = {h: i for i, h in enumerate(hdr)}
    def g(r, key):
        i = idx.get(key)
        return (r[i] if (i is not None and i < len(r) and r[i] is not None) else "")
    imported = []
    for r in rows[1:]:
        if not any(v not in (None, "") for v in r):
            continue
        imported.append({
            "Company Name": g(r, "Company"), "Tier": g(r, "Tier"),
            "Hierarchy Role": g(r, "Hierarchy Role"), "First Name": g(r, "First Name"),
            "Last Name": g(r, "Last Name"), "Job Title": g(r, "Job Title"),
            "Email": g(r, "Email"), "Phone Number": g(r, "Phone Number"),
            "LinkedIn Profile": g(r, "LinkedIn Profile"), "Company Domain": g(r, "Company Domain"),
        })
    if imported:
        cache_save(imported)   # upsert into SQLite by cache_key
    return len(imported)

def master_pull_from_drive():
    """Download the Drive master (if any) and merge it into the local SQLite. Safe no-op if Drive off."""
    if not drive_enabled():
        return 0
    try:
        token = _drive_access_token()
        fid = _drive_find_master(token)
        if not fid:
            return 0
        tmp = os.path.join(os.path.dirname(MASTER_DB), "_master_pull.xlsx")
        _drive_download(token, fid, tmp)
        n = _import_master_xlsx_into_db(tmp)
        try: os.remove(tmp)
        except Exception: pass
        return n
    except Exception as e:
        log(f"      [drive] master pull failed: {e}")
        return 0

def master_push_to_drive():
    """Export the whole SQLite cache to the single permanent master file in Drive (create or overwrite)."""
    if not drive_enabled():
        return None
    try:
        token = _drive_access_token()
        tmp = os.path.join(os.path.dirname(MASTER_DB), "_master_push.xlsx")
        export_master_xlsx(tmp)
        fid = _drive_find_master(token)
        with open(tmp, "rb") as f:
            data = f.read()
        try: os.remove(tmp)
        except Exception: pass
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if fid:
            # overwrite existing master (keeps same file & link)
            r = requests.patch(GDRIVE_UPDATE_URL.format(fid=fid),
                               headers={"Authorization": f"Bearer {token}", "Content-Type": mime},
                               data=data, timeout=120)
            r.raise_for_status()
        else:
            # create it once
            boundary = "----bimmaster7hf83n"
            meta = {"name": MASTER_DRIVE_NAME, "parents": [os.environ["GDRIVE_FOLDER_ID"]]}
            body = ((f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{json.dumps(meta)}\r\n").encode()
                    + f"--{boundary}\r\nContent-Type: {mime}\r\n\r\n".encode() + data
                    + f"\r\n--{boundary}--".encode())
            r = requests.post(GDRIVE_UPLOAD_URL,
                              headers={"Authorization": f"Bearer {token}",
                                       "Content-Type": f"multipart/related; boundary={boundary}"},
                              data=body, timeout=120)
            r.raise_for_status()
            fid = r.json().get("id")
        return f"https://drive.google.com/file/d/{fid}/view" if fid else None
    except Exception as e:
        log(f"      [drive] master push failed: {e}")
        return None

def sync_master_before_run():
    """Pull the permanent Drive master into local cache before a run (thread-safe)."""
    with _MASTER_SYNC_LOCK:
        return master_pull_from_drive()

def sync_master_after_run():
    """Push the updated local cache back to the permanent Drive master (thread-safe)."""
    with _MASTER_SYNC_LOCK:
        return master_push_to_drive()


def main():
    ap = argparse.ArgumentParser(description="BIM Data Execution - all hierarchy contacts per company")
    ap.add_argument("--input", required=True); ap.add_argument("--output", default="results.xlsx")
    ap.add_argument("--api-key", default=os.environ.get("SEAMLESS_API_KEY", ""))
    ap.add_argument("--max-contacts", type=int, default=MAX_CONTACTS_PER_COMPANY)
    args = ap.parse_args()
    if not args.api_key:
        log("ERROR: no API key. Pass --api-key or set SEAMLESS_API_KEY."); sys.exit(1)
    headers = auth_headers(args.api_key)
    companies = load_companies(args.input)
    log(f"Loaded {len(companies)} companies.")
    all_rows = []
    for i, company in enumerate(companies, 1):
        log(f"[{i}/{len(companies)}] {company}")
        try:
            rows, kind = process_company(company, headers, max_contacts=args.max_contacts)
        except Exception as e:
            rows = [note_row(company, f"ERROR: {e}")]
        all_rows.extend(rows)
        for r in rows:
            who = f"{r['First Name']} {r['Last Name']}".strip()
            log(f"      {r.get('Hierarchy Role') or ''}: {who or r['Note']}")
        try: write_xlsx(all_rows, args.output)
        except Exception: pass
        time.sleep(DELAY_BETWEEN_COMPANIES)
    write_xlsx(all_rows, args.output)
    log(f"\nDone. Wrote {len(all_rows)} rows to {args.output}")


if __name__ == "__main__":
    main()