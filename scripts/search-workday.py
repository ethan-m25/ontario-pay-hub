#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-workday.py
Workday API scraper — NO JavaScript rendering required.

Strategy:
  1. CXS JSON API  → paginate all jobs, filter Ontario locations (fast, no JS)
  2. HTML job page → server-rendered, salary embedded in plain text (no JS)
  3. Pure regex extraction — no LLM needed, very fast (~1-2s/job)

Covers 8 major Ontario employers:
  RBC, TD Bank, BMO, CIBC, Manulife, Sun Life, Walmart Canada, Brookfield

Run: python3 ~/ontario-pay-hub/scripts/search-workday.py
"""

import atexit
import http.client
import json
import os
import re
import signal
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import date

socket.setdefaulttimeout(20)

TODAY        = date.today().isoformat()
SHARED_DIR   = os.path.expanduser("~/.openclaw/shared")
OUTPUT_FILE  = os.path.join(SHARED_DIR, f"ontario-jobs-raw-{TODAY}.txt")
LOG_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/workday.log")
LOCK_FILE    = os.path.expanduser("~/ontario-pay-hub/scripts/.workday.lock")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# (host, company_id, tenant, display_name)
# host format: subdomain.wdN.myworkdayjobs.com
WORKDAY_TENANTS = [
    ("rbc.wd3.myworkdayjobs.com",       "rbc",       "RBCGLOBAL1",        "RBC"),
    ("td.wd3.myworkdayjobs.com",         "td",        "TD_Bank_Careers",   "TD Bank"),
    ("bmo.wd3.myworkdayjobs.com",        "bmo",       "External",          "BMO"),
    ("cibc.wd3.myworkdayjobs.com",       "cibc",      "search",            "CIBC"),
    ("manulife.wd3.myworkdayjobs.com",   "manulife",  "MFCJH_Jobs",        "Manulife"),
    ("sunlife.wd3.myworkdayjobs.com",    "sunlife",   "Experienced-Jobs",  "Sun Life"),
    ("walmart.wd5.myworkdayjobs.com",    "walmart",   "WalmartExternal",   "Walmart Canada"),
    ("brookfield.wd5.myworkdayjobs.com", "brookfield","brookfield",        "Brookfield"),
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "london", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on,",
    "locations",  # "2 Locations" = multi-location, may include Ontario
]

# Salary patterns for Workday HTML (no LLM — regex is sufficient)
SALARY_RE = [
    # "$86,100.00 CAD - $136,100 CAD" or "between $86,100 and $136,100"
    re.compile(r'\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000" (without dollar sign)
    re.compile(r'(?:pay|salary|compensation|wage)[^$\n]{0,30}([\d,]{6,})\s*[-–—to]+\s*([\d,]{6,})', re.IGNORECASE),
]


# ── Lock file ─────────────────────────────────────────────────────────────────
def _release_lock():
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def _acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)
            log(f"Another instance running (PID {old_pid}). Exiting.")
            return False
        except (OSError, ValueError):
            log("Stale lock file — removing.")
            os.remove(LOCK_FILE)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(_release_lock)
    signal.signal(signal.SIGTERM, lambda s, f: (_release_lock(), sys.exit(1)))
    return True


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Workday CXS JSON API ──────────────────────────────────────────────────────
_ssl_ctx = ssl.create_default_context()


def wd_list_jobs(host, company_id, tenant, offset=0, limit=50):
    """Return list of job postings from Workday CXS API.

    Uses http.client directly to avoid urllib's automatic 'Accept-Encoding: identity'
    header, which some Workday tenants reject with HTTP 400.
    """
    path = f"/wday/cxs/{company_id}/{tenant}/jobs"
    body = json.dumps({
        "appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": UA,
        "Content-Length": str(len(body)),
    }
    try:
        conn = http.client.HTTPSConnection(host, context=_ssl_ctx, timeout=20)
        conn.request("POST", path, body=body, headers=headers)
        resp = conn.getresponse()
        if resp.status != 200:
            log(f"  API {host}: HTTP {resp.status}")
            return [], 0
        data = json.loads(resp.read())
        conn.close()
        return data.get("jobPostings", []), data.get("total", 0)
    except Exception as e:
        log(f"  API error ({host}): {e}")
        return [], 0


def is_ontario(locations_text):
    lt = (locations_text or "").lower()
    return any(t in lt for t in ONTARIO_TERMS)


def parse_location(locations_text, external_path):
    """Best-effort Ontario city from locationsText or URL path."""
    lt = (locations_text or "").lower()
    city_map = {
        "toronto": "Toronto, ON", "ottawa": "Ottawa, ON",
        "waterloo": "Waterloo, ON", "mississauga": "Mississauga, ON",
        "hamilton": "Hamilton, ON", "london": "London, ON",
        "brampton": "Brampton, ON", "markham": "Markham, ON",
        "vaughan": "Vaughan, ON", "oakville": "Oakville, ON",
        "kitchener": "Kitchener, ON", "windsor": "Windsor, ON",
        "richmond hill": "Richmond Hill, ON",
    }
    for city, label in city_map.items():
        if city in lt:
            return label
    # Try from external_path
    path_lower = external_path.lower()
    for city, label in city_map.items():
        if city.replace(" ", "-") in path_lower or city.replace(" ", "") in path_lower:
            return label
    return "Ontario, ON"


# ── Job HTML fetch + salary extraction ───────────────────────────────────────
def fetch_job_html(host, tenant, external_path):
    """Fetch Workday job page and return raw HTML.

    Workday pages are JS SPAs (body = <div id="root"></div>), but the job
    description and salary range are embedded in <meta content="..."> attributes
    in the <head>. Stripping tags would discard those attribute values, so we
    return the raw HTML and let extract_salary search it directly.
    """
    url = f"https://{host}/en-US/{tenant}{external_path}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "text/html,application/xhtml+xml,*/*;q=0.9")
    req.add_header("Accept-Language", "en-CA,en;q=0.9")
    try:
        with urllib.request.urlopen(req, timeout=18) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def extract_salary(text):
    """Try to extract min/max annual CAD salary from page text. Returns (min, max) or None."""
    if not text:
        return None
    for pattern in SALARY_RE:
        m = pattern.search(text)
        if m:
            try:
                raw_min = m.group(1).replace(",", "")
                raw_max = m.group(2).replace(",", "")
                # Handle k suffix
                if "k" in m.group(0).lower():
                    val_min = int(float(raw_min) * 1000)
                    val_max = int(float(raw_max) * 1000)
                else:
                    val_min = int(float(raw_min))
                    val_max = int(float(raw_max))
                # Sanity check
                if 25_000 <= val_min <= 700_000 and val_min < val_max:
                    return val_min, val_max
            except (ValueError, IndexError):
                continue
    return None


def load_existing_keys():
    data_file = os.path.expanduser("~/ontario-pay-hub/data/jobs.json")
    try:
        with open(data_file) as f:
            db = json.load(f)
        return set(
            f"{j['role'].lower().strip()}|{j['company'].lower().strip()}"
            for j in db.get("jobs", [])
        )
    except Exception:
        return set()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not _acquire_lock():
        return 1

    log("=== Workday scraper started ===")
    log(f"Tenants: {len(WORKDAY_TENANTS)} | Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(SHARED_DIR, exist_ok=True)

    total_found = 0

    for host, company_id, tenant, company_name in WORKDAY_TENANTS:
        log(f"\n── {company_name} ({host}) ──")

        # Paginate through all jobs, collect Ontario ones
        ontario_jobs = []
        offset = 0
        limit = 50
        max_pages = 6  # cap at 300 jobs per company per run

        while offset // limit < max_pages:
            postings, total = wd_list_jobs(host, company_id, tenant, offset, limit)
            if not postings:
                break
            log(f"  API offset={offset}: {len(postings)} postings (total={total})")
            for p in postings:
                lt = p.get("locationsText", "")
                if is_ontario(lt):
                    ontario_jobs.append(p)
            offset += limit
            if offset >= total:
                break
            time.sleep(0.5)

        log(f"  Ontario jobs: {len(ontario_jobs)}")

        # Fetch HTML for each Ontario job and extract salary
        for i, posting in enumerate(ontario_jobs, 1):
            title       = posting.get("title", "").strip()
            ext_path    = posting.get("externalPath", "")
            posted_on   = posting.get("postedOn", TODAY)
            locations   = posting.get("locationsText", "")

            # Deduplicate early
            key = f"{title.lower()}|{company_name.lower()}"
            if key in seen_keys:
                continue

            log(f"  [{i}/{len(ontario_jobs)}] {title[:55]}")
            text = fetch_job_html(host, tenant, ext_path)
            if not text:
                log(f"    → fetch failed")
                time.sleep(0.5)
                continue

            salary = extract_salary(text)
            if not salary:
                log(f"    → no salary")
                time.sleep(0.3)
                continue

            val_min, val_max = salary
            location = parse_location(locations, ext_path)
            source_url = f"https://{host}/en-US/{tenant}{ext_path}"

            # Parse posted date
            # Workday returns "Posted 30+ Days Ago", "Posted Today", or ISO date
            posted = TODAY
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', posted_on or "")
            if date_match:
                posted = date_match.group(1)

            job = {
                "role":       title,
                "company":    company_name,
                "min":        val_min,
                "max":        val_max,
                "location":   location,
                "source_url": source_url,
                "posted":     posted,
            }

            seen_keys.add(key)
            with open(OUTPUT_FILE, "a") as f:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
            total_found += 1
            log(f"    → FOUND: ${val_min:,}–${val_max:,} [{location}]")
            time.sleep(0.8)

        # Brief pause between companies to avoid Workday API rate limiting
        time.sleep(2)

    log(f"\n=== Workday scraper complete: {total_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
