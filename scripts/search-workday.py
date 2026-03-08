#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-workday.py
Workday API scraper — NO JavaScript rendering required.

Strategy:
  1. Tenant discovery — Exa search finds *.myworkdayjobs.com job URLs → extract all employers
  2. CXS JSON API   — paginate all jobs per employer, filter Ontario (fast, no JS)
  3. HTML job page  — salary in raw HTML (meta content attrs); pure regex, no LLM

Seed tenants (8 known) + dynamic discovery via Exa = covers ALL Workday employers
posting Ontario jobs, not just the predefined list.

Run: python3 ~/ontario-pay-hub/scripts/search-workday.py
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import date, timedelta

from _common import (
    make_logger, acquire_lock, exa_search, load_existing_keys, write_job,
    TODAY, OUTPUT_FILE,
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/workday.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.workday.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=60)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# ── Seed tenants (known-good with verified tenant IDs) ────────────────────────
# (host, company_id, tenant, display_name)
SEED_TENANTS = [
    ("rbc.wd3.myworkdayjobs.com",       "rbc",       "RBCGLOBAL1",        "RBC"),
    ("td.wd3.myworkdayjobs.com",         "td",        "TD_Bank_Careers",   "TD Bank"),
    ("bmo.wd3.myworkdayjobs.com",        "bmo",       "External",          "BMO"),
    ("cibc.wd3.myworkdayjobs.com",       "cibc",      "search",            "CIBC"),
    ("manulife.wd3.myworkdayjobs.com",   "manulife",  "MFCJH_Jobs",        "Manulife"),
    ("sunlife.wd3.myworkdayjobs.com",    "sunlife",   "Experienced-Jobs",  "Sun Life"),
    ("walmart.wd5.myworkdayjobs.com",    "walmart",   "WalmartExternal",   "Walmart Canada"),
    ("brookfield.wd5.myworkdayjobs.com", "brookfield","brookfield",        "Brookfield"),
]

# Exa queries to discover additional Workday tenants (Ontario employers)
DISCOVERY_QUERIES = [
    'site:myworkdayjobs.com Ontario Canada job 2026',
    'site:myworkdayjobs.com Toronto OR Ottawa OR Waterloo job 2026',
    'site:myworkdayjobs.com Ontario Canada salary "$" CAD',
    'site:myworkdayjobs.com "Ontario" Canada engineer OR analyst OR manager OR director',
]

# Ontario location terms — Workday-specific: includes "locations" for multi-site postings
# and ", on," (with trailing comma) to match Workday's locationsText comma format
ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "london", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on,",
    "locations",  # "2 Locations" = multi-location, may include Ontario
]

# Salary regex patterns for Workday HTML (no LLM — regex is sufficient for structured pages)
SALARY_RE = [
    # "$86,100.00 CAD - $136,100 CAD" or "between $86,100 and $136,100"
    re.compile(r'\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000" (without dollar sign)
    re.compile(r'(?:pay|salary|compensation|wage)[^$\n]{0,30}([\d,]{6,})\s*[-–—to]+\s*([\d,]{6,})', re.IGNORECASE),
]


# ── Tenant discovery via Exa ──────────────────────────────────────────────────
# Workday URL format: https://rbc.wd3.myworkdayjobs.com/en-US/RBCGLOBAL1/job/...
_WD_URL_RE = re.compile(
    r'https?://([a-z0-9][a-z0-9-]*)\.wd\d+\.myworkdayjobs\.com'
    r'(?:/[a-z]{2}-[A-Z]{2})?/([^/?#]+)',
    re.IGNORECASE,
)
_SKIP_TENANTS = {'job', 'jobs', 'search', 'en', 'en-us', 'en-gb', 'fr', 'fr-ca'}


def parse_workday_tenant(url):
    """Extract (host, company_id, tenant) from a myworkdayjobs.com URL, or None."""
    m = _WD_URL_RE.match(url)
    if not m:
        return None
    company_id = m.group(1).lower()
    host_m = re.match(r'https?://([^/]+)', url)
    if not host_m:
        return None
    host = host_m.group(1).lower()
    tenant = m.group(2)
    if tenant.lower() in _SKIP_TENANTS or len(tenant) < 3:
        return None
    return host, company_id, tenant


def discover_tenants():
    """Use Exa to find myworkdayjobs.com job URLs and extract unique (host, tenant) pairs."""
    discovered = {}  # host → (host, company_id, tenant, display_name)

    for i, query in enumerate(DISCOVERY_QUERIES, 1):
        log(f"  Discovery Exa [{i}/{len(DISCOVERY_QUERIES)}]: {query[:60]}...")
        resp = exa_search(query, num_results=15, start_date=LOOKBACK_DATE, log=log)
        if not resp:
            continue
        results = resp.get("results", [])
        new = 0
        for r in results:
            parsed = parse_workday_tenant(r.get("url", ""))
            if parsed and parsed[0] not in discovered:
                host, company_id, tenant = parsed
                discovered[host] = (host, company_id, tenant, company_id.upper())
                new += 1
        log(f"    → {len(results)} results, {new} new tenants")
        time.sleep(1.5)

    return list(discovered.values())


# ── Workday CXS JSON API ──────────────────────────────────────────────────────
# NOTE: Python's TLS stack (http.client / urllib) has a distinct JA3 fingerprint
# that Cloudflare identifies as bot traffic after repeated calls and rate-limits
# with HTTP 400. curl uses a browser-like TLS fingerprint and is not affected.
# Solution: delegate API calls to curl via subprocess.

def wd_list_jobs(host, company_id, tenant, offset=0, limit=50):
    """Return (job_postings, total) from Workday CXS API via curl.

    curl's TLS fingerprint (JA3) passes Cloudflare's bot detection;
    Python's http.client/urllib fingerprint gets blocked after repeated calls.
    """
    url = f"https://{host}/wday/cxs/{company_id}/{tenant}/jobs"
    body = json.dumps({
        "appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""
    })
    cmd = [
        "curl", "-s", "--max-time", "20",
        "-X", "POST", url,
        "-H", "Content-Type: application/json",
        "-H", "Accept: application/json",
        "-H", f"User-Agent: {UA}",
        "-d", body,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=25)
        if result.returncode != 0:
            log(f"  curl error ({host}): {result.stderr.decode()[:100]}")
            return [], 0
        data = json.loads(result.stdout)
        if "total" not in data:
            log(f"  API {host}: unexpected response: {result.stdout[:80]}")
            return [], 0
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
    path_lower = external_path.lower()
    for city, label in city_map.items():
        if city.replace(" ", "-") in path_lower or city.replace(" ", "") in path_lower:
            return label
    return "Ontario, ON"


# ── Job HTML fetch + salary extraction ────────────────────────────────────────
def fetch_job_html(host, tenant, external_path):
    """Fetch a Workday job page and return raw HTML.

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
    """Try to extract min/max annual CAD salary from page HTML. Returns (min, max) or None."""
    if not text:
        return None
    for pattern in SALARY_RE:
        m = pattern.search(text)
        if m:
            try:
                raw_min = m.group(1).replace(",", "")
                raw_max = m.group(2).replace(",", "")
                if "k" in m.group(0).lower():
                    val_min = int(float(raw_min) * 1000)
                    val_max = int(float(raw_max) * 1000)
                else:
                    val_min = int(float(raw_min))
                    val_max = int(float(raw_max))
                if 25_000 <= val_min <= 700_000 and val_min < val_max:
                    return val_min, val_max
            except (ValueError, IndexError):
                continue
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Workday scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    # Build tenant list: seed (known-good) + dynamically discovered via Exa
    log(f"Seed tenants: {len(SEED_TENANTS)} | Running tenant discovery via Exa...")
    discovered = discover_tenants()

    # Merge: seed takes priority (has proper display names + verified tenant IDs)
    seed_hosts = {t[0] for t in SEED_TENANTS}
    extra = [t for t in discovered if t[0] not in seed_hosts]
    all_tenants = SEED_TENANTS + extra
    log(f"Total tenants: {len(all_tenants)} ({len(SEED_TENANTS)} seed + {len(extra)} discovered)")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    total_found = 0

    for host, company_id, tenant, company_name in all_tenants:
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
                if is_ontario(p.get("locationsText", "")):
                    ontario_jobs.append(p)
            offset += limit
            if offset >= total:
                break
            time.sleep(0.5)

        log(f"  Ontario jobs: {len(ontario_jobs)}")

        # Fetch HTML for each Ontario job and extract salary
        for i, posting in enumerate(ontario_jobs, 1):
            title    = posting.get("title", "").strip()
            ext_path = posting.get("externalPath", "")
            posted_on = posting.get("postedOn", TODAY)
            locations = posting.get("locationsText", "")

            # Deduplicate early — skip HTML fetch if already known
            key = f"{title.lower()}|{company_name.lower()}"
            if key in seen_keys:
                continue

            log(f"  [{i}/{len(ontario_jobs)}] {title[:55]}")
            text = fetch_job_html(host, tenant, ext_path)
            if not text:
                log("    → fetch failed")
                time.sleep(0.5)
                continue

            salary = extract_salary(text)
            if not salary:
                log("    → no salary")
                time.sleep(0.3)
                continue

            val_min, val_max = salary
            location = parse_location(locations, ext_path)
            source_url = f"https://{host}/en-US/{tenant}{ext_path}"

            # Parse posted date — Workday returns "Posted 30+ Days Ago", "Posted Today", or ISO date
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
            write_job(OUTPUT_FILE, job)
            total_found += 1
            log(f"    → FOUND: ${val_min:,}–${val_max:,} [{location}]")
            time.sleep(0.8)

        # 60s pause between companies — confirmed minimum needed to avoid Workday rate limiting.
        # Single calls work fine; rapid sequential calls (< ~30s apart) trigger HTTP 400 blocks.
        time.sleep(60)

    log(f"\n=== Workday scraper complete: {total_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
