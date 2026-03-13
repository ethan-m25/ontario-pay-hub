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
    # Major Canadian banks
    ("rbc.wd3.myworkdayjobs.com",       "rbc",       "RBCGLOBAL1",        "RBC"),
    ("td.wd3.myworkdayjobs.com",        "td",        "TD_Bank_Careers",   "TD Bank"),
    ("bmo.wd3.myworkdayjobs.com",       "bmo",       "External",          "BMO"),
    ("cibc.wd3.myworkdayjobs.com",      "cibc",      "search",            "CIBC"),
    # Insurance / financial services
    ("manulife.wd3.myworkdayjobs.com",  "manulife",  "MFCJH_Jobs",        "Manulife"),
    ("sunlife.wd3.myworkdayjobs.com",   "sunlife",   "Experienced-Jobs",  "Sun Life"),
    # Pension funds (Toronto-HQ, large Ontario employers)
    ("omers.wd3.myworkdayjobs.com",     "omers",     "OMERS_External",    "OMERS"),
    # Real estate / asset management
    ("brookfield.wd5.myworkdayjobs.com","brookfield","brookfield",        "Brookfield"),
    # Retail
    ("walmart.wd5.myworkdayjobs.com",   "walmart",   "WalmartExternal",   "Walmart Canada"),
]

# Known company_id → display name overrides for dynamically discovered tenants.
# Keyed by the Workday subdomain (company_id, lowercase).
# Add entries here when a new bad name appears in the data.
KNOWN_COMPANY_OVERRIDES = {
    "talentmanagementsolution": "Jonas Software Canada",
    "intactfc":                 "Intact Financial Corporation",
    "sdm":                      "Shoppers Drug Mart",
    "myview":                   "Shoppers Drug Mart",
    "colliers1":                "Colliers",
    "ontarioteachers":          "Ontario Teachers' Pension Plan",
    "cppinvestments":           "CPP Investments",
    "canadalife":               "Canada Life",
    "hydro1":                   "Hydro One",
    "opg":                      "Ontario Power Generation",
}

# Exa queries to discover additional Workday tenants (Ontario employers)
# Runs each nightly — finds newly indexed job URLs and extracts myworkdayjobs.com tenant IDs.
# Queries target specific major Ontario employers and sectors to fill coverage gaps.
DISCOVERY_QUERIES = [
    # General Ontario coverage
    'site:myworkdayjobs.com Ontario Canada job 2026',
    'site:myworkdayjobs.com Toronto OR Ottawa OR Waterloo job 2026',
    'site:myworkdayjobs.com Ontario Canada salary "$" CAD',
    'site:myworkdayjobs.com "Ontario" Canada engineer OR analyst OR manager OR director',
    # Major Ontario financial / investment employers
    'site:myworkdayjobs.com "CPP Investments" OR "CPPIB" OR "OMERS" OR "Ontario Teachers" Ontario',
    'site:myworkdayjobs.com "Canada Life" OR "Great-West Life" OR "Intact" Ontario Canada',
    'site:myworkdayjobs.com "Fairfax" OR "Mackenzie" OR "IGM Financial" OR "Power Corporation" Ontario',
    # Ontario tech / professional services
    'site:myworkdayjobs.com "OpenText" OR "Celestica" OR "Mitel" OR "Descartes" Ontario Canada',
    'site:myworkdayjobs.com "Colliers" OR "CBRE" OR "Avison Young" OR "Hatch" Ontario salary',
    # Ontario mining / energy / resources (Toronto-HQ companies)
    'site:myworkdayjobs.com "Kinross" OR "Agnico" OR "Barrick" OR "Teck" Ontario Canada',
    'site:myworkdayjobs.com "Enbridge" OR "TC Energy" OR "OPG" OR "Hydro One" Ontario',
    # Ontario healthcare / pharma (beyond Roche)
    'site:myworkdayjobs.com "Sanofi" OR "AstraZeneca" OR "Novartis" OR "Bayer" Ontario salary',
]

# Ontario location terms — matched against locationsText from Workday API.
# Removed: "locations" (matched ANY "2 Locations" posting including US jobs)
# Removed: "london" (ambiguous — also matches London, England in global tenants like OMERS)
# London, Ontario is caught by _ONTARIO_PATH_TERMS via "/job/London-Ontario/" URL pattern.
ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on,",
]

# Ontario indicators in Workday URL path (used for secondary check when locationsText is vague)
_ONTARIO_PATH_TERMS = ["-ontario", "-on-can", "/ontario-", "can-ontario", "/can-on-"]

# Non-Ontario location patterns in URL path — these explicitly contradict an Ontario match.
# Covers US states, BC, AB, QC to reject false positives from global Workday tenants.
_NON_ONTARIO_PATH_TERMS = [
    "-usa/", "-usa-", "az-usa", "us-telecommut", "us-remote",
    "/new-york/", "/new-york-city/", "/california/", "/texas/", "/florida/",
    "/north-carolina/", "/new-jersey/", "/south-san-francisco",
    "/wellesley-hills", "/richmond/", "/phoenix/", "/chicago/",
    "/boston/", "/seattle/", "/atlanta/", "/san-francisco/", "/los-angeles/",
    "/emeryville", "/new-haven", "/new-brunswick",   # more US cities
    "/london-london",   # London, England (OMERS-style "London, London" → /job/London-London/)
    "/united-kingdom", "/england",
    "/vancouver/", "/british-columbia", "/alberta/", "/quebec/",
]

# Salary regex patterns for Workday HTML (no LLM — regex is sufficient for structured pages)
# NOTE: Canadian job postings use both "$" and "C$" (e.g. Brookfield: "C$90,000.00 - C$105,000.00")
# The (?:[A-Z])? prefix on \$ handles C$, US$, etc. without breaking plain-$ matches.
SALARY_RE = [
    # "$86,100.00 CAD - $136,100 CAD" or "C$90,000 - C$105,000" (Brookfield style)
    re.compile(r'(?:[A-Z])?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z])?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K" or "C$86K – C$136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
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


def format_tenant_name(company_id, tenant):
    """Derive a human-readable company name from Workday identifiers.

    Resolution order:
    1. KNOWN_COMPANY_OVERRIDES dict (manual corrections for known bad names)
    2. CamelCase-split of the tenant string (e.g. "JonasSoftwareCanada" → "Jonas Software Canada")
    3. company_id title-cased as last resort
    """
    # Tier 1: known overrides
    override = KNOWN_COMPANY_OVERRIDES.get(company_id.lower())
    if override:
        return override

    # Tier 2: CamelCase-split the tenant name (skip generic single-word tenants)
    # Strip common boilerplate suffixes first
    clean = re.sub(r'(?i)(External|Careers?|Jobs?|_[A-Z]{2}$)', '', tenant)
    clean = clean.replace('_', ' ').strip()
    # Split on CamelCase boundaries
    words = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean).split()
    if len(words) >= 2:
        # Drop trailing generic words like "Canada" only if > 2 words
        return ' '.join(words)

    # Tier 3: company_id, title-cased and de-hyphenated
    return company_id.replace('-', ' ').title()


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
                discovered[host] = (host, company_id, tenant, format_tenant_name(company_id, tenant))
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


def is_ontario(locations_text, external_path=""):
    """Return True only if the job is plausibly located in Ontario.

    Two-stage check:
    1. Reject if the URL path contains an explicit non-Ontario location (US state, BC, AB, QC).
    2. Accept if locationsText mentions an Ontario city/term, OR if the URL path contains
       an Ontario path segment (covers jobs whose locationsText is just a generic city name
       that didn't match, but whose Workday URL makes the province clear).
    """
    ep = (external_path or "").lower()
    lt = (locations_text or "").lower()

    # Stage 1: reject if URL path names a non-Ontario location
    if any(t in ep for t in _NON_ONTARIO_PATH_TERMS):
        return False

    # Stage 2: accept on explicit Ontario term in locationsText OR Ontario URL segment
    return (
        any(t in lt for t in ONTARIO_TERMS)
        or any(t in ep for t in _ONTARIO_PATH_TERMS)
    )


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


def extract_company_from_html(text):
    """Try to extract the real hiring organization name from Workday job page HTML.

    Workday embeds structured data in a <script type="application/ld+json"> block.
    The hiringOrganization.name field holds the canonical company name.
    Falls back to og:site_name meta tag. Returns None if not found.
    """
    if not text:
        return None

    # Try JSON-LD first (most reliable)
    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text, re.DOTALL | re.IGNORECASE
    )
    for block in ld_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                data = data[0]
            org = data.get("hiringOrganization", {})
            name = org.get("name", "").strip()
            if name and len(name) > 1:
                return name
        except (json.JSONDecodeError, AttributeError, IndexError):
            continue

    # Fallback: og:site_name meta tag
    m = re.search(r'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']+)["\']',
                  text, re.IGNORECASE)
    if not m:
        m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:site_name["\']',
                      text, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if name and name.lower() not in ('workday', 'myworkdayjobs.com'):
            return name

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
        limit = 10     # Workday blocks limit >= 25 (anti-scraping); 10 is safe
        max_pages = 5  # covers 50 most recent jobs per company

        # wd5 tenants (Brookfield, Walmart) return total=0 on offset>0 despite having more jobs.
        # Track the first valid total and use it for pagination decisions; if a page returns
        # total=0 we still continue until we hit max_pages or get an empty postings list.
        known_total = 0
        while offset // limit < max_pages:
            postings, total = wd_list_jobs(host, company_id, tenant, offset, limit)
            if not postings:
                break
            if total > 0:
                known_total = total  # Only trust non-zero totals (wd5 bug: returns 0 on page 2+)
            log(f"  API offset={offset}: {len(postings)} postings (total={total})")
            for p in postings:
                if is_ontario(p.get("locationsText", ""), p.get("externalPath", "")):
                    ontario_jobs.append(p)
            offset += limit
            if known_total > 0 and offset >= known_total:
                break
            time.sleep(5)  # brief pause between pages within same company

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

            # Resolve display company name: HTML JSON-LD > tenant-derived name
            resolved_company = extract_company_from_html(text) or company_name
            if resolved_company != company_name:
                log(f"    → company resolved: {company_name!r} → {resolved_company!r}")

            # Parse posted date — Workday returns "Posted 30+ Days Ago", "Posted Today", or ISO date
            posted = TODAY
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', posted_on or "")
            if date_match:
                posted = date_match.group(1)

            job = {
                "role":       title,
                "company":    resolved_company,
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
