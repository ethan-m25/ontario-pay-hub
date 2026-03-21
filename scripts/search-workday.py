#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-workday.py
Workday API scraper — NO JavaScript rendering required.

Strategy:
  1. Tenant discovery — Exa search finds *.myworkdayjobs.com job URLs → extract all employers
  2. CXS JSON API   — paginate all jobs per employer, filter Ontario (fast, no JS)
  3. HTML job page  — salary in raw HTML (meta content attrs); pure regex, no LLM

Seed tenants (37 known) + dynamic discovery via Exa = covers ALL Workday employers
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
import urllib.parse
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
    ("rbc.wd3.myworkdayjobs.com",           "rbc",        "RBCGLOBAL1",           "RBC"),
    ("td.wd3.myworkdayjobs.com",            "td",         "TD_Bank_Careers",      "TD Bank"),
    ("bmo.wd3.myworkdayjobs.com",           "bmo",        "External",             "BMO"),
    ("cibc.wd3.myworkdayjobs.com",          "cibc",       "search",               "CIBC"),
    # Insurance / financial services
    ("manulife.wd3.myworkdayjobs.com",      "manulife",   "MFCJH_Jobs",           "Manulife"),
    ("sunlife.wd3.myworkdayjobs.com",       "sunlife",    "Experienced-Jobs",     "Sun Life"),
    # Pension funds (Toronto-HQ, large Ontario employers)
    ("omers.wd3.myworkdayjobs.com",         "omers",      "OMERS_External",       "OMERS"),
    ("otppb.wd3.myworkdayjobs.com",         "otppb",      "OntarioTeachers_Careers", "Ontario Teachers' Pension Plan"),
    # Real estate / asset management
    ("brookfield.wd5.myworkdayjobs.com",    "brookfield", "brookfield",           "Brookfield"),
    # Retail
    ("walmart.wd5.myworkdayjobs.com",       "walmart",    "WalmartExternal",      "Walmart Canada"),
    # ── New additions discovered 2026-03-19 ───────────────────────────────────
    # Tech (large Canadian presence)
    ("salesforce.wd12.myworkdayjobs.com",   "salesforce", "External_Career_Site", "Salesforce"),
    ("etsy.wd5.myworkdayjobs.com",          "etsy",       "Etsy_Careers",         "Etsy"),
    # Professional services
    ("bdo.wd3.myworkdayjobs.com",           "bdo",        "BDO",                  "BDO Canada"),
    ("pwc.wd3.myworkdayjobs.com",           "pwc",        "Global_Experienced_Careers", "PwC"),
    # Energy / utilities
    ("tcenergy.wd3.myworkdayjobs.com",      "tcenergy",   "CAREER_SITE_TC",       "TC Energy"),
    # Telecom / media
    ("cogeco.wd3.myworkdayjobs.com",        "cogeco",     "Cogeco_Careers",       "Cogeco"),
    # Government / regulated
    ("navcanada.wd10.myworkdayjobs.com",    "navcanada",  "NAV_Careers",          "NAV Canada"),
    # Retail (Canadian)
    ("canadiantirecorporation.wd3.myworkdayjobs.com", "canadiantirecorporation", "Enterprise_External_Careers_Site", "Canadian Tire"),
    ("mywdhr.wd1.myworkdayjobs.com",        "mywdhr",     "HBC_Careers",          "Hudson's Bay Company"),
    # Loblaw (board confirmed as loblaw_careers)
    ("myview.wd3.myworkdayjobs.com",        "myview",     "loblaw_careers",       "Loblaw Companies"),
    # Energy
    ("enbridge.wd3.myworkdayjobs.com",      "enbridge",   "Enbridge_Careers",     "Enbridge"),
    # Restaurant / food service (Toronto HQ — Tim Hortons, Burger King, Popeyes, Firehouse Subs)
    ("rbi.wd3.myworkdayjobs.com",           "rbi",        "RBI_External_Career_Site", "Restaurant Brands International"),
    # Insurance (Toronto HQ — Intact Financial Corporation)
    ("intactfc.wd3.myworkdayjobs.com",      "intactfc",   "IntactFC",             "Intact Financial"),
    # Media / tech (Toronto HQ — posts Ontario salary range)
    ("thomsonreuters.wd5.myworkdayjobs.com","thomsonreuters","External_Career_Site","Thomson Reuters"),
    # ── myworkdaysite.com tenants (path-based company routing, same CXS API) ───
    # Manufacturing / automotive (Toronto-area plants, salary-disclosed roles)
    ("wd3.myworkdaysite.com",               "magna",      "Magna",                "Magna International"),
    # ── Additional tenants discovered 2026-03-19 ──────────────────────────────
    # Pharma (Mississauga HQ — confirmed salary disclosure)
    ("astrazeneca.wd3.myworkdayjobs.com",   "astrazeneca","Careers",              "AstraZeneca"),
    # Utilities (Ottawa electric utility)
    ("hydroottawa.wd3.myworkdayjobs.com",   "hydroottawa","hydro_ottawa_careersite","Hydro Ottawa"),
    # Automotive (GM Canada — Oshawa/Ontario plants)
    ("generalmotors.wd5.myworkdayjobs.com", "generalmotors","Careers_GM",         "General Motors"),
    # Pharma (Mississauga/Toronto — two separate boards)
    ("roche.wd3.myworkdayjobs.com",         "roche",        "roche-ext",          "Roche Canada"),
    ("roche.wd3.myworkdayjobs.com",         "roche",        "ROG-A2O-GENE",       "Roche Canada (Gene Therapies)"),
    # Medical devices (Hamilton/Ontario operations)
    ("stryker.wd1.myworkdayjobs.com",       "stryker",      "StrykerCareers",     "Stryker"),
    # Consumer goods (Belleville/Toronto plants — salary disclosed)
    ("pg.wd5.myworkdayjobs.com",            "pg",           "1000",               "Procter & Gamble"),
    # Pension fund (Toronto HQ — 34 jobs, Toronto-focused)
    ("cppib.wd10.myworkdayjobs.com",        "cppib",        "cppinvestments",     "CPP Investments"),
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
    "salesforce":               "Salesforce",
    "bdo":                      "BDO Canada",
    "pwc":                      "PwC",
    "tcenergy":                 "TC Energy",
    "cogeco":                   "Cogeco",
    "navcanada":                "NAV Canada",
    "canadiantirecorporation":  "Canadian Tire",
    "mywdhr":                   "Hudson's Bay Company",
    "rbi":                      "Restaurant Brands International",
    "lseg":                     "LSEG (London Stock Exchange Group)",
    "capitalone":               "Capital One",
    "lcbo":                     "LCBO",
    "accenture":                "Accenture",
    "magna":                    "Magna International",
    "astrazeneca":              "AstraZeneca",
    "hydroottawa":              "Hydro Ottawa",
    "generalmotors":            "General Motors",
    "roche":                    "Roche Canada",
    "stryker":                  "Stryker",
    "pg":                       "Procter & Gamble",
    "cppib":                    "CPP Investments",
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
    # Big tech Canada offices
    'site:myworkdayjobs.com "Salesforce" OR "Adobe" OR "ServiceNow" OR "Workday" Toronto Ontario',
    'site:myworkdayjobs.com "Accenture" OR "Deloitte" OR "KPMG" OR "EY" Toronto Ontario Canada',
    'site:myworkdayjobs.com "Rogers" OR "Bell" OR "Telus" OR "Cogeco" Ontario Canada salary',
    'site:myworkdayjobs.com "LSEG" OR "Capital One" OR "RBI" OR "Restaurant Brands" Toronto',
    'site:myworkdayjobs.com "LCBO" OR "OLG" OR "Metrolinx" OR "GO Transit" Ontario salary',
    'site:myworkdayjobs.com "Etsy" OR "Shopify" OR "Hootsuite" OR "Lightspeed" Canada Ontario',
    # myworkdaysite.com — separate Workday hosting domain (Magna and others)
    'site:myworkdaysite.com Ontario Canada salary 2026',
    'site:myworkdaysite.com Toronto OR Waterloo OR Ottawa engineer analyst manager salary',
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
    # "$86,100 CAD - $136,100 CAD" or "C$90,000 - C$105,000" or "CAD $96,000 - CAD $120,000"
    # (?:[A-Z]{1,3}\s*)? handles single-letter (C$, U$) and 3-letter (CAD, USD) currency prefixes
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K" or "C$86K – C$136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000" or "Ontario Salary: 94,500 - $130,000" (optional $ before second)
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
]


# ── Tenant discovery via Exa ──────────────────────────────────────────────────
# Workday URL formats:
#   myworkdayjobs.com: https://{company}.wd3.myworkdayjobs.com/en-US/{tenant}/job/...
#   myworkdaysite.com: https://wd3.myworkdaysite.com/en-US/recruiting/{company}/{tenant}/details/...
_WD_URL_RE = re.compile(
    r'https?://([a-z0-9][a-z0-9-]*)\.wd\d+\.myworkdayjobs\.com'
    r'(?:/[a-z]{2}-[A-Z]{2})?/([^/?#]+)',
    re.IGNORECASE,
)
_WD_SITE_URL_RE = re.compile(
    r'https?://wd\d+\.myworkdaysite\.com'
    r'(?:/[a-z]{2}-[A-Z]{2})?/recruiting/([a-z0-9][a-z0-9-]*)/([^/?#]+)',
    re.IGNORECASE,
)
_SKIP_TENANTS = {'job', 'jobs', 'search', 'en', 'en-us', 'en-gb', 'fr', 'fr-ca', 'details', 'recruiting'}


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
    """Use Exa to find myworkdayjobs.com URLs and extract tenants plus direct job URLs."""
    discovered = {}  # host → (host, company_id, tenant, display_name)
    candidate_urls = {}  # direct Workday job URL → metadata

    for i, query in enumerate(DISCOVERY_QUERIES, 1):
        log(f"  Discovery Exa [{i}/{len(DISCOVERY_QUERIES)}]: {query[:60]}...")
        resp = exa_search(query, num_results=15, start_date=LOOKBACK_DATE, log=log)
        if not resp:
            continue
        results = resp.get("results", [])
        new = 0
        for r in results:
            url = (r.get("url") or "").strip()
            parsed = parse_workday_tenant(url)
            if parsed and parsed[0] not in discovered:
                host, company_id, tenant = parsed
                discovered[host] = (host, company_id, tenant, format_tenant_name(company_id, tenant))
                new += 1
            # Also match myworkdaysite.com URLs — use company+tenant as key
            m_site = _WD_SITE_URL_RE.search(url)
            if m_site:
                company_id = m_site.group(1).lower()
                tenant = m_site.group(2)
                host_m = re.match(r'https?://(wd\d+\.myworkdaysite\.com)', url, re.IGNORECASE)
                if host_m:
                    host = host_m.group(1).lower()
                    site_key = f"{host}/{company_id}"
                    if site_key not in discovered and tenant.lower() not in _SKIP_TENANTS:
                        discovered[site_key] = (host, company_id, tenant, format_tenant_name(company_id, tenant))
                        new += 1
            job_url = parse_workday_job_url(url)
            if job_url:
                host, company_id, tenant, external_path = job_url
                candidate_urls[url] = {
                    "host": host,
                    "company_id": company_id,
                    "tenant": tenant,
                    "external_path": external_path,
                    "fallback_company": format_tenant_name(company_id, tenant),
                }
        log(f"    → {len(results)} results, {new} new tenants")
        time.sleep(1.5)

    return list(discovered.values()), candidate_urls


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

    myworkdaysite.com uses a path-based tenant: externalPath already contains
    the full /recruiting/{company}/{tenant}/details/{id} segment, so the tenant
    must NOT be inserted separately.
    """
    if "myworkdaysite.com" in host:
        url = f"https://{host}/en-US{external_path}"
    else:
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


def fetch_job_html_from_url(url):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "text/html,application/xhtml+xml,*/*;q=0.9")
    req.add_header("Accept-Language", "en-CA,en;q=0.9")
    try:
        with urllib.request.urlopen(req, timeout=18) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def parse_workday_job_url(url):
    """Extract (host, company_id, tenant, external_path) from a direct Workday job URL.

    Handles both myworkdayjobs.com (subdomain-per-company) and myworkdaysite.com
    (path-based company routing at wd3.myworkdaysite.com/en-US/recruiting/{company}/{tenant}/...).
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None
    netloc = (parsed.netloc or "").lower()

    # myworkdaysite.com: path-based tenant
    if "myworkdaysite.com" in netloc:
        m = _WD_SITE_URL_RE.search(url)
        if not m:
            return None
        company_id = m.group(1).lower()
        tenant = m.group(2)
        host_m = re.match(r'https?://(wd\d+\.myworkdaysite\.com)', url, re.IGNORECASE)
        if not host_m:
            return None
        host = host_m.group(1).lower()
        # external_path = everything from /recruiting/ onward (after /en-XX prefix)
        path = parsed.path
        idx = path.lower().find("/recruiting/")
        if idx < 0:
            return None
        external_path = path[idx:]
        return host, company_id, tenant, external_path

    if "myworkdayjobs.com" not in netloc:
        return None

    host = (parsed.netloc or "").lower()
    company_id = host.split(".")[0]
    parts = [p for p in (parsed.path or "").split("/") if p]
    if not parts:
        return None

    tenant_idx = 0
    if re.fullmatch(r"[a-z]{2}-[A-Z]{2}", parts[0]):
        tenant_idx = 1
    if len(parts) <= tenant_idx + 1:
        return None
    tenant = parts[tenant_idx]
    if parts[tenant_idx + 1].lower() != "job":
        return None
    external_path = "/" + "/".join(parts[tenant_idx + 1 :])
    return host, company_id, tenant, external_path


# ── Company name normalisation ─────────────────────────────────────────────────
# Workday often returns internal legal-entity names rather than consumer brands.
# Two problems we fix here:
#   1. Numeric prefix  e.g. "2105 The TDL Group Corp./ Groupe TDL Corporation"
#   2. Legal-entity → brand  e.g. "TDL Group" → "Tim Hortons"

_NUMERIC_PREFIX_RE = re.compile(r'^\d{3,5}\s+')

# Ordered list: (pattern, canonical_brand).  First match wins.
_BRAND_MAP = [
    (re.compile(r'tdl\s+group|tim horton', re.IGNORECASE), "Tim Hortons"),
    (re.compile(r'\bplk\b|popeyes', re.IGNORECASE),        "Popeyes"),
    (re.compile(r'firehouse subs', re.IGNORECASE),          "Firehouse Subs"),
    (re.compile(r'burger king', re.IGNORECASE),             "Burger King"),
    (re.compile(r'salesforce\.com', re.IGNORECASE),         "Salesforce"),
]


def normalize_company_name(name):
    """Strip Workday numeric prefix and map legal entities to consumer brands."""
    if not name:
        return name
    name = _NUMERIC_PREFIX_RE.sub('', name).strip()
    # Unescape HTML entities that sometimes appear in JSON-LD blocks
    import html as _html
    name = _html.unescape(name)
    for pattern, brand in _BRAND_MAP:
        if pattern.search(name):
            return brand
    return name


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
                # Skip Workday internal "Company N - Legal Name" strings; the
                # caller already has the correct display name from SEED_TENANTS.
                if re.match(r'^Company\s+\d+\b', name):
                    continue
                return normalize_company_name(name)
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
            return normalize_company_name(name)

    return None


def extract_title_from_html(text):
    if not text:
        return None

    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text, re.DOTALL | re.IGNORECASE
    )
    for block in ld_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                data = data[0]
            name = (data.get("title") or data.get("name") or "").strip()
            if name:
                return name
        except Exception:
            continue

    for pattern in (
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
        r'<title>(.*?)</title>',
    ):
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            title = re.sub(r'\s+', ' ', m.group(1)).strip()
            title = re.sub(r'\s*[-|]\s*Workday.*$', '', title, flags=re.IGNORECASE)
            if title:
                return title
    return None


def extract_location_from_html(text, external_path=""):
    if not text:
        return parse_location("", external_path)

    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text, re.DOTALL | re.IGNORECASE
    )
    for block in ld_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                data = data[0]
            loc = data.get("jobLocation")
            if isinstance(loc, list):
                loc = loc[0]
            addr = (loc or {}).get("address", {})
            locality = (addr.get("addressLocality") or "").strip()
            region = (addr.get("addressRegion") or "").strip()
            if locality:
                if region.upper() in {"ON", "ONTARIO"}:
                    return f"{locality}, ON"
                return locality
        except Exception:
            continue
    return parse_location("", external_path)


def extract_posted_from_html(text):
    if not text:
        return TODAY
    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text, re.DOTALL | re.IGNORECASE
    )
    for block in ld_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                data = data[0]
            posted = str(data.get("datePosted") or "").strip()
            m = re.search(r'(\d{4}-\d{2}-\d{2})', posted)
            if m:
                return m.group(1)
        except Exception:
            continue
    return TODAY


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
    discovered, candidate_urls = discover_tenants()

    # Merge: seed takes priority (has proper display names + verified tenant IDs)
    seed_hosts = {t[0] for t in SEED_TENANTS}
    extra = [t for t in discovered if t[0] not in seed_hosts]
    all_tenants = SEED_TENANTS + extra
    log(f"Total tenants: {len(all_tenants)} ({len(SEED_TENANTS)} seed + {len(extra)} discovered)")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    total_found = 0
    api_failures = 0
    failed_hosts = set()

    for host, company_id, tenant, company_name in all_tenants:
        log(f"\n── {company_name} ({host}) ──")

        # Paginate through all jobs, collect Ontario ones
        ontario_jobs = []
        offset = 0
        limit = 10     # Workday blocks limit >= 25 (anti-scraping); 10 is safe
        max_pages = 10 # covers 100 most recent jobs per company

        # wd5 tenants (Brookfield, Walmart) return total=0 on offset>0 despite having more jobs.
        # Track the first valid total and use it for pagination decisions; if a page returns
        # total=0 we still continue until we hit max_pages or get an empty postings list.
        known_total = 0
        while offset // limit < max_pages:
            postings, total = wd_list_jobs(host, company_id, tenant, offset, limit)
            if not postings:
                if offset == 0:
                    api_failures += 1
                    failed_hosts.add(host)
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
            time.sleep(2)  # brief pause between pages within same company

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

    direct_fallback_found = 0
    fallback_candidates = [
        (url, meta)
        for url, meta in candidate_urls.items()
        if meta["host"] in failed_hosts
    ][:30]
    if fallback_candidates:
        log(
            f"\n── Direct Workday URL fallback ({len(fallback_candidates)} candidates "
            f"from {len(failed_hosts)} failed hosts) ──"
        )
    for index, (url, meta) in enumerate(fallback_candidates, 1):
        host = meta["host"]
        company_id = meta["company_id"]
        tenant = meta["tenant"]
        external_path = meta["external_path"]
        fallback_company = meta["fallback_company"]
        log(f"  [fallback {index}/{len(fallback_candidates)}] {host}")
        html = fetch_job_html_from_url(url)
        if not html:
            continue
        salary = extract_salary(html)
        if not salary:
            continue

        title = extract_title_from_html(html)
        if not title:
            continue

        company_name = extract_company_from_html(html) or fallback_company or format_tenant_name(company_id, tenant)
        key = f"{title.lower()}|{company_name.lower()}"
        if key in seen_keys:
            continue

        location = extract_location_from_html(html, external_path)
        if "on" not in location.lower() and not is_ontario(location, external_path):
            continue

        val_min, val_max = salary
        job = {
            "role": title,
            "company": company_name,
            "min": val_min,
            "max": val_max,
            "location": location,
            "source_url": url,
            "posted": extract_posted_from_html(html),
        }
        write_job(OUTPUT_FILE, job)
        seen_keys.add(key)
        total_found += 1
        direct_fallback_found += 1
        log(f"  → DIRECT FOUND: {title[:50]} @ {company_name} ${val_min:,}–${val_max:,} [{location}]")
        time.sleep(0.4)

    log(
        f"\n=== Workday scraper complete: {total_found} new jobs written to {OUTPUT_FILE} "
        f"(api_failures={api_failures}, direct_fallback={direct_fallback_found}) ==="
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
