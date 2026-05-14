#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-greenhouse.py
Greenhouse job board scraper.

Strategy:
  1. Seed slugs + Exa discovery → Greenhouse company board slugs
  2. Greenhouse public boards JSON API → all jobs per company (no auth needed)
  3. Salary extracted from job content HTML (regex) with double-unescape
  4. Ontario filter: location field OR content mentions "Ontario"

Uses Scrapling's Fetcher for TLS-fingerprint-resilient HTTP (same reason
search-workday.py delegates to curl — avoids JA3 bot detection on repeated calls).

Output: JSONL appended to shared raw file (same format as search-workday.py),
with source_platform="greenhouse" tag for delta tracking.

Run: python3 ~/ontario-pay-hub/scripts/search-greenhouse.py
"""

import html as html_mod
import json
import os
import re
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, exa_search, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

from scrapling import Fetcher

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/greenhouse.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.greenhouse.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=60)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)
fetcher = Fetcher()

# ── Seed slugs — known Ontario-present companies on Greenhouse ────────────────
# Each entry is (slug, optional_display_name_override)
# display_name=None means use company_name from the API response
SEED_SLUGS = [
    ("stackadapt", None),          # Toronto, adtech
    ("tealbook", None),            # Toronto, procurement
    ("achievers", None),           # Toronto, HR
    ("verafin", None),             # St. John's / Toronto, fintech
    ("d2l", None),                 # Kitchener, edtech
    ("tulip", None),               # Toronto, retail tech
    ("coinsquare", None),          # Toronto, crypto
    ("properly", None),            # Toronto, real estate
    ("inkblot", None),             # Ottawa, mental health
    ("pivotal", None),             # Toronto, consulting
    ("cossette", None),            # Toronto, marketing
    ("beanfield", None),           # Toronto, ISP
    ("konrad", None),              # Toronto, tech consulting
    ("clearco", None),             # Toronto, fintech
    ("nudge", None),               # Toronto, employee comms
    ("ritual", None),              # Toronto, food tech
    ("flipp", None),               # Toronto, retail tech
    ("loblaw", None),              # Toronto, retail
    ("rogerscommunications", None),# Toronto, telecom
    ("scotiabank", None),          # Toronto, bank
    ("softchoice", None),          # Toronto, IT
    ("miovision", None),           # Kitchener, smart cities
    ("intellicheck", None),        # Ontario
    ("messagebird", None),         # Toronto, comms
    ("faire", None),               # Toronto office, wholesale
    ("tailscale", None),           # Toronto, networking
    ("weaveworks", None),          # Toronto, cloud
    ("nuvei", None),               # Toronto, payments
    ("opentext", None),            # Waterloo, enterprise software
    ("ptc", None),                 # Ontario, industrial tech
    ("dayforce", None),            # Toronto, HCM
    # ── High-yield additions discovered 2026-03-19 ────────────────────────────
    ("databricks", None),          # US tech, large Canada presence — 10 Ontario+salary
    ("twilio", None),              # US tech, Toronto office — 12 Ontario+salary
    ("brainstation", None),        # Toronto, tech education — 8 Ontario+salary
    ("hootsuite", None),           # Vancouver/Toronto — 22 Ontario+salary
    ("samsara", None),             # US tech, Canada remote — 63 Ontario+salary
    ("affirm", None),              # US fintech, Canada remote — 53 Ontario+salary
    ("brex", None),                # US fintech, Canada remote — 31 Ontario+salary
    ("marqeta", None),             # US fintech, Toronto office — 9 Ontario+salary
    ("lyft", None),                # US tech, Canada remote — 54 Ontario+salary
    ("instacart", None),           # US tech, Toronto/Canada — 44 Ontario+salary
    ("mercury", None),             # US fintech, Canada remote — 29 Ontario+salary
    ("gusto", None),               # US HR-tech, Canada remote — 16 Ontario+salary
    ("lattice", None),             # US HR-tech, Canada remote — 9 Ontario+salary
    ("carta", None),               # US fintech, Toronto office — 7 Ontario+salary
    ("fivetran", None),            # US data, Canada remote — 6 Ontario+salary
    ("robinhood", None),           # US fintech, Canada remote — 19 Ontario+salary
    ("gitlab", None),              # Remote-first, strong Canada presence — 29 Ontario+salary
    ("dropbox", None),             # US tech, Toronto office — 40 Ontario+salary
    ("okta", None),                # US identity, Canada remote — 45 Ontario+salary
    ("elastic", None),             # US search/analytics, Canada — 30 Ontario+salary
    ("mongodb", None),             # US database, Toronto office — 24 Ontario+salary
    ("airbnb", None),              # US tech, Canada remote — 28 Ontario+salary
    # ── Additional slugs discovered 2026-03-19 ───────────────────────────────────
    ("datadog", None),             # US monitoring, Canada remote — 2 Ontario+salary
    ("pagerduty", None),           # US ops, Canada remote — 8 Ontario+salary
    ("benevity", None),            # Calgary HQ, Ontario presence — 13 Ontario+salary
    ("amplitude", None),           # US analytics, Canada remote — 2 Ontario+salary
    ("braze", None),               # US marketing tech, Toronto office — 4 Ontario+salary
    ("scaleai", None),             # US AI data, Canada remote — 1 Ontario+salary
    ("tenstorrent", None),         # Toronto, AI hardware — 1 Ontario+salary
    ("lightspeedhq", None),        # Toronto/Ottawa, commerce platform — 37 Ontario+salary

    ("grammarly", None),          # auto-discovered 2026-03-23 — 7 Ontario+salary
    ("kensingtontours", None),          # auto-discovered 2026-03-23 — 3 Ontario+salary

    ("7shifts", None),          # auto-discovered 2026-03-31 — 10 Ontario+salary
    ("toast", None),          # auto-discovered 2026-03-31 — 6 Ontario+salary
    ("opentable", None),          # auto-discovered 2026-03-31 — 3 Ontario+salary

    ("hellofresh", None),          # auto-discovered 2026-04-21 — 39 Ontario+salary
    ("afresh", None),          # auto-discovered 2026-04-21 — 6 Ontario+salary
    ("lithic", None),          # auto-discovered 2026-04-21 — 5 Ontario+salary
    ("cerebrassystems", None),          # auto-discovered 2026-04-21 — 3 Ontario+salary

    ("constantcontact", None),          # auto-discovered 2026-04-22 — 9 Ontario+salary
    ("range", None),          # auto-discovered 2026-04-22 — 7 Ontario+salary
    ("hillel", None),          # auto-discovered 2026-04-22 — 6 Ontario+salary

    ("cyclicmaterialsinc", None),          # auto-discovered 2026-04-23 — 15 Ontario+salary
    ("fanduel", None),          # auto-discovered 2026-04-23 — 10 Ontario+salary
    ("life360", None),          # auto-discovered 2026-04-23 — 7 Ontario+salary
    ("aottechnologies", None),          # auto-discovered 2026-04-23 — 3 Ontario+salary

    ("bergindustrialservice", None),          # auto-discovered 2026-04-27 — 3 Ontario+salary
    ("localcoin", None),          # auto-discovered 2026-04-27 — 3 Ontario+salary
]

# Exa queries to discover additional Greenhouse slugs posting Ontario jobs
DISCOVERY_QUERIES = [
    'site:job-boards.greenhouse.io Ontario Canada salary 2026',
    'site:job-boards.greenhouse.io Toronto engineer analyst manager salary',
    'site:boards.greenhouse.io Ontario Canada "CAD" 2026',
    'site:job-boards.greenhouse.io Toronto OR Ottawa OR Waterloo salary range 2026',
    'site:boards.greenhouse.io Canada Ontario "$" annual salary 2026',
    'site:job-boards.greenhouse.io "Toronto" developer engineer analyst director 2026',
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
    "canada",   # captures "Remote Canada" roles (consistent with search-lever.py)
]

# Explicit non-Ontario province indicators — hard-exclude before any content check.
# Prevents BC/AB/QC-located jobs from passing is_ontario() when their content
# mentions Ontario salary ranges (common in multi-province job postings).
_NON_ONTARIO_LOC_TERMS = [
    "british columbia", ", bc", "bc,", "vancouver", "victoria", "burnaby",
    "surrey", "kelowna", "abbotsford", "coquitlam",
    "alberta", ", ab", "ab,", "calgary", "edmonton",
    "quebec", "québec", ", qc", "qc,", "montréal", "montreal",
    "nova scotia", ", ns", "new brunswick", ", nb",
    "manitoba", ", mb", "winnipeg",
    "saskatchewan", ", sk", "regina", "saskatoon",
    "newfoundland", "prince edward island",
]

# Same salary patterns as search-workday.py
SALARY_RE = [
    # "$86,100 CAD - $136,100 CAD" or "C$90,000 - C$105,000" or "CAD $96,000 - CAD $120,000"
    # (?:[A-Z]{1,3}\s*)? handles single-letter (C$) and 3-letter (CAD, USD) currency prefixes
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000"
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
]

GH_SLUG_RE = re.compile(
    r'https?://(?:job-boards|boards)\.greenhouse\.io/([a-zA-Z0-9_-]+)',
    re.IGNORECASE,
)
_SKIP_SLUGS = {'jobs', 'search', 'home', 'embed', 'job_app'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def discover_slugs(seed_slugs):
    """Use Exa to find additional Greenhouse company slugs."""
    known = {slug for slug, _ in seed_slugs}
    discovered = set()

    for i, query in enumerate(DISCOVERY_QUERIES, 1):
        log(f"  Discovery Exa [{i}/{len(DISCOVERY_QUERIES)}]: {query[:60]}...")
        resp = exa_search(query, num_results=15, start_date=LOOKBACK_DATE, log=log)
        if not resp:
            continue
        new = 0
        for r in resp.get("results", []):
            m = GH_SLUG_RE.search(r.get("url", ""))
            if not m:
                continue
            slug = m.group(1).lower()
            if slug in _SKIP_SLUGS or slug in known or len(slug) < 3:
                continue
            discovered.add(slug)
            new += 1
        log(f"    → {new} new slugs")
        time.sleep(1.5)

    return discovered


def fetch_company_jobs(slug):
    """Fetch all jobs from Greenhouse boards API for a given slug.

    Returns (company_name, jobs_list) or (slug_title, []) on failure.
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    fallback_name = slug.replace("-", " ").replace("_", " ").title()
    try:
        page = fetcher.get(url, timeout=25)
        if page.status != 200:
            return fallback_name, []
        d = page.json()
        jobs = d.get("jobs", [])
        # Greenhouse API includes company_name in each job dict
        company_name = jobs[0].get("company_name") if jobs else fallback_name
        return company_name or fallback_name, jobs
    except Exception as e:
        log(f"  API error ({slug}): {e}")
        return fallback_name, []


def is_ontario(location_str, content_text=""):
    """True if location field or content text indicates Ontario."""
    loc = (location_str or "").lower()
    # Hard exclusion: explicit non-Ontario province overrides everything, including
    # content-text checks. Prevents BC/QC jobs from passing when their description
    # mentions Ontario salary ranges.
    if any(t in loc for t in _NON_ONTARIO_LOC_TERMS):
        return False
    if any(t in loc for t in ONTARIO_TERMS):
        return True
    # Some companies list salary as "Ontario Residents Only $X–$Y" with a generic
    # location like "Canada" or "Remote" — treat as Ontario if content says so.
    if "ontario" in (content_text or "").lower():
        return True
    return False


def parse_location(location_str):
    city_map = {
        "toronto": "Toronto, ON", "ottawa": "Ottawa, ON",
        "waterloo": "Waterloo, ON", "mississauga": "Mississauga, ON",
        "hamilton": "Hamilton, ON", "london": "London, ON",
        "brampton": "Brampton, ON", "markham": "Markham, ON",
        "vaughan": "Vaughan, ON", "oakville": "Oakville, ON",
        "kitchener": "Kitchener, ON", "windsor": "Windsor, ON",
        "richmond hill": "Richmond Hill, ON",
    }
    loc = (location_str or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def extract_content_text(raw_content):
    """Decode double-HTML-escaped content from Greenhouse API.

    Greenhouse API returns HTML-entity-encoded HTML (outer escape), where the
    HTML itself may contain entities like &mdash; (inner escape).
    Process: outer unescape → strip tags → inner unescape → normalize whitespace.
    """
    if not raw_content:
        return ""
    text = html_mod.unescape(raw_content)         # &lt;p&gt; → <p>
    text = re.sub(r'<[^>]+>', ' ', text)           # strip HTML tags
    text = html_mod.unescape(text)                 # &mdash; → —, &amp; → &
    return re.sub(r'\s+', ' ', text).strip()


def extract_salary(content_text):
    """Extract (min, max) annual CAD salary from plain text. Returns None if not found."""
    for pat in SALARY_RE:
        m = pat.search(content_text)
        if m:
            try:
                raw_min = m.group(1).replace(",", "")
                raw_max = m.group(2).replace(",", "")
                if "k" in m.group(0).lower():
                    vmin = int(float(raw_min) * 1000)
                    vmax = int(float(raw_max) * 1000)
                else:
                    vmin = int(float(raw_min))
                    vmax = int(float(raw_max))
                if 25_000 <= vmin <= 700_000 and vmin < vmax:
                    return vmin, vmax
            except (ValueError, IndexError):
                continue
    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Greenhouse scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    # Build slug list: seeds + Exa discovery
    log(f"Running Exa discovery ({len(DISCOVERY_QUERIES)} queries)...")
    extra_slugs = discover_slugs(SEED_SLUGS)
    log(f"  {len(SEED_SLUGS)} seed + {len(extra_slugs)} discovered = "
        f"{len(SEED_SLUGS) + len(extra_slugs)} total slugs")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    total_found = 0
    api_failures = 0
    discovered_slug_yield = {}  # slug → jobs-with-salary count (discovered only)

    # Process seeds first (known-good), then discovered
    all_slug_entries = list(SEED_SLUGS) + [(s, None) for s in sorted(extra_slugs)]

    for slug, display_name_override in all_slug_entries:
        company_name, jobs = fetch_company_jobs(slug)
        if display_name_override:
            company_name = display_name_override

        if not jobs:
            log(f"── {slug}: no jobs or API error")
            api_failures += 1
            time.sleep(1)
            continue

        log(f"\n── {company_name} ({slug}): {len(jobs)} total jobs ──")
        ontario_count = 0
        found_this_company = 0

        for job in jobs:
            loc_name = (job.get("location") or {}).get("name", "")
            content_text = extract_content_text(job.get("content", ""))

            if not is_ontario(loc_name, content_text):
                continue
            ontario_count += 1

            title = (job.get("title") or "").strip()
            if not title:
                continue

            # Pre-filter: skip already-known jobs before salary extraction
            job_company = job.get("company_name") or company_name
            key = f"{title.lower()}|{job_company.lower()}"
            if key in seen_keys:
                continue

            salary = extract_salary(content_text)
            if not salary:
                log(f"  [{title[:50]}] → no salary")
                continue

            vmin, vmax = salary
            abs_url = job.get("absolute_url", "")

            # Parse date from updated_at (ISO 8601)
            posted = TODAY
            date_m = re.search(r'(\d{4}-\d{2}-\d{2})', job.get("updated_at") or "")
            if date_m:
                posted = date_m.group(1)

            job_out = {
                "role":            title,
                "company":         job_company,
                "min":             vmin,
                "max":             vmax,
                "location":        parse_location(loc_name),
                "source_url":      abs_url,
                "posted":          posted,
                "source_platform": "greenhouse",
            }

            write_job(OUTPUT_FILE, job_out)
            seen_keys.add(key)
            total_found += 1
            found_this_company += 1
            log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{loc_name}]")

        log(f"  Ontario: {ontario_count} | New w/ salary: {found_this_company}")
        if slug in extra_slugs:
            discovered_slug_yield[slug] = found_this_company
        time.sleep(3)  # polite pause between companies

    log(
        f"\n=== Greenhouse scraper complete: {total_found} new jobs written "
        f"(api_failures={api_failures}) ==="
    )

    # ── Auto-inject high-yield discovered slugs into SEED_SLUGS ──────────────
    seed_set = {slug for slug, _ in SEED_SLUGS}
    newly_qualified = {
        slug: count
        for slug, count in discovered_slug_yield.items()
        if slug not in seed_set and count >= 3
    }
    if newly_qualified:
        log(f"\nAuto-injecting {len(newly_qualified)} high-yield discovered slug(s) into SEED_SLUGS:")
        script_path = os.path.abspath(__file__)
        try:
            source = open(script_path).read()
            new_lines = []
            for slug, count in sorted(newly_qualified.items(), key=lambda x: -x[1]):
                # Skip if slug already present anywhere in file (idempotency guard)
                if f'("{slug}"' in source:
                    log(f"  skip {slug} — already in file")
                    continue
                log(f"  + {slug} ({count} Ontario+salary jobs)")
                new_lines.append(
                    f'    ("{slug}", None),          # auto-discovered {TODAY} — {count} Ontario+salary'
                )
            if new_lines:
                insert_block = "\n".join(new_lines)
                marker = "]\n\n# Exa queries to discover"
                if marker in source:
                    source = source.replace(
                        marker,
                        f"\n{insert_block}\n{marker}"
                    )
                    open(script_path, "w").write(source)
                    log(f"  Persisted {len(new_lines)} slug(s) to SEED_SLUGS in script file")
                else:
                    log("  Could not find SEED_SLUGS end marker — skipping persist")
        except Exception as e:
            log(f"  Auto-inject error: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
