#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-lever.py
Lever job board scraper.

Strategy:
  1. Seed slugs + Exa discovery → Lever company board slugs
  2. Lever public postings JSON API → all jobs per company (no auth needed)
  3. Salary extracted from structured salaryRange field (CAD, annual only),
     with fallback to salaryDescriptionPlain and descriptionPlain regex

Lever API endpoint: https://api.lever.co/v0/postings/{slug}?mode=json
Salary field: salaryRange = {min, max, currency, interval}

Uses Scrapling's Fetcher for TLS-fingerprint-resilient HTTP.

Output: JSONL appended to shared raw file, with source_platform="lever".

Run: python3 ~/ontario-pay-hub/scripts/search-lever.py
"""

import html as html_mod
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, exa_search, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

from scrapling import Fetcher

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/lever.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.lever.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=60)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)
fetcher = Fetcher()

# ── Seed slugs ────────────────────────────────────────────────────────────────
SEED_SLUGS = [
    # Known from existing data
    "achievers",
    "arcteryx.com",
    "benchsci",
    "caseware",
    "fullscript",
    "infrastructureontario",
    "owner",
    "pointclickcare",
    "policyme",
    "teleport",
    "waabi",
    "wealthsimple",
    # Additional known Ontario Lever companies
    "nudge",
    "kira",
    "nuvei",
    "properly",
    "dialog",
    "sampler",
    "tulip-retail",
    "tealbook",
    "inkblot-therapy",
    "mappedin",
    "validere",
    "dialogue",
    "distributedfinance",
    "clearbanc",
    "relay",
    "cohere",
    "novafinancial",
    "intellicheck",
    "auvik",
    "mytwentytwenty",
    "betterdoctor",
    "momentive",
    "assent",
    "tenstorrent",
    "certn",
    "highspot",
    "miovision",
    "martello",
    "venngage",
    "aislelabs",
    "portaone",
    "iversoft",
    "navigator",

    "lvs1",  # auto-discovered 2026-03-28 — 10 Ontario+salary
    "zensurance",  # auto-discovered 2026-03-28 — 8 Ontario+salary

    "metergysolutions",  # auto-discovered 2026-04-08 — 11 Ontario+salary
    "riocan",  # auto-discovered 2026-04-08 — 7 Ontario+salary

    "11855760-canada-inc",  # auto-discovered 2026-04-21 — 4 Ontario+salary
]

DISCOVERY_QUERIES = [
    'site:jobs.lever.co Ontario Canada salary 2026',
    'site:jobs.lever.co Toronto engineer analyst manager salary',
    'site:jobs.lever.co "Ontario" OR "Toronto" "CAD" 2026',
    'site:jobs.lever.co Toronto OR Ottawa OR Waterloo salary range 2026',
    'site:jobs.lever.co Canada Ontario salary annual 2026',
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
    "canada",  # Lever often just says "Canada" for remote roles
]

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

SALARY_RE = [
    # "$86,100 CAD - $136,100 CAD" or "C$90,000 - C$105,000" or "CAD $96,000 - CAD $120,000"
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
]

LEVER_SLUG_RE = re.compile(
    r'https?://jobs\.lever\.co/([a-zA-Z0-9._-]+)',
    re.IGNORECASE,
)
_SKIP_SLUGS = {'jobs', 'search', 'home'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def discover_slugs(seed_slugs):
    known = set(seed_slugs)
    discovered = set()
    for i, query in enumerate(DISCOVERY_QUERIES, 1):
        log(f"  Discovery Exa [{i}/{len(DISCOVERY_QUERIES)}]: {query[:60]}...")
        resp = exa_search(query, num_results=15, start_date=LOOKBACK_DATE, log=log)
        if not resp:
            continue
        new = 0
        for r in resp.get("results", []):
            m = LEVER_SLUG_RE.search(r.get("url", ""))
            if not m:
                continue
            slug = m.group(1).lower()
            if slug in _SKIP_SLUGS or slug in known or len(slug) < 2:
                continue
            discovered.add(slug)
            new += 1
        log(f"    → {new} new slugs")
        time.sleep(1.5)
    return discovered


def fetch_company_jobs(slug):
    """Fetch all postings from Lever API. Returns list of posting dicts."""
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        page = fetcher.get(url, timeout=20)
        if page.status != 200:
            return []
        return page.json() or []
    except Exception as e:
        log(f"  API error ({slug}): {e}")
        return []


def is_ontario(location_str, desc_text=""):
    loc = (location_str or "").lower()
    if any(t in loc for t in _NON_ONTARIO_LOC_TERMS):
        return False
    if any(t in loc for t in ONTARIO_TERMS):
        return True
    if "ontario" in (desc_text or "").lower():
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


def extract_salary_from_range(sal_range):
    """Extract (min, max) from Lever's structured salaryRange field.

    Returns None if not annual CAD salary.
    Lever format: {'min': 92700, 'max': 103000, 'currency': 'CAD', 'interval': 'per-year-salary'}
    """
    if not sal_range:
        return None
    if sal_range.get("currency", "").upper() != "CAD":
        return None
    if sal_range.get("interval", "") != "per-year-salary":
        return None  # skip hourly rates
    try:
        vmin = int(float(sal_range["min"]))
        vmax = int(float(sal_range["max"]))
        if 25_000 <= vmin <= 700_000 and vmin < vmax:
            return vmin, vmax
    except (KeyError, ValueError, TypeError):
        pass
    return None


def extract_salary_from_text(text):
    """Fallback: extract salary from description text via regex."""
    if not text:
        return None
    clean = html_mod.unescape(re.sub(r'<[^>]+>', ' ', text))
    clean = html_mod.unescape(re.sub(r'\s+', ' ', clean).strip())
    for pat in SALARY_RE:
        m = pat.search(clean)
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

    log("=== Lever scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

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
    all_slugs = list(SEED_SLUGS) + sorted(extra_slugs)

    for slug in all_slugs:
        jobs = fetch_company_jobs(slug)
        if not jobs:
            log(f"── {slug}: no jobs or API error")
            api_failures += 1
            time.sleep(1)
            continue

        # Infer company name from first job's company field or slug
        company_name = slug.replace("-", " ").replace("_", " ").replace(".", " ").title()

        log(f"\n── {company_name} ({slug}): {len(jobs)} postings ──")
        ontario_count = 0
        found_this = 0

        for job in jobs:
            cats = job.get("categories") or {}
            loc_name = cats.get("location", "") or cats.get("allLocations", "")
            if isinstance(loc_name, list):
                loc_name = ", ".join(loc_name)

            desc_plain = job.get("descriptionPlain") or ""
            if not is_ontario(loc_name, desc_plain):
                continue
            ontario_count += 1

            title = (job.get("text") or "").strip()
            if not title:
                continue

            key = f"{title.lower()}|{company_name.lower()}"
            if key in seen_keys:
                continue

            # Salary: structured field first, then text fallback
            salary = extract_salary_from_range(job.get("salaryRange"))
            if not salary:
                sal_desc = job.get("salaryDescriptionPlain") or job.get("salaryDescription") or ""
                salary = extract_salary_from_text(sal_desc) or extract_salary_from_text(desc_plain)

            if not salary:
                log(f"  [{title[:50]}] → no salary")
                continue

            vmin, vmax = salary
            job_id = job.get("id", "")
            abs_url = f"https://jobs.lever.co/{slug}/{job_id}" if job_id else ""

            # createdAt is Unix milliseconds
            posted = TODAY
            created_ms = job.get("createdAt")
            if created_ms:
                try:
                    posted = datetime.fromtimestamp(
                        int(created_ms) / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    pass

            job_out = {
                "role":            title,
                "company":         company_name,
                "min":             vmin,
                "max":             vmax,
                "location":        parse_location(loc_name),
                "source_url":      abs_url,
                "posted":          posted,
                "source_platform": "lever",
            }

            write_job(OUTPUT_FILE, job_out)
            seen_keys.add(key)
            total_found += 1
            found_this += 1
            log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{loc_name}]")

        log(f"  Ontario: {ontario_count} | New w/ salary: {found_this}")
        if slug in extra_slugs:
            discovered_slug_yield[slug] = found_this
        time.sleep(2)

    log(
        f"\n=== Lever scraper complete: {total_found} new jobs written "
        f"(api_failures={api_failures}) ==="
    )

    # ── Auto-inject high-yield discovered slugs into SEED_SLUGS ──────────────
    seed_set = set(SEED_SLUGS)
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
                if f'"{slug}"' in source:
                    log(f"  skip {slug} — already in file")
                    continue
                log(f"  + {slug} ({count} Ontario+salary jobs)")
                new_lines.append(
                    f'    "{slug}",  # auto-discovered {TODAY} — {count} Ontario+salary'
                )
            if new_lines:
                insert_block = "\n".join(new_lines)
                marker = "]\n\nDISCOVERY_QUERIES"
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
