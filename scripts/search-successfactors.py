#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-successfactors.py
SAP SuccessFactors career portal scraper.

Strategy:
  1. Each portal exposes a "sitemal.xml" RSS feed (undocumented SF endpoint)
     that returns ALL job listings with <g:location> fields — no pagination needed.
  2. Filter Ontario jobs by location text from the RSS feed.
  3. Fetch each detail page (static HTML) and extract salary + title.
  4. EY Canada: sitemal.xml is global and unfiltered, so use Exa to find
     Ontario job URLs, then fetch detail pages.

Confirmed salary formats by company:
  - Telus:  "Salary Range: $86,000-$136,000"
  - OPG:    "$1,704.68 - $2,658.86 Per Week"  (converted to annual ×52)
  - EY CA:  "$65,600 to $98,400"
  - Deloitte/Scotiabank: no salary disclosed → will yield 0 results (OK)

Portal URLs (all use SAP SF; sitemal.xml is the RSS discovery endpoint):
  https://{host}/sitemal.xml  → RSS with <g:location> field per item

Run: python3 ~/ontario-pay-hub/scripts/search-successfactors.py
"""

import html as html_mod
import os
import re
import sys
import time
from datetime import date, timedelta

from scrapling import Fetcher

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE, exa_search,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/successfactors.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.successfactors.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)
fetcher = Fetcher()

# ── Seed portals ──────────────────────────────────────────────────────────────
# (host, display_name)
# sitemal.xml returns ALL jobs with <g:location> — filter Ontario client-side.
SEED_PORTALS = [
    # Telecom (confirmed salary: "Salary Range: $86,000-$136,000")
    ("careers.telus.com",   "Telus"),
    # Provincial utility (confirmed salary: "$1,704/week" → ×52 annual)
    ("jobs.opg.com",        "Ontario Power Generation"),
    # Provincial utility (confirmed salary: "$3,952/bi-weekly" or "$57.72/hourly")
    # Uses Google Base RSS at /sitemap.xml instead of sitemal.xml
    ("jobs.hydroone.com",   "Hydro One"),
    # NOTE: Scotiabank (jobs.scotiabank.com) and Deloitte Canada (careers.deloitte.ca)
    # also use SF sitemal.xml and have 800+ / 355 Ontario jobs respectively, but neither
    # discloses salary ranges on job detail pages — scraping yields 0 results (confirmed).
    # They are covered by search-browser.py Exa queries instead.
]

# EY Canada jobs: global sitemal.xml does not filter by country.
# Use Exa to find recent EY Canada job URLs, then fetch detail pages.
# Confirmed salary: "$65,600 to $98,400" format.
EXA_QUERIES_EY = [
    'site:careers.ey.com/ey/job Ontario Canada salary range "$" 2026',
    'site:careers.ey.com/ey/job Toronto OR Ottawa salary range "$" 2026 consultant OR analyst OR manager OR senior',
]

# ── Constants ─────────────────────────────────────────────────────────────────

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
]

_NON_ON_PROVINCES = [
    "british columbia", "alberta", "quebec", "québec",
    "manitoba", "saskatchewan", "nova scotia", "new brunswick",
    "prince edward", "newfoundland", "northwest territories",
    "yukon", "nunavut", "vancouver", "calgary", "edmonton",
    "montreal", "winnipeg", ", bc,", ", ab,", ", qc,",
    # US states
    "new york", "california", "texas", "florida", "washington",
    "illinois", "georgia", "ohio", "arizona", "colorado",
]

SALARY_RE = [
    # "Salary Range: $86,000-$136,000" or "$65,600 to $98,400"
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000"
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
    # OPG: "$1,704.68 - $2,658.86 Per Week" (weekly → flag for conversion)
    re.compile(r'\$\s*([\d,]+(?:\.\d{2})?)\s*[-–—]\s*\$\s*([\d,]+(?:\.\d{2})?)\s*(?:per\s+week|/\s*week)', re.IGNORECASE),
]

_WEEKLY_RE = re.compile(r'per\s+week|/\s*week', re.IGNORECASE)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_html(url):
    """Fetch a URL and return raw HTML string. Returns None on failure."""
    try:
        page = fetcher.get(url, timeout=25)
        if page.status != 200:
            return None
        return page.html_content or ""
    except Exception as e:
        log(f"  Fetch error ({url[:60]}): {e}")
        return None


def html_to_text(raw):
    """Strip HTML tags and return clean plain text."""
    raw = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.DOTALL | re.IGNORECASE)
    raw = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL | re.IGNORECASE)
    text = html_mod.unescape(re.sub(r'<[^>]+>', ' ', raw))
    return re.sub(r'\s+', ' ', text).strip()


def is_ontario(loc_text):
    loc = (loc_text or "").lower()
    if any(p in loc for p in _NON_ON_PROVINCES):
        return False
    return any(t in loc for t in ONTARIO_TERMS)


def parse_location(loc_text):
    city_map = {
        "toronto":      "Toronto, ON",
        "ottawa":       "Ottawa, ON",
        "waterloo":     "Waterloo, ON",
        "mississauga":  "Mississauga, ON",
        "hamilton":     "Hamilton, ON",
        "london":       "London, ON",
        "brampton":     "Brampton, ON",
        "markham":      "Markham, ON",
        "vaughan":      "Vaughan, ON",
        "oakville":     "Oakville, ON",
        "kitchener":    "Kitchener, ON",
        "windsor":      "Windsor, ON",
        "richmond hill": "Richmond Hill, ON",
    }
    loc = (loc_text or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def extract_salary(text):
    """Return (vmin, vmax) annual CAD or None.

    OPG uses weekly pay — multiplied by 52.
    Hydro One uses bi-weekly (×26) or hourly (×2080).
    """
    if not text:
        return None

    num_range = r'\$\s*([\d,]+(?:\.\d{1,2})?)\s*[-–—]\s*\$\s*([\d,]+(?:\.\d{1,2})?)'

    # Bi-weekly → ×26 (Hydro One format: "$3,952.00 - $5,646.00 / Bi-weekly")
    biweekly_pat = re.compile(num_range + r'\s*(?:/|-per-|per\s+)?\s*bi[\s-]?weekly', re.IGNORECASE)
    m = biweekly_pat.search(text)
    if m:
        try:
            vmin = int(float(m.group(1).replace(",", "")) * 26)
            vmax = int(float(m.group(2).replace(",", "")) * 26)
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            pass

    # Hourly → ×2080 (Hydro One format: "$57.72 - $72.49 / hourly")
    hourly_pat = re.compile(num_range + r'\s*(?:/|-per-|per\s+)?\s*hour(?:ly)?', re.IGNORECASE)
    m = hourly_pat.search(text)
    if m:
        try:
            vmin = int(float(m.group(1).replace(",", "")) * 2080)
            vmax = int(float(m.group(2).replace(",", "")) * 2080)
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            pass

    # Weekly → ×52 (OPG format: "$1,704/week")
    weekly_pat = re.compile(num_range + r'\s*(?:per\s+week|/\s*week)', re.IGNORECASE)
    m = weekly_pat.search(text)
    if m:
        try:
            vmin = int(float(m.group(1).replace(",", "")) * 52)
            vmax = int(float(m.group(2).replace(",", "")) * 52)
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            pass

    for pat in SALARY_RE[:-1]:  # skip weekly pattern (already handled)
        m = pat.search(text)
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


def extract_title(raw_html, url):
    """Extract title from <h1> or <title> tag."""
    if raw_html:
        m = re.search(r'<h1[^>]*>(.*?)</h1>', raw_html, re.DOTALL | re.IGNORECASE)
        if m:
            title = html_mod.unescape(re.sub(r'<[^>]+>', '', m.group(1))).strip()
            if len(title) >= 5:
                return title
        m = re.search(r'<title[^>]*>([^<]+)</title>', raw_html, re.IGNORECASE)
        if m:
            title = html_mod.unescape(m.group(1)).strip()
            # Strip trailing " | Company" or " - Company"
            title = re.sub(r'\s*[\|—–-]\s*(?:EY|Deloitte|Telus|Scotiabank|OPG|Ontario Power)\b.*$',
                           '', title, flags=re.IGNORECASE).strip()
            if len(title) >= 5:
                return title
    return None


def extract_posted(text):
    """Extract posting date from page text, fallback to TODAY."""
    m = re.search(r'(20\d{2}-\d{2}-\d{2})', (text or "")[:5000])
    if m:
        return m.group(1)
    return TODAY


# ── sitemal.xml RSS feed scraper ──────────────────────────────────────────────

def get_ontario_urls_from_sitemal(host):
    """Fetch sitemal.xml (or sitemap.xml fallback) and return Ontario job URLs.

    Tries sitemal.xml first (SAP SF standard endpoint); falls back to sitemap.xml
    for portals like Hydro One that use Google Base RSS format.

    Returns list of (url, loc_text) tuples.
    """
    for xml_path in ("sitemal.xml", "sitemap.xml"):
        sitemal_url = f"https://{host}/{xml_path}"
        log(f"  Fetching {xml_path}: {sitemal_url}")
        raw = fetch_html(sitemal_url)
        if raw and "<item>" in raw:
            break
        raw = None

    if not raw:
        log(f"  → sitemap fetch failed for {host}")
        return []

    # sitemal.xml has malformed XML (unclosed <link> tags, bad CDATA) on most SF instances.
    # Regex parsing is the only reliable approach.
    items_raw = re.findall(r'<item>(.*?)</item>', raw, re.DOTALL)
    results = []
    for item_text in items_raw:
        # <link> in SF sitemal.xml often has NO closing </link> tag — match URL greedily
        link_m = re.search(r'<link>\s*(https?://[^\s<]+)', item_text)
        loc_m  = re.search(r'<g:location>(.*?)</g:location>', item_text)
        if not link_m:
            continue
        url = link_m.group(1).strip()
        loc = loc_m.group(1).strip() if loc_m else ""
        if is_ontario(loc):
            results.append((url, loc))

    log(f"  → {len(results)} Ontario jobs found")
    return results


# ── EY Canada via Exa ─────────────────────────────────────────────────────────

def get_ey_canada_urls():
    """Return EY Canada Ontario job URLs via Exa search.

    EY's global sitemal.xml has <1% Canadian jobs and no filtering.
    Exa indexes individual job pages reliably for EY Canada.
    """
    log("  EY Canada: collecting URLs via Exa...")
    candidate_urls = {}  # url → snippet

    for query in EXA_QUERIES_EY:
        try:
            results = exa_search(query, num_results=10, start_date=LOOKBACK_DATE)
            for r in (results.get("results") or []):
                url = (r.get("url") or "").strip()
                if "careers.ey.com" in url and "/job/" in url:
                    candidate_urls[url] = r.get("text", "")
        except Exception as e:
            log(f"  Exa error: {e}")
        time.sleep(1)

    log(f"  EY Canada: {len(candidate_urls)} candidate URLs")
    return candidate_urls


# ── Job page processing ───────────────────────────────────────────────────────

def process_job(url, loc_text, company_name, seen_keys):
    """Fetch detail page, extract salary+title, return job dict or None."""
    raw_html = fetch_html(url)
    if not raw_html:
        return None

    text = html_to_text(raw_html)

    # Re-confirm Ontario (detail page may have fuller location info)
    if not is_ontario(text) and not is_ontario(loc_text):
        return None

    title = extract_title(raw_html, url)
    if not title:
        return None

    key = f"{title.lower().strip()}|{company_name.lower().strip()}"
    if key in seen_keys:
        return None

    salary = extract_salary(text)
    if not salary:
        return None

    vmin, vmax = salary
    location = parse_location(loc_text or text)
    posted = extract_posted(text)

    return {
        "role":            title,
        "company":         company_name,
        "min":             vmin,
        "max":             vmax,
        "location":        location,
        "source_url":      url,
        "posted":          posted,
        "source_platform": "successfactors",
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== SuccessFactors scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    total_found = 0
    no_salary = 0
    not_ontario = 0

    # ── Step 1: sitemal.xml portals ───────────────────────────────────────────
    for host, company_name in SEED_PORTALS:
        log(f"\n--- {company_name} ({host}) ---")
        ontario_jobs = get_ontario_urls_from_sitemal(host)

        for url, loc_text in ontario_jobs:
            job = process_job(url, loc_text, company_name, seen_keys)
            if job is None:
                # Distinguish not-ontario (detail page) vs no-salary
                no_salary += 1
                time.sleep(0.5)
                continue

            write_job(OUTPUT_FILE, job)
            seen_keys.add(f"{job['role'].lower().strip()}|{company_name.lower().strip()}")
            total_found += 1
            log(f"  FOUND: {job['role'][:50]} | ${job['min']:,}–${job['max']:,} [{job['location']}]")
            time.sleep(1.5)

    # ── Step 2: EY Canada via Exa ─────────────────────────────────────────────
    log("\n--- EY Canada (via Exa) ---")
    ey_urls = get_ey_canada_urls()

    for url in ey_urls:
        job = process_job(url, "", "EY Canada", seen_keys)
        if job is None:
            no_salary += 1
            time.sleep(0.5)
            continue

        write_job(OUTPUT_FILE, job)
        seen_keys.add(f"{job['role'].lower().strip()}|ey canada")
        total_found += 1
        log(f"  FOUND: {job['role'][:50]} | ${job['min']:,}–${job['max']:,} [{job['location']}]")
        time.sleep(1.5)

    log(
        f"\n=== SuccessFactors scraper complete: {total_found} new jobs written "
        f"(no_salary={no_salary}, not_ontario={not_ontario}) ==="
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
