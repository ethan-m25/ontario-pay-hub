#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-sap.py
SAP Jobs portal scraper.

Strategy:
  1. Fetch SAP jobs search page filtered to Ontario, Canada (static HTML)
  2. Extract all job page URLs from the listing
  3. Fetch each job detail page (static HTML) and extract salary + metadata
  4. Salary format: "targeted combined range for this position is 69300-149100 CAD"
     Handled by SALARY_RE pattern 3 (text-triggered bare number ranges)

SAP jobs portal: https://jobs.sap.com/search/?searchkey=&country=CA&state=ON
Job URL pattern: https://jobs.sap.com/job/{title}-ON-{postal}/{id}/

Output: JSONL appended to shared raw file, with source_platform="sap".

Run: python3 ~/ontario-pay-hub/scripts/search-sap.py
"""

import html as html_mod
import os
import re
import sys
import time
import urllib.parse
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

from scrapling import Fetcher

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/sap.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.sap.lock")

# SAP Ontario jobs search — static HTML listing
SAP_SEARCH_URLS = [
    "https://jobs.sap.com/search/?searchkey=&country=CA&state=ON",
    "https://jobs.sap.com/search/?searchkey=&country=CA&state=ON&pg=2",
    "https://jobs.sap.com/search/?searchkey=&country=CA&state=ON&pg=3",
]

log = make_logger(LOG_FILE)
fetcher = Fetcher()

# ── Regex patterns ─────────────────────────────────────────────────────────────

# Extract relative job URLs from listing page
JOB_LINK_RE = re.compile(
    r'href="(/job/[^"]+)"',
    re.IGNORECASE,
)

# Date pattern in SAP job pages: "Posted: January 15, 2026" or "2026-01-15"
DATE_RE = re.compile(
    r'(?:Posted[:\s]+)?(\w+ \d{1,2},?\s*\d{4}|\d{4}-\d{2}-\d{2})',
    re.IGNORECASE,
)

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
    "canada",
]

SALARY_RE = [
    # "$86,100 CAD - $136,100 CAD" or "C$90,000 - C$105,000" or "CAD $96,000 - CAD $120,000"
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # SAP: "targeted combined range for this position is 69300-149100 CAD"
    # Also handles: "pay range: 80,000 to 120,000"
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_html(url):
    """Fetch a page and return raw HTML string. Returns None on failure."""
    try:
        page = fetcher.get(url, timeout=25)
        if page.status != 200:
            return None
        return page.html_content or ""
    except Exception as e:
        log(f"  Fetch error ({url[:60]}): {e}")
        return None


def html_to_text(html):
    """Strip HTML tags and return plain text."""
    html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = html_mod.unescape(re.sub(r'<[^>]+>', ' ', html))
    return re.sub(r'\s+', ' ', text).strip()


def extract_job_urls_from_listing(html):
    """Extract /job/... paths from listing page raw HTML."""
    seen = set()
    urls = []
    for m in JOB_LINK_RE.finditer(html or ""):
        # unescape HTML entities in the path (e.g. &amp; → &, %28 stays as-is)
        path = html_mod.unescape(m.group(1)).split("?")[0].rstrip("/")
        if path not in seen and path.startswith("/job/"):
            seen.add(path)
            # Keep % in safe set to avoid double-encoding already-percent-encoded sequences
            urls.append("https://jobs.sap.com" + urllib.parse.quote(path, safe="%/-_.~"))
    return urls


def extract_salary(text):
    """Extract (min, max) annual CAD salary from plain text. Returns None if not found."""
    if not text:
        return None
    for pat in SALARY_RE:
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


def is_ontario(text):
    loc = (text or "").lower()
    return any(t in loc for t in ONTARIO_TERMS)


def parse_location(text):
    city_map = {
        "toronto": "Toronto, ON", "ottawa": "Ottawa, ON",
        "waterloo": "Waterloo, ON", "mississauga": "Mississauga, ON",
        "hamilton": "Hamilton, ON", "london": "London, ON",
        "brampton": "Brampton, ON", "markham": "Markham, ON",
        "vaughan": "Vaughan, ON", "oakville": "Oakville, ON",
        "kitchener": "Kitchener, ON", "windsor": "Windsor, ON",
        "richmond hill": "Richmond Hill, ON",
    }
    loc = (text or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def extract_title_from_html(html):
    """Extract job title from SAP job page raw HTML via <h1> tag."""
    m = re.search(r'<h1[^>]*>(.*?)</h1>', html or "", re.DOTALL | re.IGNORECASE)
    if m:
        title = html_mod.unescape(re.sub(r'<[^>]+>', '', m.group(1))).strip()
        # SAP titles often end in " - Toronto (Hybrid)" or " | SAP" — strip city suffix
        title = re.sub(r'\s*[-|]\s*(Toronto|Ottawa|Waterloo|Vancouver|Canada|SAP)\b.*$', '', title, flags=re.IGNORECASE)
        if len(title) >= 5:
            return title.strip()
    return None


def extract_title_from_url(url):
    """Derive job title from SAP URL slug as fallback.

    URL format: https://jobs.sap.com/job/Toronto-Software-Engineer-ON-M4W/1234567/
    Strips leading city, trailing province+postal, converts hyphens to spaces.
    """
    m = re.search(r'/job/([^/]+)(?:/|$)', url)
    if not m:
        return None
    # URL-decode the slug so we get readable text (e.g. %28 → (, %26 → &)
    slug = urllib.parse.unquote(m.group(1))
    # Remove trailing province+postal: "-ON-M4W1B7" or "-ON"
    slug = re.sub(r'-[A-Z]{2}(?:-[A-Z0-9]+){0,2}$', '', slug)
    # Remove leading city
    parts = slug.replace("(", "").replace(")", "").split("-")
    if parts and parts[0].lower() in {
        "toronto", "ottawa", "waterloo", "mississauga", "hamilton",
        "brampton", "markham", "vaughan", "oakville", "kitchener", "windsor",
        "richmond", "london", "canada", "remote",
    }:
        parts = parts[1:]
    title = " ".join(p for p in parts if p).strip()
    return title or None


def extract_posted_date(text):
    """Try to extract a posting date from page text. Returns TODAY on failure."""
    if not text:
        return TODAY
    m = DATE_RE.search(text[:3000])
    if not m:
        return TODAY
    raw = m.group(1).strip()
    # Try ISO format first
    iso_m = re.match(r'(\d{4}-\d{2}-\d{2})', raw)
    if iso_m:
        return iso_m.group(1)
    # Try "Month DD, YYYY" or "Month DD YYYY"
    for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return TODAY


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== SAP scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Step 1: Collect job URLs from listing pages
    all_job_urls = []
    seen_job_urls = set()
    for search_url in SAP_SEARCH_URLS:
        log(f"Fetching listing: {search_url}")
        listing_html = fetch_html(search_url)
        if not listing_html:
            log("  No content — skipping page")
            continue
        urls = extract_job_urls_from_listing(listing_html)
        new_count = 0
        for u in urls:
            if u not in seen_job_urls:
                seen_job_urls.add(u)
                all_job_urls.append(u)
                new_count += 1
        log(f"  Found {new_count} new job URLs (total: {len(all_job_urls)})")
        time.sleep(2)

    log(f"Total SAP Ontario job URLs: {len(all_job_urls)}")

    # Step 2: Fetch each job page and extract data
    total_found = 0
    no_salary = 0

    for job_url in all_job_urls:
        log(f"  Fetching: {job_url}")
        raw = fetch_html(job_url)
        text = html_to_text(raw) if raw else None
        if not text:
            log(f"    → fetch failed")
            time.sleep(1)
            continue

        # Ontario filter
        if not is_ontario(text):
            log(f"    → not Ontario")
            time.sleep(1)
            continue

        # Title
        title = extract_title_from_html(raw) or extract_title_from_url(job_url)
        if not title:
            log(f"    → no title")
            time.sleep(1)
            continue

        # Dedup
        key = f"{title.lower().strip()}|sap"
        if key in seen_keys:
            log(f"    → duplicate: {title[:50]}")
            time.sleep(1)
            continue

        # Salary
        salary = extract_salary(text)
        if not salary:
            log(f"    [{title[:50]}] → no salary")
            no_salary += 1
            time.sleep(1)
            continue

        vmin, vmax = salary
        location = parse_location(text)
        posted = extract_posted_date(text)

        job_out = {
            "role":            title,
            "company":         "SAP",
            "min":             vmin,
            "max":             vmax,
            "location":        location,
            "source_url":      job_url,
            "posted":          posted,
            "source_platform": "sap",
        }

        write_job(OUTPUT_FILE, job_out)
        seen_keys.add(key)
        total_found += 1
        log(f"    FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{location}]")
        time.sleep(2)

    log(
        f"\n=== SAP scraper complete: {total_found} new jobs written "
        f"(no_salary={no_salary}) ==="
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
