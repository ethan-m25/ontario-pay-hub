#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-google.py
Google Careers scraper.

Strategy:
  1. Playwright renders Google Careers search (Angular SPA) for Toronto/Canada.
     Paginate through all result pages to collect job URLs.
  2. Scrapling (TLS-resilient) fetches each individual job HTML page.
     Salary is embedded in the static HTML — no JS needed for extraction.
     Format: "The Canada base salary range ... is CAD 194,000-199,000"
  3. Filter: Ontario/Canada location + salary present.

Ontario search queries (location param):
  - "Toronto Canada"
  - "Ottawa Canada"
  - "Waterloo Canada"
  - (covers ~95% of Google's Ontario roles)

Output: JSONL appended to shared raw file, source_platform="google".

Run: python3 ~/ontario-pay-hub/scripts/search-google.py
"""

import html as html_mod
import json
import os
import re
import sys
import time

from playwright.sync_api import sync_playwright
from scrapling import Fetcher

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/google.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.google.lock")

log = make_logger(LOG_FILE)
fetcher = Fetcher()

BASE_URL = "https://www.google.com/about/careers/applications/jobs/results/"

# Location search queries → normalized location label
SEARCH_LOCATIONS = [
    ("Toronto Canada",   "Toronto, ON"),
    ("Ottawa Canada",    "Ottawa, ON"),
    ("Waterloo Canada",  "Waterloo, ON"),
    ("Ontario Canada",   "Ontario, ON"),
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "kitchener", "canada",
]

SALARY_RE = [
    # "CAD 194,000-199,000" or "CAD 194,000 – 199,000"
    re.compile(r'CAD\s*([\d,]+)\s*[-–—]\s*([\d,]+)', re.IGNORECASE),
    # "CA$194,000 – CA$199,000"
    re.compile(r'CA\$\s*([\d,]+)\s*[-–—]\s*CA\$\s*([\d,]+)', re.IGNORECASE),
    # "$194,000 – $199,000 CAD"
    re.compile(r'\$\s*([\d,]+)\s*[-–—]\s*\$\s*([\d,]+)\s*(?:CAD|CDN)', re.IGNORECASE),
    # "194,000 to 199,000 CAD"
    re.compile(r'([\d,]{6,})\s+to\s+([\d,]{6,})\s*(?:CAD|CDN)', re.IGNORECASE),
]


def _extract_salary(text):
    """Extract (min, max) annual salary from plain text. Returns None if not found."""
    for pat in SALARY_RE:
        m = pat.search(text)
        if m:
            try:
                vmin = int(m.group(1).replace(",", ""))
                vmax = int(m.group(2).replace(",", ""))
                if 25_000 <= vmin <= 700_000 and vmin < vmax:
                    return vmin, vmax
            except (ValueError, IndexError):
                continue
    return None


def _extract_job_text(html_bytes):
    """Strip HTML and decode text from a Google job page."""
    html_str = html_bytes.decode("utf-8", errors="ignore") if isinstance(html_bytes, bytes) else html_bytes
    # Unescape HTML entities, strip tags
    text = html_mod.unescape(re.sub(r'<[^>]+>', ' ', html_str))
    return re.sub(r'\s+', ' ', text).strip()


def _is_ontario(location_str):
    loc = (location_str or "").lower()
    return any(t in loc for t in ONTARIO_TERMS)


def _parse_location(location_str):
    city_map = {
        "toronto":      "Toronto, ON",
        "ottawa":       "Ottawa, ON",
        "waterloo":     "Waterloo, ON",
        "mississauga":  "Mississauga, ON",
        "markham":      "Markham, ON",
        "kitchener":    "Kitchener, ON",
    }
    loc = (location_str or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def _get_job_urls_via_playwright(location_query):
    """Use Playwright to render Google Careers search and collect all job URLs."""
    search_url = f"{BASE_URL}?location={location_query.replace(' ', '+')}&employment_type=FULL_TIME"
    job_urls = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)

            # Accept cookie consent if shown
            try:
                page.locator("text=Agree").first.click(timeout=4_000)
                time.sleep(1)
            except Exception:
                pass

            page.wait_for_load_state("networkidle", timeout=20_000)
            time.sleep(2)

            page_num = 1
            while True:
                # Extract job URLs from current page
                new_urls = page.evaluate(r"""() => {
                    const links = new Set();
                    document.querySelectorAll("a").forEach(a => {
                        if (a.href && a.href.includes("/jobs/results/") && /\d{10,}/.test(a.href)) {
                            links.add(a.href.split("?")[0]);  // strip query params
                        }
                    });
                    return Array.from(links);
                }""")

                before = len(job_urls)
                job_urls.update(new_urls)
                log(f"  Page {page_num}: {len(new_urls)} URLs ({len(job_urls)} total)")

                # Navigate to next page if present
                next_link = page.get_by_label("Go to next page")
                if not next_link.is_visible(timeout=2_000):
                    break

                next_link.click()
                page.wait_for_load_state("networkidle", timeout=15_000)
                time.sleep(1.5)
                page_num += 1

        except Exception as e:
            log(f"  Playwright error: {e}")
        finally:
            browser.close()

    return job_urls


def _fetch_job_details(url):
    """
    Fetch individual Google job page via Scrapling.
    Returns (title, location_str, salary_tuple) or None.
    Salary is in the static HTML — no JS rendering required.
    """
    try:
        page = fetcher.get(url, timeout=20)
        if page.status != 200:
            return None
        text = _extract_job_text(page.body)
    except Exception as e:
        log(f"  fetch error ({url[-40:]}): {e}")
        return None

    # Extract title
    title_m = re.search(r'<title[^>]*>([^<]+)</title>', page.body.decode("utf-8", errors="ignore"))
    title = title_m.group(1).strip() if title_m else ""
    # Google titles: "Job Title | Google Careers" or "Job Title — Google Careers"
    title = re.sub(r'\s*[\|—]\s*(Google Careers|Google|YouTube).*$', '', title, flags=re.IGNORECASE).strip()
    if not title:
        # Fallback: extract from URL slug
        slug_m = re.search(r'/results/\d+[-]([^?#]+)', url)
        if slug_m:
            title = slug_m.group(1).replace("-", " ").title()

    # Extract location (e.g. "Toronto, ON, Canada")
    loc_m = re.search(r'(Toronto|Ottawa|Waterloo|Mississauga|Markham|Kitchener|Brampton),?\s+ON[,\s]', text, re.IGNORECASE)
    location_str = loc_m.group(0).strip() if loc_m else ""
    if not location_str:
        # Check broader Canada context
        loc_m2 = re.search(r'(Canada|Ontario)', text, re.IGNORECASE)
        location_str = loc_m2.group(0) if loc_m2 else ""

    salary = _extract_salary(text)
    return title, location_str, salary


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Google Careers scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    all_job_urls = set()

    # Phase 1: Collect job URLs via Playwright
    for location_query, default_loc in SEARCH_LOCATIONS:
        log(f"\nSearching: {location_query}")
        urls = _get_job_urls_via_playwright(location_query)
        all_job_urls.update(urls)
        log(f"  → {len(urls)} URLs found")
        time.sleep(2)

    log(f"\n{len(all_job_urls)} unique job URLs collected")

    total_found = 0

    # Phase 2: Fetch each job page for salary
    for i, url in enumerate(sorted(all_job_urls)):
        result = _fetch_job_details(url)
        if not result:
            time.sleep(0.5)
            continue

        title, location_str, salary = result

        if not title:
            continue

        if not _is_ontario(location_str):
            log(f"  [{title[:40]}] → not Ontario ({location_str})")
            time.sleep(0.5)
            continue

        if not salary:
            log(f"  [{title[:40]}] → no salary")
            time.sleep(0.5)
            continue

        key = f"{title.lower()}|google"
        if key in seen_keys:
            time.sleep(0.3)
            continue

        vmin, vmax = salary

        job_out = {
            "role":            title,
            "company":         "Google",
            "min":             vmin,
            "max":             vmax,
            "location":        _parse_location(location_str),
            "source_url":      url,
            "posted":          TODAY,
            "source_platform": "google",
        }

        write_job(OUTPUT_FILE, job_out)
        seen_keys.add(key)
        total_found += 1
        log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{location_str}]")
        time.sleep(1)

    log(f"\n=== Google scraper complete: {total_found} new jobs written ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
