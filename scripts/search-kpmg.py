#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-kpmg.py
KPMG Canada careers scraper.

Strategy:
  1. Playwright renders careers.kpmg.ca/professionals/jobs (Jibe CDN + iCIMS backend,
     JS-rendered listing — no static HTML job list available).
     Scrolls through all pages to collect job detail URLs.
  2. Scrapling fetches each job detail page (static HTML, 400KB+).
     Salary format: "$55,000 to $82,000" — matches SALARY_RE pattern 1.
  3. Ontario filter on location string from page.

Job URL pattern: https://careers.kpmg.ca/professionals/jobs/{id}

Output: JSONL appended to shared raw file, source_platform="kpmg".

Run: python3 ~/ontario-pay-hub/scripts/search-kpmg.py
"""

import html as html_mod
import os
import re
import sys
import time

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from scrapling import Fetcher

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE, _UA,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/kpmg.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.kpmg.lock")

LISTING_URL = "https://careers.kpmg.ca/professionals/jobs?lang=en-US"

log = make_logger(LOG_FILE)
fetcher = Fetcher()

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
    "canada",
]

SALARY_RE = [
    # "$55,000 to $82,000" or "$55,000 – $82,000" or "C$90,000 - C$105,000"
    re.compile(r'(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—to]+\s*(?:[A-Z]{1,3}\s*)?\$\s*([\d,]+)', re.IGNORECASE),
    # "$86K – $136K"
    re.compile(r'(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]\s*[-–—]\s*(?:[A-Z])?\$([\d]+(?:\.\d+)?)[kK]', re.IGNORECASE),
    # "pay range: 80,000 to 120,000" or "salary range is 69300 to 149100"
    re.compile(r'(?:pay|salary|compensation|wage|combined range|targeted range|salary range is)[^$\n]{0,50}([\d,]{5,})\s*[-–—to]+\s*\$?([\d,]{5,})', re.IGNORECASE),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

_NON_ON_PROVINCES = [
    "british columbia", "alberta", "quebec", "québec",
    "manitoba", "saskatchewan", "nova scotia", "new brunswick",
    "prince edward", "newfoundland", "northwest territories",
    "yukon", "nunavut",
]

_EXTRACT_JOBS_JS = r"""() => {
    const results = [];
    document.querySelectorAll("a[href]").forEach(a => {
        const href = a.href || '';
        if (href.includes('/professionals/jobs/') && /\/\d+/.test(href)) {
            let card = a.closest('li, article, [class*="job"], [class*="result"]') || a.parentElement;
            const loc = card ? card.innerText : '';
            results.push([href.split('?')[0], loc.toLowerCase()]);
        }
    });
    return results;
}"""


def _filter_ontario_urls(url_loc_pairs, existing):
    """Return set of new Ontario-candidate URLs from (url, loc_text) pairs."""
    new_urls = set()
    for job_url, loc_lower in url_loc_pairs:
        if job_url in existing:
            continue
        is_on = any(t in loc_lower for t in ONTARIO_TERMS)
        is_clearly_other = any(p in loc_lower for p in _NON_ON_PROVINCES)
        if is_on or not is_clearly_other:
            new_urls.add(job_url)
    return new_urls


def get_job_urls_via_playwright():
    """Render KPMG listing via Playwright, paginate via ?page=N, return Ontario URLs.

    Jibe listing supports URL-based pagination: ?page=1, ?page=2, etc.
    10 jobs per page, 322+ total (~33 pages). Pre-filters by location text.
    """
    job_urls = set()
    MAX_PAGES = 40  # guard against infinite loops (322 jobs / 10 per page = ~33 pages)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=_UA,
            locale="en-CA",
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()
        page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())

        try:
            for page_num in range(1, MAX_PAGES + 1):
                url = f"{LISTING_URL}&page={page_num}"
                log(f"  Playwright page {page_num}: {url}")
                page.goto(url, wait_until="domcontentloaded", timeout=40_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=12_000)
                except PwTimeout:
                    pass
                time.sleep(2)

                url_loc_pairs = page.evaluate(_EXTRACT_JOBS_JS)
                if not url_loc_pairs:
                    log(f"  Page {page_num}: no jobs — stopping")
                    break

                new = _filter_ontario_urls(url_loc_pairs, job_urls)
                job_urls.update(new)
                log(f"  Page {page_num}: +{len(new)} Ontario URLs ({len(job_urls)} total)")

                # Check if this is the last page (fewer than expected jobs on page)
                if len(url_loc_pairs) < 10:
                    log(f"  Page {page_num}: only {len(url_loc_pairs)} jobs — last page")
                    break

        except Exception as e:
            log(f"  Playwright error: {e}")
        finally:
            browser.close()

    return job_urls


def _careers_url_to_icims(careers_url):
    """Convert careers.kpmg.ca job URL to iCIMS iframe URL.

    careers.kpmg.ca/professionals/jobs/31762
    → exphire-kpmgca.icims.com/jobs/31762/job?in_iframe=1&hashed=-1

    The ?in_iframe=1 parameter bypasses the AngularJS redirect and returns
    the actual job content as static HTML.
    """
    m = re.search(r'/professionals/jobs/(\d+)', careers_url)
    if not m:
        return None
    job_id = m.group(1)
    return f"https://exphire-kpmgca.icims.com/jobs/{job_id}/job?in_iframe=1&hashed=-1"


def fetch_job_text(url):
    """Fetch KPMG job detail page via iCIMS iframe URL.

    Returns (raw_html, plain_text). Returns (None, None) on failure.
    """
    icims_url = _careers_url_to_icims(url)
    if not icims_url:
        return None, None
    try:
        page = fetcher.get(icims_url, timeout=30)
        if page.status != 200:
            return None, None
        raw_html = page.html_content or ""
        raw_html_stripped = re.sub(r'<script[^>]*>.*?</script>', ' ', raw_html, flags=re.DOTALL | re.IGNORECASE)
        raw_html_stripped = re.sub(r'<style[^>]*>.*?</style>', ' ', raw_html_stripped, flags=re.DOTALL | re.IGNORECASE)
        text = html_mod.unescape(re.sub(r'<[^>]+>', ' ', raw_html_stripped))
        return raw_html, re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        log(f"  Fetch error ({url[:60]}): {e}")
        return None, None


def extract_title(html, url):
    """Extract job title from KPMG job detail page."""
    if html:
        m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
        if m:
            title = html_mod.unescape(re.sub(r'<[^>]+>', '', m.group(1))).strip()
            if len(title) >= 5:
                return title
        # Try title tag
        m = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        if m:
            title = html_mod.unescape(m.group(1)).strip()
            title = re.sub(r'\s*[\|—]\s*KPMG.*$', '', title, flags=re.IGNORECASE).strip()
            if len(title) >= 5:
                return title
    return None


def extract_salary(text):
    """Extract (min, max) annual CAD salary from plain text."""
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== KPMG scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Step 1: Get job URLs from Playwright-rendered listing
    log("Step 1: collecting job URLs via Playwright...")
    job_urls = get_job_urls_via_playwright()
    log(f"  {len(job_urls)} unique job URLs found")

    if not job_urls:
        log("No job URLs found — check if Playwright/Chromium is installed")
        log("  Install: pip3 install playwright && python3 -m playwright install chromium")
        return 1

    # Step 2: Fetch each job page and extract data
    total_found = 0
    no_salary = 0
    not_ontario = 0

    for job_url in sorted(job_urls):
        raw_html, text = fetch_job_text(job_url)
        if not text:
            log(f"  → fetch failed: {job_url}")
            time.sleep(1)
            continue

        if not is_ontario(text):
            not_ontario += 1
            time.sleep(0.5)
            continue

        title = extract_title(raw_html, job_url) if raw_html else None
        if not title:
            log(f"  → no title: {job_url}")
            time.sleep(1)
            continue

        key = f"{title.lower().strip()}|kpmg"
        if key in seen_keys:
            time.sleep(0.5)
            continue

        salary = extract_salary(text)
        if not salary:
            log(f"  [{title[:50]}] → no salary")
            no_salary += 1
            time.sleep(1)
            continue

        vmin, vmax = salary
        location = parse_location(text)

        # Try to extract posted date
        posted = TODAY
        date_m = re.search(r'(20\d{2}-\d{2}-\d{2})', text[:5000])
        if date_m:
            posted = date_m.group(1)

        job_out = {
            "role":            title,
            "company":         "KPMG",
            "min":             vmin,
            "max":             vmax,
            "location":        location,
            "source_url":      job_url,
            "posted":          posted,
            "source_platform": "kpmg",
        }

        write_job(OUTPUT_FILE, job_out)
        seen_keys.add(key)
        total_found += 1
        log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{location}]")
        time.sleep(2)

    log(
        f"\n=== KPMG scraper complete: {total_found} new jobs written "
        f"(no_salary={no_salary}, not_ontario={not_ontario}) ==="
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
