#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-amazon.py
Amazon Jobs scraper.

Strategy:
  1. Query amazon.jobs/en/search.json for Ontario locations (no auth needed)
  2. Deduplicate by job ID across multiple location queries
  3. Fetch each Ontario job page — salary is appended to page HTML but NOT in the API
     response. Typical format: "114,800.00 - 191,800.00 CAD annually"
  4. Extract salary from page HTML via regex

API: GET https://amazon.jobs/en/search.json
     ?normalized_location[]=Toronto%2C+Ontario%2C+CAN
     &result_limit=100&offset=0

Output: JSONL appended to shared raw file, with source_platform="amazon".

Run: python3 ~/ontario-pay-hub/scripts/search-amazon.py
"""

import html as html_mod
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date

from scrapling import Fetcher

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/amazon.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.amazon.lock")

log = make_logger(LOG_FILE)
fetcher = Fetcher()

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BASE_URL     = "https://amazon.jobs/en/search.json"
RESULT_LIMIT = 100

# Ontario location strings used in Amazon Jobs normalized_location filter.
# These correspond to location labels Amazon uses in their system.
QUERY_LOCATIONS = [
    "Toronto, Ontario, CAN",
    "Ottawa, Ontario, CAN",
    "Waterloo, Ontario, CAN",
    "Toronto",                  # catches some roles labeled just "Toronto"
    "Virtual Location - Ontario, CAN",  # remote Ontario roles
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
]

# Salary regex patterns — ordered by specificity (CAD-explicit first).
SALARY_RE = [
    # "114,800.00 - 191,800.00 CAD" or "80,000 - 120,000 CAD annually"
    re.compile(
        r'\$?\s*([\d,]+(?:\.\d+)?)\s*[-–—]\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:CAD|CDN)\b',
        re.IGNORECASE,
    ),
    # "$C 114,800 - $C 191,800" or "C$114,800 – C$191,800"
    re.compile(
        r'(?:C|\bCAD)?\$\s*([\d,]+)(?:\.\d+)?\s*(?:CAD)?\s*[-–—]\s*(?:C|\bCAD)?\$\s*([\d,]+)',
        re.IGNORECASE,
    ),
    # "pay range ... 80,000 to 120,000 CAD"
    re.compile(
        r'(?:pay|salary|compensation|wage)[^$\n]{0,60}([\d,]{6,})(?:\.\d+)?\s*[-–—to]+\s*([\d,]{6,})(?:\.\d+)?\s*(?:CAD|CDN)?',
        re.IGNORECASE,
    ),
    # Generic "$X - $Y" — only accepted when no USD indicator nearby
    re.compile(r'\$\s*([\d,]+)\s*[-–—]\s*\$\s*([\d,]+)', re.IGNORECASE),
]


def _api_fetch(location, offset):
    """Fetch one page from Amazon Jobs API. Returns parsed JSON or None."""
    params = urllib.parse.urlencode([
        ("normalized_location[]", location),
        ("result_limit", RESULT_LIMIT),
        ("offset", offset),
        ("country[]", "CAN"),
    ])
    req = urllib.request.Request(f"{BASE_URL}?{params}")
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "application/json, */*; q=0.01")
    req.add_header("Accept-Language", "en-CA,en;q=0.9")
    req.add_header("Referer", "https://www.amazon.jobs/en/search")
    req.add_header("X-Requested-With", "XMLHttpRequest")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  API error ({location} offset={offset}): {e}")
        return None


def fetch_ontario_jobs():
    """Fetch all Ontario jobs from Amazon Jobs API, deduped by job ID."""
    all_jobs = []
    seen_ids = set()

    for location in QUERY_LOCATIONS:
        offset = 0
        while True:
            data = _api_fetch(location, offset)
            if not data:
                break

            jobs = data.get("jobs", [])
            hits  = data.get("hits", 0)
            log(f"  [{location}] offset={offset}: {len(jobs)}/{hits} jobs")

            if not jobs:
                break

            for job in jobs:
                job_id = str(job.get("id_icims") or job.get("id", ""))
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    all_jobs.append(job)

            offset += RESULT_LIMIT
            if offset >= hits or len(jobs) < RESULT_LIMIT:
                break
            time.sleep(1)

        time.sleep(2)

    return all_jobs


def is_ontario(location_str):
    loc = (location_str or "").lower()
    return any(t in loc for t in ONTARIO_TERMS)


def parse_location(location_str):
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
        "richmond hill":"Richmond Hill, ON",
    }
    loc = (location_str or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def fetch_page_salary(job_path):
    """Fetch the job HTML page and extract salary.

    Amazon posts salary at the bottom of the job page HTML (not in the API):
      "CAN, ON, Toronto - 114,800.00 - 191,800.00 CAD annually"

    Returns (min, max) or None.
    """
    url = f"https://www.amazon.jobs{job_path}"
    try:
        page = fetcher.get(url, timeout=20)
        if page.status != 200:
            return None
        html = page.body
    except Exception as e:
        log(f"  Page fetch error ({job_path}): {e}")
        return None

    # Decode to text
    html_str = html.decode("utf-8", errors="ignore") if isinstance(html, bytes) else html
    text = html_mod.unescape(re.sub(r'<[^>]+>', ' ', html_str))
    text = html_mod.unescape(re.sub(r'\s+', ' ', text).strip())

    return extract_salary_from_text(text)


def extract_salary_from_text(content_text):
    """Extract (min, max) annual salary from plain text. Returns None if not found.

    Prefers explicit CAD patterns. Falls back to generic dollar ranges only when
    no USD indicator appears in the surrounding ±100 chars.
    """
    # First 3 patterns: CAD-explicit (accept unconditionally)
    for pat in SALARY_RE[:3]:
        m = pat.search(content_text)
        if not m:
            continue
        try:
            raw_min = m.group(1).replace(",", "").split(".")[0]
            raw_max = m.group(2).replace(",", "").split(".")[0]
            vmin, vmax = int(raw_min), int(raw_max)
            if "k" in m.group(0).lower():
                vmin, vmax = vmin * 1000, vmax * 1000
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            continue

    # Last pattern: generic "$X - $Y" — reject if USD context nearby
    for pat in SALARY_RE[3:]:
        m = pat.search(content_text)
        if not m:
            continue
        ctx_start = max(0, m.start() - 100)
        ctx_end   = min(len(content_text), m.end() + 100)
        context   = content_text[ctx_start:ctx_end]
        if re.search(r'\bUSD\b|\bUS\s+dollar', context, re.IGNORECASE):
            continue
        try:
            vmin = int(m.group(1).replace(",", ""))
            vmax = int(m.group(2).replace(",", ""))
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            continue

    return None


def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Amazon Jobs scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    log("Fetching Ontario jobs from amazon.jobs API...")
    all_jobs = fetch_ontario_jobs()
    log(f"Total unique Ontario jobs fetched: {len(all_jobs)}")

    total_found = 0

    for job in all_jobs:
        location_str = job.get("location", "")

        # Secondary Ontario filter (API may return adjacent-province jobs)
        if not is_ontario(location_str):
            continue

        title = (job.get("title") or "").strip()
        if not title:
            continue

        company = "Amazon"
        key = f"{title.lower()}|{company.lower()}"
        if key in seen_keys:
            continue

        job_path = job.get("job_path", "")
        if not job_path:
            continue

        salary = fetch_page_salary(job_path)
        if not salary:
            log(f"  [{title[:50]}] → no salary ({location_str})")
            time.sleep(0.5)
            continue

        vmin, vmax = salary
        abs_url = f"https://www.amazon.jobs{job_path}"

        posted   = TODAY
        date_m   = re.search(r'(\d{4}-\d{2}-\d{2})', job.get("posted_date") or "")
        if date_m:
            posted = date_m.group(1)

        job_out = {
            "role":            title,
            "company":         company,
            "min":             vmin,
            "max":             vmax,
            "location":        parse_location(location_str),
            "source_url":      abs_url,
            "posted":          posted,
            "source_platform": "amazon",
        }

        write_job(OUTPUT_FILE, job_out)
        seen_keys.add(key)
        total_found += 1
        log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{location_str}]")
        time.sleep(1)

    log(f"\n=== Amazon Jobs scraper complete: {total_found} new jobs written ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
