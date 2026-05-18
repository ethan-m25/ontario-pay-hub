#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-ashby.py
Ashby job board scraper (server-rendered boards only).

Strategy:
  1. Fetch https://jobs.ashbyhq.com/{slug} — some boards embed full job JSON
     in the HTML (server-rendered). Those are scrapable without JS.
  2. Parse jobPostings array from the embedded JSON.
  3. Filter Ontario/Canada locations.
  4. Extract salary from compensationTierSummary field (format: "CA$52K – CA$65K").
  5. Skip hourly-rated jobs ("per hour").
  6. If compensationTierSummary is empty, fetch individual job page and try
     regex extraction from the HTML description.

Companies with server-rendered boards (confirmed 2026-03-19):
  wealthsimple, 1password, cohere, certn

Output: JSONL appended to shared raw file, source_platform="ashby".

Run: python3 ~/ontario-pay-hub/scripts/search-ashby.py
"""

import html as html_mod
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    write_job, TODAY, OUTPUT_FILE,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/ashby.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.ashby.lock")

log = make_logger(LOG_FILE)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"

# ── Seed slugs — confirmed server-rendered (data embedded in HTML) ────────────
# (slug, company_name)  company_name=None → use name from page
SEED_SLUGS = [
    ("wealthsimple", "Wealthsimple"),    # Toronto fintech — 31 Ontario+salary
    ("certn",        "Certn"),           # Canada remote — 4 Ontario+salary
    ("1password",    "1Password"),       # Waterloo — try individual pages
    ("cohere",       "Cohere"),          # Toronto AI — try individual pages
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
    "canada",   # "Remote (Canada)" / "Canada - Remote"
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

# Salary patterns — ordered by specificity
SALARY_RE = [
    # "CA$52K – CA$65K" or "CA$52,880 – CA$66,100"
    re.compile(
        r'CA\$\s*([\d,]+(?:\.\d+)?)\s*[kK]?\s*[-–—]\s*CA\$\s*([\d,]+(?:\.\d+)?)\s*[kK]?',
        re.IGNORECASE,
    ),
    # "$52,000 – $65,000 CAD" or "$52K - $65K CAD"
    re.compile(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*[kK]?\s*[-–—]\s*\$\s*([\d,]+(?:\.\d+)?)\s*[kK]?\s*(?:CAD|CDN)',
        re.IGNORECASE,
    ),
    # Generic "$X – $Y" fallback (no USD indicator)
    re.compile(r'\$\s*([\d,]+)\s*[-–—]\s*\$\s*([\d,]+)', re.IGNORECASE),
]


def _fetch(url):
    """Simple HTTP GET, returns text or None."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        log(f"  fetch error ({url}): {e}")
        return None


def _parse_salary_summary(summary):
    """
    Parse compensationTierSummary string like:
      "CA$52K – CA$65K • Offers Equity"
      "CA$52,880 – CA$66,100 • Offers Equity"
      "CA$25.00 – CA$26.50 per hour"   ← skip
    Returns (vmin, vmax) or None.
    """
    if not summary:
        return None
    # Skip hourly pay
    if "per hour" in summary.lower() or "/hr" in summary.lower():
        return None

    m = SALARY_RE[0].search(summary)
    if m:
        try:
            raw_min = m.group(1).replace(",", "")
            raw_max = m.group(2).replace(",", "")
            # Detect K suffix from original string near match
            k_suffix = "k" in summary[m.start():m.end()].lower()
            vmin = int(float(raw_min) * (1000 if k_suffix and float(raw_min) < 1000 else 1))
            vmax = int(float(raw_max) * (1000 if k_suffix and float(raw_max) < 1000 else 1))
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            pass
    return None


def _parse_salary_from_text(text):
    """Regex salary extraction from plain text (for individual job pages)."""
    for i, pat in enumerate(SALARY_RE):
        m = pat.search(text)
        if not m:
            continue
        # Reject generic pattern if USD context nearby
        if i == 2:
            ctx = text[max(0, m.start()-100):m.end()+100]
            if re.search(r'\bUSD\b|\bUS\s+dollar', ctx, re.IGNORECASE):
                continue
        try:
            raw_min = m.group(1).replace(",", "")
            raw_max = m.group(2).replace(",", "")
            k_suffix = "k" in m.group(0).lower()
            vmin = int(float(raw_min) * (1000 if k_suffix and float(raw_min) < 1000 else 1))
            vmax = int(float(raw_max) * (1000 if k_suffix and float(raw_max) < 1000 else 1))
            if 25_000 <= vmin <= 700_000 and vmin < vmax:
                return vmin, vmax
        except (ValueError, IndexError):
            continue
    return None


def _fetch_job_salary(slug, job_id):
    """Fetch individual job page and extract salary from description HTML."""
    url = f"https://jobs.ashbyhq.com/{slug}/{job_id}"
    html = _fetch(url)
    if not html:
        return None
    # Extract description from jobPosting JSON in page
    idx = html.find('"descriptionHtml\"')
    if idx != -1:
        chunk = html[idx+len('"descriptionHtml\"') + 1:]
        # The value is a JSON string — find end quote
        if chunk.startswith('"'):
            end = chunk.find('",\n') if '",\n' in chunk[:5000] else chunk.find('"', 1)
            desc_raw = chunk[1:end]
            desc_text = re.sub(r'<[^>]+>', ' ', html_mod.unescape(desc_raw.replace('\\n', '\n').replace('\\"', '"')))
            desc_text = html_mod.unescape(re.sub(r'\s+', ' ', desc_text))
            return _parse_salary_from_text(desc_text)
    # Fallback: strip all HTML and regex search
    plain = html_mod.unescape(re.sub(r'<[^>]+>', ' ', html))
    plain = re.sub(r'\s+', ' ', plain)
    return _parse_salary_from_text(plain)


def _parse_jobs_from_html(html):
    """Extract jobPostings JSON array embedded in server-rendered Ashby HTML."""
    idx = html.find('jobPostings\":[')
    if idx == -1:
        return None  # JS-rendered SPA — not supported
    chunk = html[idx + len('jobPostings\":['):]
    depth = 1
    i = 0
    while i < len(chunk) and depth > 0:
        if chunk[i] == '[':
            depth += 1
        elif chunk[i] == ']':
            depth -= 1
        i += 1
    try:
        return json.loads('[' + chunk[:i])
    except json.JSONDecodeError:
        return None


def _parse_location(location_str):
    city_map = {
        "toronto":       "Toronto, ON",
        "ottawa":        "Ottawa, ON",
        "waterloo":      "Waterloo, ON",
        "mississauga":   "Mississauga, ON",
        "hamilton":      "Hamilton, ON",
        "brampton":      "Brampton, ON",
        "markham":       "Markham, ON",
        "vaughan":       "Vaughan, ON",
        "oakville":      "Oakville, ON",
        "kitchener":     "Kitchener, ON",
        "windsor":       "Windsor, ON",
        "richmond hill": "Richmond Hill, ON",
    }
    loc = (location_str or "").lower()
    for city, label in city_map.items():
        if city in loc:
            return label
    return "Ontario, ON"


def _is_ontario(location_str):
    loc = (location_str or "").lower()
    if any(t in loc for t in _NON_ONTARIO_LOC_TERMS):
        return False
    return any(t in loc for t in ONTARIO_TERMS)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Ashby scraper started ===")
    log(f"Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    seen_keys = set(existing_keys)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    total_found = 0
    skipped_spa = 0

    for slug, company_display in SEED_SLUGS:
        url = f"https://jobs.ashbyhq.com/{slug}"
        html = _fetch(url)
        if not html:
            log(f"── {slug}: fetch failed")
            continue

        jobs = _parse_jobs_from_html(html)
        if jobs is None:
            log(f"── {slug}: JS-rendered SPA — skipping")
            skipped_spa += 1
            time.sleep(1)
            continue

        company_name = company_display or slug.replace("-", " ").title()
        log(f"\n── {company_name} ({slug}): {len(jobs)} total jobs ──")

        ontario_count = 0
        found_this = 0

        for job in jobs:
            loc_name = job.get("locationName", "") or ""
            if not _is_ontario(loc_name):
                continue
            ontario_count += 1

            title = (job.get("title") or "").strip()
            if not title:
                continue

            key = f"{title.lower()}|{company_name.lower()}"
            if key in seen_keys:
                continue

            # Try compensationTierSummary first
            salary = _parse_salary_summary(job.get("compensationTierSummary", ""))

            # Fallback: fetch individual job page
            if not salary:
                job_id = job.get("id", "")
                if job_id:
                    salary = _fetch_job_salary(slug, job_id)
                    time.sleep(0.5)

            if not salary:
                log(f"  [{title[:50]}] → no salary")
                continue

            vmin, vmax = salary

            posted = TODAY
            date_m = re.search(r'(\d{4}-\d{2}-\d{2})', job.get("publishedDate") or "")
            if date_m:
                posted = date_m.group(1)

            abs_url = f"https://jobs.ashbyhq.com/{slug}/{job.get('id', '')}"

            job_out = {
                "role":            title,
                "company":         company_name,
                "min":             vmin,
                "max":             vmax,
                "location":        _parse_location(loc_name),
                "source_url":      abs_url,
                "posted":          posted,
                "source_platform": "ashby",
            }

            write_job(OUTPUT_FILE, job_out)
            seen_keys.add(key)
            total_found += 1
            found_this += 1
            log(f"  FOUND: {title[:50]} | ${vmin:,}–${vmax:,} [{loc_name}]")

        log(f"  Ontario: {ontario_count} | New w/ salary: {found_this}")
        time.sleep(2)

    log(
        f"\n=== Ashby scraper complete: {total_found} new jobs written "
        f"(skipped_spa={skipped_spa}) ==="
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
