#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-browser.py
Browser-based job scraper for JS-rendered / bot-protected platforms.

Covers platforms that block plain HTTP fetches:
  - SuccessFactors (Scotiabank, Rogers, Magna)
  - Phenom People (Bell Canada)
  - Amazon.jobs
  - Ontario Public Service (NeoGov / gojobs.gov.on.ca)
  - Shopify (Greenhouse, Cloudflare-protected)
  - Loblaw (Paradox), CN Rail (Cornerstone), BambooHR, Breezy.hr, Taleo

Strategy:
  1. Query Exa API for JS-platform job URLs (Ontario, salary, 2026)
  2. For each URL, try plain HTTP first; fall back to Playwright if JS wall detected
  3. Send rendered text to local ollama (qwen2.5:14b) for extraction
  4. Write valid Ontario jobs with disclosed CAD salary to shared output

No token cost — uses local LLM only.
Run: python3 ~/ontario-pay-hub/scripts/search-browser.py
"""

import os
import sys
import time
from datetime import date, timedelta

from _common import (
    OUTPUT_FILE, TODAY, _UA,
    make_logger, acquire_lock,
    fetch_html_text, extract_job,
    load_existing_keys, collect_candidates, write_job,
    is_job_page,
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/browser.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.browser.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

EXA_QUERIES = [
    # NOTE: SuccessFactors (Telus, Deloitte, EY, Scotiabank, OPG) → handled by search-successfactors.py
    # NOTE: KPMG → handled by search-kpmg.py
    # NOTE: Amazon → handled by search-amazon.py
    # This scraper focuses on JS-rendered portals NOT covered by dedicated scrapers.

    # --- Phenom People (RBC jobs.rbc.com, CIBC jobs.cibc.com, TD) ---
    # RBC has two portals: rbc.wd3.myworkdayjobs.com (covered) AND jobs.rbc.com (Phenom, this)
    'site:jobs.rbc.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager OR director',
    'site:jobs.rbc.com Toronto 2026 salary "$" CAD specialist OR associate OR VP OR senior',
    'site:jobs.cibc.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager',
    'site:careers.td.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager',

    # --- IBM Canada (AWS WAF protected, needs Playwright) ---
    'site:careers.ibm.com Ontario Canada salary range "$" CAD 2026 engineer OR analyst OR consultant',
    '"IBM Canada" Ontario job 2026 "salary range" "$" CAD software engineer OR consultant OR manager',

    # --- Microsoft Canada ---
    'site:careers.microsoft.com Ontario Canada salary range "$" CAD 2026 engineer OR program manager',
    '"Microsoft Canada" Ontario job 2026 "salary" "$" CAD software engineer OR product manager',

    # --- Ontario Public Service (gojobs.gov.on.ca / NeoGov) ---
    'site:gojobs.gov.on.ca salary range "$" CAD 2026 manager OR analyst OR specialist OR coordinator',
    '"Ontario Public Service" job 2026 "salary range" "$" CAD analyst OR specialist OR manager OR director',

    # --- Loblaw / Paradox ---
    'site:careers.loblaw.ca Ontario salary range "$" CAD 2026',
    'Loblaw Companies 2026 Ontario job posting "salary range" "$" CAD manager OR analyst OR director',

    # --- Canada Life / Great-West Life ---
    '"Canada Life" OR "Great-West Life" Ontario job posting salary range "$" CAD 2026 analyst OR manager',

    # --- CN Rail (Cornerstone) ---
    'site:cn.ca/careers Ontario salary "$" CAD 2026 engineer OR analyst OR manager OR director',

    # --- Taleo (legacy large-enterprise users: hospitals, municipalities, Hydro) ---
    'site:tbe.taleo.net Ontario Canada salary "$" CAD 2026 manager OR engineer OR analyst',
    'site:career.taleo.net Ontario Canada salary range "$" CAD 2026',

    # --- SmartRecruiters (growing in Toronto scale-ups) ---
    'site:jobs.smartrecruiters.com Ontario Canada salary "$" CAD 2026 engineer OR manager OR analyst',

    # --- Apple Canada ---
    'site:jobs.apple.com Ontario Canada salary range "$" CAD 2026 engineer OR software OR specialist',

    # --- CGI Group ---
    '"CGI Group" OR "CGI Inc" Ontario 2026 "salary range" "$" CAD consultant OR analyst OR developer',

    # --- WSP Canada / Stantec / Jacobs (engineering firms, Workday/custom) ---
    '"WSP Canada" OR "Stantec" Ontario 2026 "salary range" "$" CAD engineer OR specialist OR manager',

    # --- Manulife / Sun Life (branded Workday domains) ---
    'site:careers.manulife.com Ontario salary range "$" CAD 2026 analyst OR manager OR actuary',

    # --- Intact Financial / Aviva Canada / Definity (insurers) ---
    '"Intact Financial" OR "Aviva Canada" OR "Definity" Ontario job salary range "$" CAD 2026',

    # --- Open search: catch new Ontario employers with salary disclosure not in our DB ---
    'Ontario employer job posting 2026 "salary range" "$" CAD site:jobs.lever.co OR site:job-boards.greenhouse.io OR site:jobs.ashby.com',
    'Ontario Canada 2026 "salary range" "$80,000" OR "$90,000" OR "$100,000" CAD job posting manager OR engineer OR analyst -site:glassdoor.com -site:indeed.com',
]


# ── Playwright page fetch ─────────────────────────────────────────────────────
def _fetch_with_browser(url, timeout_ms=15000):
    """Render a JS-heavy page with Playwright headless Chromium.
    Returns plain text (max 5000 chars) or None on failure.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log("  Playwright not installed — pip3 install playwright && python3 -m playwright install chromium")
        return None

    attempts = [
        {"args": ["--disable-http2"], "wait_until": "domcontentloaded", "label": "browser-h1"},
        {"args": [], "wait_until": "commit", "label": "browser-commit"},
    ]

    last_err = None
    for idx, attempt in enumerate(attempts, 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=attempt["args"])
                ctx = browser.new_context(
                    user_agent=_UA,
                    locale="en-CA",
                    viewport={"width": 1280, "height": 800},
                    ignore_https_errors=True,
                )
                page = ctx.new_page()
                page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())
                page.goto(url, wait_until=attempt["wait_until"], timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=4000)
                except PwTimeout:
                    pass
                text = page.inner_text("body")
                browser.close()
            if text:
                return text[:5000], attempt["label"]
        except Exception as e:
            last_err = e
            log(f"  Browser attempt {idx} failed ({attempt['label']}): {e}")
            time.sleep(1)

    if last_err:
        log(f"  Browser error: {last_err}")
    return None, None


def _fetch_page(url):
    """Try plain HTTP first (via _common.fetch_html_text); fall back to Playwright if JS wall.

    Returns (text, method) where method is "http" or a browser variant / fallback.
    """
    text = fetch_html_text(url, max_chars=5000, skip_workday=False, min_content_len=300)
    if text:
        return text, "http"

    text, method = _fetch_with_browser(url)
    if text:
        return text, method

    retry_text = fetch_html_text(url, timeout=20, max_chars=5000, skip_workday=False, min_content_len=120)
    if retry_text:
        log("  HTTP fallback recovered partial content after browser failure")
        return retry_text, "http-retry"

    return None, method or "browser-failed"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Browser scraper started ===")
    log(f"Queries: {len(EXA_QUERIES)} | Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    log(f"Existing jobs to skip: {len(existing_keys)}")

    candidates = collect_candidates(EXA_QUERIES, num_results=8, log=log, start_date=LOOKBACK_DATE)
    log(f"Unique URLs to process: {len(candidates)}")

    jobs_found = 0
    seen_keys = set(existing_keys)

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:3d}/{len(candidates)}] {url[:70]}")
        t0 = time.time()

        page_text, method = _fetch_page(url)
        elapsed_fetch = time.time() - t0
        log(f"  fetch={method} {elapsed_fetch:.1f}s text={len(page_text) if page_text else 0}ch")

        if not page_text:
            log("  → no content")
            time.sleep(1)
            continue

        if not is_job_page(page_text):
            log("  → skip (not a job page)")
            continue

        t1 = time.time()
        try:
            job = extract_job(url, snippet, page_text, log)
        except Exception as e:
            log(f"  → error: {e}")
            continue
        elapsed_llm = time.time() - t1

        if job:
            key = f"{job['role'].lower().strip()}|{job['company'].lower().strip()}"
            if key in seen_keys:
                log(f"  → SKIP duplicate: {job['role']} @ {job['company']}")
                continue
            seen_keys.add(key)
            write_job(OUTPUT_FILE, job)
            jobs_found += 1
            log(f"  → FOUND ({elapsed_llm:.1f}s): {job['role']} @ {job['company']} "
                f"${job['min']:,}–${job['max']:,} [{job.get('location', '')}]")
        else:
            log(f"  → skip ({elapsed_llm:.1f}s)")

        time.sleep(0.5)

    log(f"=== Browser scraper complete: {jobs_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
