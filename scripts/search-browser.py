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
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/browser.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.browser.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

EXA_QUERIES = [
    # --- SAP SuccessFactors (Scotiabank, Rogers, Magna, Telus, CIBC uses hybrid) ---
    'site:scotiabank.com Ontario job salary range "$" CAD 2026 engineer OR analyst OR manager',
    'site:jobs.rogers.com Ontario salary "$" CAD 2026',  # correct domain (careers.rogers.com does not resolve)
    'site:magna.com careers Ontario salary range "$" CAD 2026',
    'SuccessFactors Ontario Canada job posting 2026 salary "$" CAD engineer OR director OR manager',

    # --- Telus (SuccessFactors at careers.telus.com — confirmed salary disclosure) ---
    'site:careers.telus.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager OR specialist',
    'Telus Communications Ontario job 2026 "salary range" "$" CAD analyst OR engineer OR manager OR director',

    # --- Phenom People (Bell Canada, RBC jobs.rbc.com, CIBC jobs.cibc.com) ---
    # NOTE: RBC has two portals — rbc.wd3.myworkdayjobs.com (Workday, covered by search-workday.py)
    # AND jobs.rbc.com (Phenom People, different job set). Both need to be scraped.
    'site:jobs.bell.ca Ontario salary range "$" CAD 2026',
    'Bell Canada Ontario job 2026 salary "$" CAD engineer OR analyst OR manager',
    'site:jobs.rbc.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager OR director',
    'site:jobs.rbc.com Toronto 2026 salary "$" CAD specialist OR associate OR VP OR senior',
    'site:jobs.cibc.com Ontario salary range "$" CAD 2026',
    'site:careers.td.com Ontario salary range "$" CAD 2026 engineer OR analyst OR manager',

    # --- Amazon.jobs ---
    'site:amazon.jobs Ontario Canada salary range "$" CAD 2026',
    'amazon.ca OR amazon.jobs Ontario 2026 salary range "$" CAD software engineer OR operations OR analyst',

    # --- Ontario Public Service / NeoGov ---
    'site:gojobs.gov.on.ca salary range "$" CAD 2026 manager OR analyst OR specialist OR coordinator',
    'Ontario Public Service 2026 salary range "$" CAD "Ministry of" job posting annual',
    '"Ontario Public Service" job 2026 "salary range" "$" CAD analyst OR specialist OR manager OR director',

    # --- Shopify (Greenhouse, Cloudflare-protected) ---
    'site:shopify.com/careers Ontario salary range "$" CAD 2026',

    # --- Paradox (Loblaw) ---
    'site:careers.loblaw.ca Ontario salary range "$" CAD 2026',
    'Loblaw Companies 2026 Ontario job posting "salary range" "$" CAD manager OR analyst OR director',

    # --- Canada Life / Great-West Life / Lifeco (Winnipeg HQ but large Ontario presence) ---
    'site:canadalife.com careers Ontario salary range "$" CAD 2026',
    '"Canada Life" OR "Great-West Life" Ontario job posting salary range "$" CAD 2026',

    # --- CN Rail (Cornerstone) ---
    'site:cn.ca/careers Ontario salary "$" CAD 2026',

    # --- BambooHR (mid-size Ontario tech/retail/logistics) ---
    'site:*.bamboohr.com/careers Ontario salary range "$" CAD 2026',

    # --- Breezy.hr (growing in Toronto tech) ---
    'site:app.breezy.hr Ontario salary "$" CAD 2026 engineer OR analyst OR manager',

    # --- Taleo (large legacy users) ---
    'site:oracle.taleo.net OR site:ats.ca Ontario 2026 salary range "$" CAD',
    'site:tbe.taleo.net Ontario Canada salary "$" CAD 2026 manager OR engineer OR analyst',

    # --- Workday custom domains (careers sites with own domain, not myworkdayjobs.com) ---
    # Some Ontario employers embed Workday but use branded URLs like careers.manulife.com
    'site:careers.manulife.com Ontario salary range "$" CAD 2026',
    '"Intact Financial" OR "Intact Insurance" Ontario job salary range "$" CAD 2026',
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

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=_UA,
                locale="en-CA",
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            # Block images/fonts to speed up load
            page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except PwTimeout:
                pass  # page is loaded enough
            text = page.inner_text("body")
            browser.close()
        return text[:5000] if text else None
    except Exception as e:
        log(f"  Browser error: {e}")
        return None


def _fetch_page(url):
    """Try plain HTTP first (via _common.fetch_html_text); fall back to Playwright if JS wall.

    Returns (text, method) where method is "http" or "browser".
    """
    # min_content_len=300: return None (not just empty string) when page is a JS shell
    text = fetch_html_text(url, max_chars=5000, skip_workday=False, min_content_len=300)
    if text:
        return text, "http"
    text = _fetch_with_browser(url)
    return text, "browser"


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
