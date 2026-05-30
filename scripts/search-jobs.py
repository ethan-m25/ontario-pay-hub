#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-jobs.py
Ontario job discovery: Exa API (find URLs) + qwen2.5:14b via HTTP API (extract data)

Run by kisame at 2 AM ET daily via OpenClaw cron.

Flow:
  1. Query Exa API with Ontario-salary-specific searches
  2. For each unique URL: fetch HTML, quick job-page check, then LLM extraction
  3. Write valid jobs as JSON lines to ~/.openclaw/shared/ontario-jobs-raw-DATE.txt
  4. update-jobs.sh picks up that file next

NOTE: Lever/Greenhouse/Workday/KPMG/SuccessFactors/Amazon all have dedicated scrapers.
      This script targets platforms NOT covered elsewhere: Jobvite, iCIMS, SmartRecruiters,
      Breezy, JazzHR, Indeed viewjob pages, and long-tail Ontario employers.
"""

import os
import sys
import time
from datetime import date, timedelta

from _common import (
    OUTPUT_FILE, TODAY,
    make_logger, acquire_lock,
    fetch_html_text, extract_job,
    load_existing_keys, collect_candidates, write_job,
    is_job_page,
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/search.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.search.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=14)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

EXA_QUERIES = [
    # --- Jobvite (server-rendered, confirmed Ontario salary data) ---
    'site:jobs.jobvite.com Ontario Canada salary range "$" CAD 2026 engineer OR analyst OR manager OR director',
    'site:jobs.jobvite.com Toronto OR Ottawa OR Waterloo OR Mississauga salary "$" CAD 2026',

    # --- iCIMS (hospitals, municipalities, professional services) ---
    'site:careers.icims.com Ontario Canada salary range "$" CAD 2026 analyst OR manager OR specialist OR coordinator',
    'site:jobs.icims.com Ontario Canada salary "$" CAD 2026 engineer OR nurse OR manager OR director',

    # --- SmartRecruiters (Toronto scale-ups and mid-size companies) ---
    'site:jobs.smartrecruiters.com Ontario Canada salary range "$" CAD 2026 engineer OR manager OR analyst OR designer',

    # --- JazzHR (small-to-mid Ontario employers) ---
    'site:app.jazz.co Ontario Canada salary "$" CAD 2026 manager OR analyst OR coordinator OR specialist',

    # --- Breezy.hr (growing in Toronto tech and professional services) ---
    'site:app.breezy.hr Ontario Canada salary "$" CAD 2026 engineer OR analyst OR manager OR director',

    # --- Rippling / Deel / Remote (newer HR platforms, Ontario tech companies) ---
    'site:jobs.rippling.com Ontario Canada salary range "$" CAD 2026',

    # --- Indeed Canada viewjob (employer-disclosed ranges in canonical job pages) ---
    'site:ca.indeed.com/viewjob Ontario 2026 salary "$" CAD engineer OR analyst OR manager OR director',
    'site:ca.indeed.com/viewjob Toronto OR Ottawa OR Waterloo salary range "$" CAD 2026',

    # --- Long-tail Ontario employers (catch-all for companies not on major ATSes) ---
    'Ontario Canada job posting 2026 "salary range" "$" CAD -site:glassdoor.com -site:indeed.com -site:linkedin.com -site:ziprecruiter.com engineer OR analyst OR manager site:*.com/careers OR site:*.ca/careers',
    'Toronto OR Waterloo OR Ottawa hiring 2026 "salary range" "$" CAD developer OR director OR senior -site:glassdoor.com -site:payscale.com -site:salary.com',

    # --- Public sector (municipalities, agencies, Crown corps not on Workday) ---
    'Ontario municipality OR "City of" 2026 "salary range" "$" CAD job posting manager OR analyst OR director site:*.ca',
    '"Ontario agency" OR "Crown corporation" OR "MFIPPA" 2026 salary range "$" CAD job posting annual Ontario',

    # --- Healthcare / hospitals (often on Taleo or custom ATS) ---
    'Ontario hospital OR "health network" 2026 "salary range" "$" CAD job posting nurse OR manager OR analyst OR coordinator',
    'site:tbe.taleo.net OR site:career.taleo.net Ontario Canada salary "$" CAD 2026 manager OR analyst OR specialist',
]


def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Ontario Pay Hub job search started ===")

    existing_keys = load_existing_keys()
    log(f"Existing jobs to skip: {len(existing_keys)}")

    candidates = collect_candidates(EXA_QUERIES, num_results=8, log=log, start_date=LOOKBACK_DATE)
    log(f"Unique URLs to process: {len(candidates)}")

    jobs_found = 0
    seen_keys = set(existing_keys)

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:2d}/{len(candidates)}] {url[:75]}")
        t0 = time.time()
        page_text = fetch_html_text(url)

        if not page_text:
            log(f"  → no content")
            continue

        if not is_job_page(page_text):
            log(f"  → skip (not a job page)")
            continue

        try:
            job = extract_job(url, snippet, page_text, log)
        except Exception as e:
            log(f"  → error: {e}")
            continue

        elapsed = time.time() - t0

        if job:
            key = f"{job['role'].lower().strip()}|{job['company'].lower().strip()}"
            if key in seen_keys:
                log(f"  → SKIP duplicate: {job['role']} @ {job['company']}")
                continue
            seen_keys.add(key)
            write_job(OUTPUT_FILE, job)
            jobs_found += 1
            log(f"  → FOUND ({elapsed:.1f}s): {job['role']} @ {job['company']} "
                f"${job['min']:,}–${job['max']:,} [{job.get('location', '')}]")
        else:
            log(f"  → skip ({elapsed:.1f}s)")

    log(f"=== Search complete: {jobs_found} valid jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
