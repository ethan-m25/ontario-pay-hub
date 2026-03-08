#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-jobs.py
Ontario job discovery: Exa API (find URLs) + qwen2.5:14b via HTTP API (extract data)

Run by kisame at 2 AM ET daily via OpenClaw cron.

Flow:
  1. Query Exa API with Ontario-salary-specific searches
  2. For each unique URL: fetch HTML, send to local ollama for extraction
  3. Write valid jobs as JSON lines to ~/.openclaw/shared/ontario-jobs-raw-DATE.txt
  4. update-jobs.sh picks up that file next
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
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/search.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.search.lock")
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

EXA_QUERIES = [
    # --- Lever / Greenhouse (core) ---
    'Ontario Canada job posting 2026 salary range "$" CAD engineer OR analyst OR manager site:jobs.lever.co OR site:boards.greenhouse.io OR site:job-boards.greenhouse.io',
    'Toronto OR Waterloo OR Ottawa hiring 2026 "salary range" OR "compensation range" "$" CAD developer OR director OR senior',
    'Ontario 2026 job "base salary" "$80,000" OR "$90,000" OR "$100,000" OR "$120,000" OR "$150,000" site:careers.*.com OR site:jobs.*',
    'ontario.ca OR jobs.toronto.ca OR linkedin.com/jobs Ontario 2026 salary disclosed compensation CAD',
    'Ontario employer pay transparency 2026 new opening "salary" "$" CAD VP OR director OR manager OR specialist',

    # --- Jobvite (server-rendered, confirmed Ontario salary data) ---
    'site:jobs.jobvite.com Ontario Canada salary range "$" CAD 2026 engineer OR analyst OR manager OR nurse OR director',
    'site:jobs.jobvite.com Toronto OR Ottawa OR Waterloo OR Mississauga salary "$" CAD',

    # --- Indeed Canada via Exa (Exa index bypasses 403; viewjob pages have employer-disclosed ranges) ---
    'site:ca.indeed.com/viewjob Ontario 2026 salary "$" CAD engineer OR analyst OR manager OR director',
    'site:ca.indeed.com/viewjob Toronto OR Ottawa OR Waterloo salary range "$" CAD 2026',
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
