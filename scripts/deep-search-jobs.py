#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/deep-search-jobs.py
ONE-OFF historical deep search — broader coverage, no date restriction.

Differences from search-jobs.py:
  - No startPublishedDate filter (finds pre-Jan 2026 voluntary disclosures)
  - 27 queries covering more ATS platforms, sectors, and specific Ontario companies
  - Higher num_results per query (12 vs 8)
  - Output appends to shared raw file (update-jobs.sh deduplicates)

Run manually: python3 ~/ontario-pay-hub/scripts/deep-search-jobs.py
"""

import os
import sys
import time
from datetime import date

from _common import (
    OUTPUT_FILE, TODAY,
    make_logger, acquire_lock,
    fetch_html_text, extract_job,
    load_existing_keys, collect_candidates, write_job,
)

LOG_FILE  = os.path.expanduser("~/ontario-pay-hub/scripts/deep-search.log")
LOCK_FILE = os.path.expanduser("~/ontario-pay-hub/scripts/.deep-search.lock")

log = make_logger(LOG_FILE)

# No LOOKBACK_DATE — no date filter (historical search)
EXA_QUERIES = [
    # --- Pre-2026 voluntary disclosures ---
    'Ontario Canada job posting salary range "$" CAD 2024 OR 2025 engineer OR analyst OR manager site:jobs.lever.co OR site:boards.greenhouse.io',
    'Toronto hiring 2024 2025 "salary range" OR "compensation range" "$" CAD developer OR director OR senior site:jobs.lever.co OR site:boards.greenhouse.io',
    'Ontario job posting 2024 "base salary" "$80,000" OR "$90,000" OR "$100,000" OR "$120,000" OR "$150,000" CAD',
    'Toronto Waterloo Ottawa 2025 salary disclosed "$" CAD job opening engineer OR product OR analyst',

    # --- Ashby ATS (growing in Toronto tech) ---
    'site:ashbyhq.com Ontario salary range "$" CAD',
    'site:ashbyhq.com Toronto "$" CAD engineer OR manager OR analyst',

    # --- SmartRecruiters ---
    'site:careers.smartrecruiters.com Ontario salary "$" CAD',

    # --- Specific Ontario/Canadian tech companies known for early transparency ---
    'site:shopify.com careers salary range "$" CAD Ontario',
    'site:wealthsimple.com careers salary "$" CAD',
    '"Float" OR "Cohere" OR "Veeva" OR "Caseware" OR "Procore" Toronto job salary range "$" CAD site:boards.greenhouse.io OR site:jobs.lever.co OR site:ashbyhq.com',
    '"FreshBooks" OR "Wave" OR "Ritual" OR "Koho" OR "Nuvei" Toronto job posting salary "$" CAD',
    '"Shopify" OR "Wealthsimple" OR "PointClickCare" OR "Geotab" Ontario job salary range 2024 OR 2025 "$" CAD',

    # --- Healthcare sector (Ontario hospitals) ---
    'site:uhn.ca careers salary range "$" CAD',
    'site:sunnybrook.ca careers salary "$" CAD',
    'Ontario hospital healthcare job posting salary range "$" CAD 2024 OR 2025 OR 2026 nurse OR therapist OR analyst OR manager',

    # --- Ontario government / public sector ---
    'site:gojobs.gov.on.ca salary range "$" CAD',
    'site:ontario.ca/page/careers salary OR compensation CAD',
    'Ontario Public Service job posting salary range "$" CAD manager OR analyst OR specialist OR director',

    # --- Financial services ---
    'Toronto financial services job posting salary range "$" CAD 2024 OR 2025 analyst OR associate OR manager site:boards.greenhouse.io OR site:jobs.lever.co',
    '"RBC" OR "TD Bank" OR "Scotiabank" OR "BMO" OR "CIBC" OR "Manulife" OR "Sun Life" Ontario job salary range "$" CAD site:boards.greenhouse.io OR site:jobs.lever.co OR site:ashbyhq.com',

    # --- builtintoronto.com ---
    'site:builtintoronto.com salary range "$" CAD',

    # --- Jobvite (server-rendered, confirmed Ontario salary data: Ornge, VON, Innio, etc.) ---
    'site:jobs.jobvite.com Ontario Canada salary range "$" CAD engineer OR analyst OR manager OR nurse OR director',
    'site:jobs.jobvite.com Toronto OR Ottawa OR Waterloo OR Mississauga OR Hamilton salary "$" CAD',
    'site:jobs.jobvite.com Ontario Canada salary "$" CAD 2024 OR 2025 healthcare OR education OR government OR finance',

    # --- Indeed Canada via Exa ---
    'site:ca.indeed.com/viewjob Ontario 2026 salary "$" CAD engineer OR analyst OR manager OR director OR specialist',
    'site:ca.indeed.com/viewjob Toronto OR Ottawa OR Waterloo salary range "$" CAD 2026',
    'site:ca.indeed.com/viewjob Ontario salary "$" CAD 2024 OR 2025 healthcare OR government OR finance OR technology',
]


def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Ontario Pay Hub DEEP SEARCH started ===")
    log(f"Queries: {len(EXA_QUERIES)} | No date restriction | Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    log(f"Existing jobs in DB to skip: {len(existing_keys)}")

    # No start_date — historical search
    candidates = collect_candidates(EXA_QUERIES, num_results=12, log=log)
    log(f"Unique URLs to process: {len(candidates)}")

    jobs_found = 0
    seen_keys = set(existing_keys)

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:3d}/{len(candidates)}] {url[:75]}")
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
                f"${job['min']:,}–${job['max']:,} [{job.get('location', '')}] posted={job.get('posted', '?')}")
        else:
            log(f"  → skip ({elapsed:.1f}s)")

    log(f"=== Deep search complete: {jobs_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
