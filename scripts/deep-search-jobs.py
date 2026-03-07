#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/deep-search-jobs.py
ONE-OFF historical deep search — broader coverage, no date restriction.

Differences from search-jobs.py:
  - No startPublishedDate filter (finds pre-Jan 2026 voluntary disclosures)
  - 20 queries covering more ATS platforms, sectors, and specific Ontario companies
  - Higher num_results per query (12 vs 8)
  - Output appends to shared raw file (update-jobs.sh deduplicates)

Run manually: python3 ~/ontario-pay-hub/scripts/deep-search-jobs.py
"""

import atexit
import json
import os
import re
import signal
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import date

EXA_API_KEY  = os.environ.get("EXA_API_KEY", "d0d9614a-58d8-4166-9b27-4ae6b6e2761e")
OLLAMA_API   = "http://127.0.0.1:11434/api/generate"
MODEL        = "qwen2.5:14b"
TODAY        = date.today().isoformat()
SHARED_DIR   = os.path.expanduser("~/.openclaw/shared")
OUTPUT_FILE  = os.path.join(SHARED_DIR, f"ontario-jobs-raw-{TODAY}.txt")
LOG_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/deep-search.log")
LOCK_FILE    = os.path.expanduser("~/ontario-pay-hub/scripts/.deep-search.lock")

# Global HTTP read timeout — prevents r.read() from hanging indefinitely
socket.setdefaulttimeout(20)

SKIP_PATTERNS = [
    "glassdoor.com/Salary", "payscale.com", "salary.com",
    "indeed.com/salary", "ziprecruiter.com/Salaries",
    "linkedin.com/jobs/search", "linkedin.com/jobs/?",
]

# ── 20 queries: historical + expanded sectors + specific ATS + companies ─────
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

    # --- Indeed Canada via Exa (Exa index bypasses direct 403; viewjob pages contain real employer-disclosed ranges) ---
    'site:ca.indeed.com/viewjob Ontario 2026 salary "$" CAD engineer OR analyst OR manager OR director OR specialist',
    'site:ca.indeed.com/viewjob Toronto OR Ottawa OR Waterloo salary range "$" CAD 2026',
    'site:ca.indeed.com/viewjob Ontario salary "$" CAD 2024 OR 2025 healthcare OR government OR finance OR technology',
]


# ── Lock file ─────────────────────────────────────────────────────────────────
def _release_lock():
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def _acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # raises OSError if process doesn't exist
            log(f"Another instance is already running (PID {old_pid}). Exiting.")
            return False
        except (OSError, ValueError):
            log("Stale lock file found — removing and continuing.")
            os.remove(LOCK_FILE)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(_release_lock)
    signal.signal(signal.SIGTERM, lambda s, f: (_release_lock(), sys.exit(1)))
    return True


# ── Logging ───────────────────────────────────────────────────────────────────
def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Exa search ────────────────────────────────────────────────────────────────
def exa_search(query, num_results=12):
    url = "https://api.exa.ai/search"
    # No startPublishedDate — gets historical results
    payload = json.dumps({
        "query": query,
        "numResults": num_results,
        "type": "auto",
        "contents": {"text": {"maxCharacters": 2000}},
    }).encode()

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("x-api-key", EXA_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  Exa error: {e}")
        return None


# ── Page fetch ────────────────────────────────────────────────────────────────
def fetch_page_text(url, timeout=15):
    """Fetch job posting page, strip HTML → plain text (max 4000 chars).
    socket.setdefaulttimeout() ensures r.read() cannot hang indefinitely.
    """
    if not url:
        return None
    if "myworkdayjobs.com" in url:
        return None
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (compatible; OntarioPayHub-Scraper/2.0)")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode("utf-8", errors="ignore")
        html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:4000]
    except Exception:
        return None


# ── LLM extraction via ollama HTTP API ───────────────────────────────────────
EXTRACT_PROMPT = """\
Extract ONE Ontario job posting from the text below.

URL: {url}
Summary/snippet: {snippet}
Page text: {page_text}

Today's date: {today}

Return ONLY valid JSON in this exact format if a valid Ontario job with explicit CAD salary range is found:
{{"role":"Job Title","company":"Company Name","min":80000,"max":120000,"location":"Toronto, ON","source_url":"{url}","posted":"YYYY-MM-DD"}}

Return ONLY the word null (no quotes, no JSON) if:
- No explicit CAD annual salary range with actual dollar numbers
- Not an Ontario location
- This is a salary guide / aggregator page / company careers homepage
- Hourly rate only (do NOT convert hourly to annual)
- URL is a search results page

Rules:
- min and max = annual CAD integers (e.g. 90000)
- location must be in Ontario (Toronto, Ottawa, Waterloo, Mississauga, Hamilton, London, Brampton, Markham, Vaughan, Oakville, Kitchener, Windsor, ON)
- posted = date visible in posting, or {today} if not shown
- source_url = exact URL of this specific job posting"""


def _call_ollama(prompt):
    """Call ollama HTTP API. Returns raw text output or raises exception."""
    payload = json.dumps({
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": 256},
    }).encode()
    req = urllib.request.Request(OLLAMA_API, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as r:
        resp = json.loads(r.read())
    return resp.get("response", "").strip()


def extract_job(url, snippet, page_text):
    prompt = EXTRACT_PROMPT.format(
        url=url,
        snippet=(snippet or "")[:600],
        page_text=(page_text or "")[:3000],
        today=TODAY,
    )

    output = None
    for attempt in range(2):
        try:
            output = _call_ollama(prompt)
            break
        except Exception as e:
            if attempt == 0:
                log(f"  Ollama attempt 1 failed ({type(e).__name__}: {e}) — retrying in 5s")
                time.sleep(5)
            else:
                log(f"  Ollama failed after retry: {e}")
                return None

    if output is None:
        return None
    if re.match(r'^null$', output, re.IGNORECASE):
        return None

    match = re.search(r'\{[^{}]*"role"[^{}]*\}', output, re.DOTALL)
    if not match:
        return None

    try:
        job = json.loads(match.group())
    except json.JSONDecodeError:
        return None

    for k in ("role", "company", "min", "max", "source_url"):
        if k not in job:
            return None
    if not (30_000 <= int(job["min"]) <= 700_000):
        return None
    if int(job["min"]) >= int(job["max"]):
        return None

    location = job.get("location", "")
    ontario_terms = [
        "ontario", "toronto", "ottawa", "waterloo", "mississauga",
        "hamilton", "london", "brampton", "markham", "vaughan",
        "richmond hill", "oakville", "kitchener", "windsor", ", on",
    ]
    if not any(t in location.lower() for t in ontario_terms):
        return None
    return job


def load_existing_keys():
    """Load dedup keys from existing jobs.json to avoid re-adding known jobs."""
    data_file = os.path.expanduser("~/ontario-pay-hub/data/jobs.json")
    try:
        with open(data_file) as f:
            db = json.load(f)
        return set(
            f"{j['role'].lower().strip()}|{j['company'].lower().strip()}"
            for j in db.get("jobs", [])
        )
    except Exception:
        return set()


def main():
    if not _acquire_lock():
        return 1

    log(f"=== Ontario Pay Hub DEEP SEARCH started ===")
    log(f"Queries: {len(EXA_QUERIES)} | No date restriction | Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    log(f"Existing jobs in DB to skip: {len(existing_keys)}")

    # Step 1: collect URLs
    candidates = {}
    for i, query in enumerate(EXA_QUERIES, 1):
        log(f"Exa [{i:2d}/{len(EXA_QUERIES)}]: {query[:70]}...")
        resp = exa_search(query, num_results=12)
        if not resp:
            continue
        results = resp.get("results", [])
        log(f"  → {len(results)} results")
        for r in results:
            url = r.get("url", "").strip()
            if not url or url in candidates:
                continue
            if any(p in url for p in SKIP_PATTERNS):
                continue
            text = r.get("text") or ""
            candidates[url] = text[:600] if text else ""
        time.sleep(1.5)

    log(f"Unique URLs to process: {len(candidates)}")

    # Step 2: extract — write each job immediately to survive crashes
    os.makedirs(SHARED_DIR, exist_ok=True)
    jobs_found = 0
    seen_keys = set(existing_keys)

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:3d}/{len(candidates)}] {url[:75]}")
        page_text = fetch_page_text(url)
        t0 = time.time()

        try:
            job = extract_job(url, snippet, page_text)
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
            # Write immediately — crash-safe
            with open(OUTPUT_FILE, "a") as f:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
            jobs_found += 1
            log(f"  → FOUND ({elapsed:.1f}s): {job['role']} @ {job['company']} "
                f"${job['min']:,}–${job['max']:,} [{job.get('location','')}] posted={job.get('posted','?')}")
        else:
            log(f"  → skip ({elapsed:.1f}s)")

    log(f"=== Deep search complete: {jobs_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
