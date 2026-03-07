#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-jobs.py
Ontario job discovery: Exa API (find URLs) + qwen2.5:14b (extract structured data)

Run by kisame at 2 AM ET daily via OpenClaw cron.

Flow:
  1. Query Exa API with 5 Ontario-salary-specific searches
  2. For each unique URL: fetch HTML, strip to text
  3. Pipe to local model → extract role/company/min/max/location/url/posted
  4. Write valid jobs as JSON lines to ~/.openclaw/shared/ontario-jobs-raw-DATE.txt
  5. update-jobs.sh picks up that file next

Output format (one JSON per line):
  {"role":"...","company":"...","min":N,"max":N,"location":"...","source_url":"...","posted":"YYYY-MM-DD"}
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import date, timedelta

# ── Config ─────────────────────────────────────────────────────────────────────
EXA_API_KEY  = os.environ.get("EXA_API_KEY", "d0d9614a-58d8-4166-9b27-4ae6b6e2761e")
OLLAMA_BIN   = os.path.expanduser("~/.local/bin/ollama")
MODEL        = "qwen2.5:14b"
TODAY        = date.today().isoformat()
YESTERDAY    = (date.today() - timedelta(days=1)).isoformat()
SHARED_DIR   = os.path.expanduser("~/.openclaw/shared")
OUTPUT_FILE  = os.path.join(SHARED_DIR, f"ontario-jobs-raw-{TODAY}.txt")
LOG_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/search.log")

# Thirty-day lookback for Exa date filter
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

# URLs from these domains are homepage/aggregator — skip
SKIP_PATTERNS = [
    "glassdoor.com/Salary", "payscale.com", "salary.com",
    "indeed.com/salary", "ziprecruiter.com/Salaries",
    "linkedin.com/jobs/search", "linkedin.com/jobs/?",
    "workopolis.com/search", "monster.ca/jobs/search",
    "eluta.ca", "simplyhired.ca/search",
]

# Exa search queries — optimised for job-posting pages with explicit salary ranges
EXA_QUERIES = [
    'Ontario Canada job posting 2026 salary range "$" CAD engineer OR analyst OR manager site:jobs.lever.co OR site:boards.greenhouse.io OR site:job-boards.greenhouse.io',
    'Toronto OR Waterloo OR Ottawa hiring 2026 "salary range" OR "compensation range" "$" CAD developer OR director OR senior',
    'Ontario 2026 job "base salary" "$80,000" OR "$90,000" OR "$100,000" OR "$120,000" OR "$150,000" site:careers.*.com OR site:jobs.*',
    'ontario.ca OR jobs.toronto.ca OR linkedin.com/jobs Ontario 2026 salary disclosed compensation CAD',
    'Ontario employer pay transparency 2026 new opening "salary" "$" CAD VP OR director OR manager OR specialist',
]


# ── Logging ────────────────────────────────────────────────────────────────────
def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Exa search ─────────────────────────────────────────────────────────────────
def exa_search(query, num_results=10):
    url = "https://api.exa.ai/search"
    payload = json.dumps({
        "query": query,
        "numResults": num_results,
        "type": "auto",
        "contents": {"text": {"maxCharacters": 2000}},
        "startPublishedDate": LOOKBACK_DATE,
    }).encode()

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("x-api-key", EXA_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  Exa error: {e}")
        return None


# ── Page fetch ────────────────────────────────────────────────────────────────
def fetch_page_text(url, timeout=12):
    """Fetch job posting page, strip HTML → plain text (max 4000 chars)."""
    if not url:
        return None
    if "myworkdayjobs.com" in url:
        return None  # SPA — content unreachable without JS
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


# ── LLM extraction ────────────────────────────────────────────────────────────
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


def extract_job(url, snippet, page_text):
    """Call local model to extract structured job data. Returns dict or None."""
    prompt = EXTRACT_PROMPT.format(
        url=url,
        snippet=(snippet or "")[:600],
        page_text=(page_text or "")[:3000],
        today=TODAY,
    )

    try:
        result = subprocess.run(
            [OLLAMA_BIN, "run", MODEL],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = result.stdout.strip()

        # Check for explicit null response
        if re.match(r'^null$', output, re.IGNORECASE):
            return None

        # Extract JSON object
        match = re.search(r'\{[^{}]*"role"[^{}]*\}', output, re.DOTALL)
        if not match:
            return None

        job = json.loads(match.group())

        # Validate required fields
        for k in ("role", "company", "min", "max", "source_url"):
            if k not in job:
                return None

        # Validate salary range
        if not (30_000 <= int(job["min"]) <= 700_000):
            return None
        if int(job["min"]) >= int(job["max"]):
            return None

        # Validate Ontario location
        location = job.get("location", "")
        ontario_terms = [
            "ontario", "toronto", "ottawa", "waterloo", "mississauga",
            "hamilton", "london", "brampton", "markham", "vaughan",
            "richmond hill", "oakville", "kitchener", "windsor", ", on",
        ]
        if not any(t in location.lower() for t in ontario_terms):
            return None

        return job

    except (json.JSONDecodeError, subprocess.TimeoutExpired, ValueError):
        return None
    except Exception:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log(f"=== Ontario Pay Hub job search started ===")
    log(f"Model: {MODEL} | Output: {OUTPUT_FILE}")

    # Step 1: collect URLs from Exa
    candidates = {}  # url → snippet

    for i, query in enumerate(EXA_QUERIES, 1):
        log(f"Exa [{i}/{len(EXA_QUERIES)}]: {query[:65]}...")
        resp = exa_search(query, num_results=8)
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
            snippet = text[:600] if text else ""
            candidates[url] = snippet

        time.sleep(1.5)  # Exa rate limit courtesy

    log(f"Unique URLs to process: {len(candidates)}")

    # Step 2: extract jobs
    jobs_out = []
    seen_keys = set()

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:2d}/{len(candidates)}] {url[:75]}")

        # Fetch page text (supplement Exa snippet)
        page_text = fetch_page_text(url)

        t0 = time.time()
        job = extract_job(url, snippet, page_text)
        elapsed = time.time() - t0

        if job:
            key = f"{job['role'].lower().strip()}|{job['company'].lower().strip()}"
            if key in seen_keys:
                log(f"  → SKIP duplicate: {job['role']} @ {job['company']}")
                continue
            seen_keys.add(key)
            jobs_out.append(job)
            log(f"  → FOUND ({elapsed:.1f}s): {job['role']} @ {job['company']} "
                f"${job['min']:,}–${job['max']:,} [{job.get('location','')}]")
        else:
            log(f"  → skip ({elapsed:.1f}s)")

    # Step 3: write output
    os.makedirs(SHARED_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        for job in jobs_out:
            f.write(json.dumps(job, ensure_ascii=False) + "\n")

    log(f"=== Search complete: {len(jobs_out)} valid jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
