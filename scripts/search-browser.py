#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-browser.py
Browser-based job scraper for JS-rendered / bot-protected platforms.

Covers platforms that block plain HTTP fetches:
  - SuccessFactors (Scotiabank, Rogers)
  - Phenom People (Bell Canada)
  - Amazon.jobs
  - Ontario Public Service (NeoGov / gojobs.gov.on.ca)
  - Shopify (Greenhouse, Cloudflare-protected)
  - Magna International (SuccessFactors)

Strategy:
  1. Query Exa API for JS-platform job URLs (Ontario, salary, 2026)
  2. For each URL, render with Playwright headless Chromium
  3. Send rendered text to local ollama (qwen2.5:14b) for extraction
  4. Write valid Ontario jobs with disclosed CAD salary to shared output

No token cost — uses local LLM only.
Run: python3 ~/ontario-pay-hub/scripts/search-browser.py
"""

import atexit
import json
import os
import re
import signal
import socket
import sys
import time
import urllib.request
from datetime import date

socket.setdefaulttimeout(20)

EXA_API_KEY  = os.environ.get("EXA_API_KEY", "d0d9614a-58d8-4166-9b27-4ae6b6e2761e")
OLLAMA_API   = "http://127.0.0.1:11434/api/generate"
MODEL        = "qwen2.5:14b"
TODAY        = date.today().isoformat()
SHARED_DIR   = os.path.expanduser("~/.openclaw/shared")
OUTPUT_FILE  = os.path.join(SHARED_DIR, f"ontario-jobs-raw-{TODAY}.txt")
LOG_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/browser.log")
LOCK_FILE    = os.path.expanduser("~/ontario-pay-hub/scripts/.browser.lock")

# 30-day lookback
from datetime import timedelta
LOOKBACK_DATE = (date.today() - timedelta(days=30)).isoformat() + "T00:00:00.000Z"

# URLs that are homepages / aggregators — skip
SKIP_PATTERNS = [
    "glassdoor.com/Salary", "payscale.com", "salary.com",
    "indeed.com/salary", "ziprecruiter.com/Salaries",
    "linkedin.com/jobs/search", "linkedin.com/jobs/?",
    "workopolis.com/search", "monster.ca/jobs/search",
    "eluta.ca", "simplyhired.ca/search",
    "myworkdayjobs.com",  # handled by search-workday.py
]

# Exa queries targeting JS-rendered platforms specifically
EXA_QUERIES = [
    # --- SAP SuccessFactors (Scotiabank, Rogers, Magna) ---
    'site:scotiabank.com Ontario job salary range "$" CAD 2026 engineer OR analyst OR manager',
    'site:careers.rogers.com Ontario salary "$" CAD 2026',
    'site:magna.com careers Ontario salary range "$" CAD 2026',
    'SuccessFactors Ontario Canada job posting 2026 salary "$" CAD engineer OR director OR manager',

    # --- Phenom People (Bell Canada) ---
    'site:jobs.bell.ca Ontario salary range "$" CAD 2026',
    'Bell Canada Ontario job 2026 salary "$" CAD engineer OR analyst OR manager',

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

    # --- CN Rail (Cornerstone) ---
    'site:cn.ca/careers Ontario salary "$" CAD 2026',

    # --- BambooHR (mid-size Ontario tech/retail/logistics) ---
    'site:*.bamboohr.com/careers Ontario salary range "$" CAD 2026',

    # --- Breezy.hr (growing in Toronto tech) ---
    'site:app.breezy.hr Ontario salary "$" CAD 2026 engineer OR analyst OR manager',

    # --- Taleo (large legacy users) ---
    'site:oracle.taleo.net OR site:ats.ca Ontario 2026 salary range "$" CAD',
    'site:tbe.taleo.net Ontario Canada salary "$" CAD 2026 manager OR engineer OR analyst',
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
            os.kill(old_pid, 0)
            log(f"Another instance is already running (PID {old_pid}). Exiting.")
            return False
        except (OSError, ValueError):
            log("Stale lock file — removing.")
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
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Exa search ────────────────────────────────────────────────────────────────
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
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  Exa error: {e}")
        return None


# ── Playwright page fetch ─────────────────────────────────────────────────────
def fetch_with_browser(url, timeout_ms=15000):
    """Render JS page with Playwright headless Chromium. Returns plain text (max 5000 chars)."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log("  Playwright not installed — pip3 install playwright && python3 -m playwright install chromium")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="en-CA",
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            # Block images/fonts to speed up load
            page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # Wait for main content to appear (up to 5s extra)
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except PwTimeout:
                pass  # ok — page is loaded enough
            text = page.inner_text("body")
            browser.close()
        return text[:5000] if text else None
    except Exception as e:
        log(f"  Browser error: {e}")
        return None


# ── Simple HTTP fetch fallback ────────────────────────────────────────────────
def fetch_simple(url, timeout=12):
    """Try plain HTTP first; returns None if content is a JS shell (<300 chars)."""
    if not url:
        return None
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        req.add_header("Accept-Language", "en-CA,en;q=0.9")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode("utf-8", errors="ignore")
        html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) < 300:
            return None  # JS wall — caller should use browser
        return text[:5000]
    except Exception:
        return None


def fetch_page_text(url):
    """Try simple HTTP first; fall back to Playwright if JS wall detected."""
    text = fetch_simple(url)
    if text:
        return text, "http"
    text = fetch_with_browser(url)
    return text, "browser"


# ── LLM extraction via ollama HTTP API ────────────────────────────────────────
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


# ── Dedup ─────────────────────────────────────────────────────────────────────
def load_existing_keys():
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


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not _acquire_lock():
        return 1

    log("=== Browser scraper started ===")
    log(f"Queries: {len(EXA_QUERIES)} | Model: {MODEL} | Output: {OUTPUT_FILE}")

    existing_keys = load_existing_keys()
    log(f"Existing jobs to skip: {len(existing_keys)}")

    # Step 1: collect URLs from Exa
    candidates = {}
    for i, query in enumerate(EXA_QUERIES, 1):
        log(f"Exa [{i:2d}/{len(EXA_QUERIES)}]: {query[:65]}...")
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
            candidates[url] = text[:600] if text else ""
        time.sleep(1.5)

    log(f"Unique URLs to process: {len(candidates)}")

    # Step 2: fetch + extract
    os.makedirs(SHARED_DIR, exist_ok=True)
    jobs_found = 0
    seen_keys = set(existing_keys)

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        log(f"[{i:3d}/{len(candidates)}] {url[:70]}")
        t0 = time.time()

        page_text, method = fetch_page_text(url)
        elapsed_fetch = time.time() - t0
        log(f"  fetch={method} {elapsed_fetch:.1f}s text={len(page_text) if page_text else 0}ch")

        if not page_text:
            log("  → no content")
            time.sleep(1)
            continue

        t1 = time.time()
        try:
            job = extract_job(url, snippet, page_text)
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
            with open(OUTPUT_FILE, "a") as f:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
            jobs_found += 1
            log(f"  → FOUND ({elapsed_llm:.1f}s): {job['role']} @ {job['company']} "
                f"${job['min']:,}–${job['max']:,} [{job.get('location','')}]")
        else:
            log(f"  → skip ({elapsed_llm:.1f}s)")

        time.sleep(0.5)

    log(f"=== Browser scraper complete: {jobs_found} new jobs written to {OUTPUT_FILE} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
