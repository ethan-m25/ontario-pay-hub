#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/_common.py
Shared utilities for all Ontario Pay Hub search scripts.

Provides: logging, process locking, Exa search, HTML fetch, LLM extraction,
dedup key loading, and candidate URL collection.

All search scripts import from here to avoid copy-paste drift.
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
from datetime import date, timedelta

# Global socket timeout — prevents r.read() from hanging indefinitely on any script
socket.setdefaulttimeout(20)

# ── Shared constants ───────────────────────────────────────────────────────────
EXA_API_KEY = os.environ.get("EXA_API_KEY", "d0d9614a-58d8-4166-9b27-4ae6b6e2761e")
OLLAMA_API  = "http://127.0.0.1:11434/api/generate"
MODEL       = "qwen2.5:14b"
TODAY       = date.today().isoformat()
SHARED_DIR  = os.path.expanduser("~/.openclaw/shared")
OUTPUT_FILE = os.path.join(SHARED_DIR, f"ontario-jobs-raw-{TODAY}.txt")
DATA_FILE   = os.path.expanduser("~/ontario-pay-hub/data/jobs.json")

# Chrome-like UA — less likely to be blocked than a generic scraper string
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# URLs from these domains are homepages or aggregators — skip during candidate collection
SKIP_PATTERNS = [
    "glassdoor.com/Salary", "payscale.com", "salary.com",
    "indeed.com/salary", "ziprecruiter.com/Salaries",
    "linkedin.com/jobs/search", "linkedin.com/jobs/?",
    "workopolis.com/search", "monster.ca/jobs/search",
    "eluta.ca", "simplyhired.ca/search",
    "myworkdayjobs.com",  # handled by search-workday.py
]

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "london", "brampton", "markham", "vaughan",
    "richmond hill", "oakville", "kitchener", "windsor", ", on",
]


# ── Logging ────────────────────────────────────────────────────────────────────
def make_logger(log_file):
    """Return a log(msg) function that writes timestamped lines to log_file and stdout.

    Log directory is created once at construction time, not on every call,
    which avoids a syscall overhead on every log line.
    """
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    def log(msg):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line, flush=True)
        with open(log_file, "a") as f:
            f.write(line + "\n")

    return log


# ── Process lock ──────────────────────────────────────────────────────────────
def acquire_lock(lock_file, log):
    """Acquire a PID lock file. Returns True on success, False if another instance is running.

    Registers atexit and SIGTERM handlers to release the lock on exit.
    Stale locks (process no longer running) are automatically removed.
    """
    if os.path.exists(lock_file):
        try:
            with open(lock_file) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # raises OSError if process doesn't exist
            log(f"Another instance is already running (PID {old_pid}). Exiting.")
            return False
        except (OSError, ValueError):
            log("Stale lock file — removing.")
            os.remove(lock_file)

    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    def _release():
        try:
            os.remove(lock_file)
        except OSError:
            pass

    atexit.register(_release)
    signal.signal(signal.SIGTERM, lambda s, f: (_release(), sys.exit(1)))
    return True


# ── Exa search ─────────────────────────────────────────────────────────────────
def exa_search(query, num_results=10, start_date=None, log=None):
    """Query Exa neural search API. Returns parsed JSON or None on error.

    Args:
        query:       Search string.
        num_results: Number of results to request (default 10).
        start_date:  ISO 8601 string for earliest publication date, or None for no filter.
        log:         Optional log function for error reporting.
    """
    payload = {
        "query": query,
        "numResults": num_results,
        "type": "auto",
        "contents": {"text": {"maxCharacters": 2000}},
    }
    if start_date:
        payload["startPublishedDate"] = start_date

    req = urllib.request.Request(
        "https://api.exa.ai/search",
        data=json.dumps(payload).encode(),
        method="POST",
    )
    req.add_header("x-api-key", EXA_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", _UA)

    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        if log:
            log(f"  Exa error: {e}")
        return None


# ── HTML page fetch ────────────────────────────────────────────────────────────
def fetch_html_text(url, timeout=15, user_agent=None, max_chars=4000,
                    skip_workday=True, min_content_len=0):
    """Fetch a URL, strip HTML tags, and return plain text.

    Returns None if:
    - URL is empty or a Workday SPA (when skip_workday=True)
    - Fetch or decode fails for any reason
    - Stripped text is shorter than min_content_len (JS wall — page requires rendering)

    Args:
        url:             URL to fetch.
        timeout:         HTTP request timeout in seconds.
        user_agent:      Override User-Agent header.
        max_chars:       Truncate result to this many characters.
        skip_workday:    Return None immediately for myworkdayjobs.com URLs.
        min_content_len: Minimum acceptable text length (0 = no check).
    """
    if not url:
        return None
    if skip_workday and "myworkdayjobs.com" in url:
        return None
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", user_agent or _UA)
        req.add_header("Accept-Language", "en-CA,en;q=0.9")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode("utf-8", errors="ignore")
        html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:max_chars] if len(text) >= min_content_len else None
    except Exception:
        return None


# ── LLM extraction (local ollama) ─────────────────────────────────────────────
EXTRACT_PROMPT = """\
Extract ONE Ontario job posting from the text below.

URL: {url}
Search snippet (may be from a different but related job — DO NOT use for salary numbers): {snippet}
Page text (authoritative — use THIS for all data including salary): {page_text}

Today's date: {today}

Return ONLY valid JSON in this exact format if a valid Ontario job with explicit CAD salary range is found:
{{"role":"Job Title","company":"Company Name","min":80000,"max":120000,"location":"Toronto, ON","source_url":"{url}","posted":"YYYY-MM-DD"}}

Return ONLY the word null (no quotes, no JSON) if:
- No explicit CAD annual salary range with actual dollar numbers in the PAGE TEXT
- Not an Ontario location
- This is a salary guide / aggregator page / company careers homepage
- Hourly rate only (do NOT convert hourly to annual)
- URL is a search results page

Rules:
- min and max = annual CAD integers extracted from PAGE TEXT ONLY (e.g. 90000)
- NEVER use salary numbers from the snippet — only use page text salary data
- location must be in Ontario (Toronto, Ottawa, Waterloo, Mississauga, Hamilton, London, Brampton, Markham, Vaughan, Oakville, Kitchener, Windsor, ON)
- posted = date visible in posting, or {today} if not shown
- source_url = exact URL of this specific job posting"""


def _call_ollama(prompt):
    """Call the local ollama HTTP API. Returns response text or raises on error."""
    payload = json.dumps({
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": 256},
    }).encode()
    req = urllib.request.Request(OLLAMA_API, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read()).get("response", "").strip()


def extract_job(url, snippet, page_text, log=None):
    """Send URL + page content to LLM and extract a validated job dict.

    Returns a validated job dict, or None if no valid Ontario job with
    explicit CAD salary range is found. Retries once on ollama failure.
    """
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
                if log:
                    log(f"  Ollama attempt 1 failed ({type(e).__name__}: {e}) — retrying in 5s")
                time.sleep(5)
            else:
                if log:
                    log(f"  Ollama failed after retry: {e}")
                return None

    if not output or re.match(r'^null$', output, re.IGNORECASE):
        return None

    m = re.search(r'\{[^{}]*"role"[^{}]*\}', output, re.DOTALL)
    if not m:
        return None

    try:
        job = json.loads(m.group())
    except json.JSONDecodeError:
        return None

    for k in ("role", "company", "min", "max", "source_url"):
        if k not in job:
            return None

    try:
        val_min, val_max = int(job["min"]), int(job["max"])
    except (ValueError, TypeError):
        return None

    if not (30_000 <= val_min <= 700_000) or val_min >= val_max:
        return None

    if not any(t in job.get("location", "").lower() for t in ONTARIO_TERMS):
        return None

    # Salary ground-truth check: at least one of the extracted salary values must
    # appear in the page text. If neither appears, the LLM pulled numbers from the
    # Exa snippet (which may describe a different but related job) rather than the
    # actual page — this is the root cause of role/salary/URL mismatches.
    if page_text:
        def _in_text(val):
            # Match e.g. 116000, 116,000, $116,000, $116k, 116K
            s = str(val)
            k = str(val // 1000)
            return (
                re.search(r'[,\s$]' + s[:3], page_text) is not None  # first 3 digits prefix
                or re.search(s.replace("000", "[,.]?000"), page_text) is not None
                or re.search(rf'\b{k}[kK]\b', page_text) is not None
            )
        if not (_in_text(val_min) or _in_text(val_max)):
            if log:
                log(f"  Salary {val_min:,}–{val_max:,} not found in page text (snippet hallucination) — skip")
            return None

    return job


# ── Deduplication ─────────────────────────────────────────────────────────────
def load_existing_keys():
    """Load role|company dedup keys from jobs.json to skip already-known jobs."""
    try:
        with open(DATA_FILE) as f:
            db = json.load(f)
        return {
            f"{j['role'].lower().strip()}|{j['company'].lower().strip()}"
            for j in db.get("jobs", [])
        }
    except Exception:
        return set()


# ── Candidate URL collection ───────────────────────────────────────────────────
def collect_candidates(queries, num_results, log, start_date=None, skip=None):
    """Run Exa queries and return a deduplicated {url: snippet} dict.

    Skips URLs matching SKIP_PATTERNS (or a custom list via skip=).
    Sleeps 1.5 s between queries to stay within Exa rate limits.

    Args:
        queries:     List of Exa search query strings.
        num_results: Results to request per query.
        log:         Log function (required).
        start_date:  ISO 8601 earliest date filter, or None for no filter.
        skip:        Override URL skip list (defaults to SKIP_PATTERNS).
    """
    if skip is None:
        skip = SKIP_PATTERNS
    candidates = {}
    for i, query in enumerate(queries, 1):
        log(f"Exa [{i:2d}/{len(queries)}]: {query[:65]}...")
        resp = exa_search(query, num_results=num_results, start_date=start_date, log=log)
        if not resp:
            continue
        results = resp.get("results", [])
        log(f"  → {len(results)} results")
        for r in results:
            url = r.get("url", "").strip()
            if not url or url in candidates:
                continue
            if any(p in url for p in skip):
                continue
            candidates[url] = (r.get("text") or "")[:600]
        time.sleep(1.5)
    return candidates


# ── Output ────────────────────────────────────────────────────────────────────
def write_job(output_file, job):
    """Append a job dict as a JSON line (crash-safe — opens and closes on each write)."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "a") as f:
        f.write(json.dumps(job, ensure_ascii=False) + "\n")
