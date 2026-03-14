#!/usr/bin/env python3
import hashlib
import html
import json
import os
import re
import socket
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


socket.setdefaulttimeout(25)

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_DIR / "data"
ARCHIVE_DIR = DATA_DIR / "job-archive"
ARCHIVE_JOBS_DIR = ARCHIVE_DIR / "jobs"
ARCHIVE_STATE_DIR = ARCHIVE_DIR / "state"
ARCHIVE_INDEX_FILE = ARCHIVE_DIR / "index.json"
JOBS_FILE = DATA_DIR / "jobs.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
VENV_PYTHON = REPO_DIR / ".venv" / "bin" / "python"


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_archive_dirs():
    ARCHIVE_JOBS_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_jobs():
    with JOBS_FILE.open() as f:
        return json.load(f)


def load_archive_index():
    if ARCHIVE_INDEX_FILE.exists():
        with ARCHIVE_INDEX_FILE.open() as f:
            return json.load(f)
    return {"generated_at": "", "jobs": {}}


def save_archive_index(index):
    index["generated_at"] = utc_now()
    ARCHIVE_INDEX_FILE.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")


def job_dir(job_id):
    return ARCHIVE_JOBS_DIR / str(job_id)


def source_file(job_id):
    return job_dir(job_id) / "source.json"


def snapshots_dir(job_id):
    return job_dir(job_id) / "snapshots"


def extractions_dir(job_id):
    return job_dir(job_id) / "extractions"


def normalize_job_source(job):
    return {
        "id": str(job.get("id", "")),
        "role": job.get("role", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "status": job.get("status", ""),
        "source_url": job.get("source_url", ""),
        "posted": job.get("posted", ""),
        "work_mode": job.get("work_mode", "unknown"),
        "category": job.get("category", "Other"),
        "category_tag": job.get("category_tag", "other"),
        "category_confidence": job.get("category_confidence", ""),
        "salary_type": job.get("salary_type", "unknown"),
        "min": job.get("min"),
        "max": job.get("max"),
    }


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def strip_html_to_text(raw_html):
    if not raw_html:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<noscript[^>]*>.*?</noscript>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<svg[^>]*>.*?</svg>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = html.unescape(text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def looks_like_shell_page(url, raw_html, clean_text):
    lowered_url = (url or "").lower()
    lowered_html = (raw_html or "").lower()
    if not raw_html.strip():
        return True
    if "myworkdayjobs.com" in lowered_url and len(clean_text.strip()) < 200:
        return True
    if len(clean_text.strip()) < 120 and any(token in lowered_html for token in (
        "window.workday",
        "application properties",
        "clientorigin",
        "cdnendpoint",
    )):
        return True
    return False


def classify_document_quality(text):
    lowered = (text or "").lower()
    if not lowered.strip():
        return "empty"
    if "the page you are looking for doesn't exist" in lowered:
        return "missing"
    if "click from the options below to either decline or accept" in lowered and "apply for this job" not in lowered:
        return "cookie-shell"
    if len(lowered.strip()) < 200:
        return "thin"
    return "full"


def render_page_snapshot(url, timeout=20):
    if not VENV_PYTHON.exists():
        return "", "", {"fetch_mode": "rendered", "fetch_status": "no-renderer", "error": "venv-python-missing"}
    script = """
import json, sys
from playwright.sync_api import sync_playwright
url = sys.argv[1]
timeout_ms = int(sys.argv[2]) * 1000
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale="en-CA",
        viewport={"width": 1440, "height": 1200},
        ignore_https_errors=True,
    )
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass
    body_text = page.inner_text("body") if page.locator("body").count() else ""
    payload = {
        "final_url": page.url,
        "html": page.content(),
        "text": body_text,
    }
    print(json.dumps(payload))
    browser.close()
"""
    started = time.time()
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), "-c", script, url, str(timeout)],
            capture_output=True,
            text=True,
            timeout=timeout + 20,
            check=True,
        )
        payload = json.loads(result.stdout)
        meta = {
            "fetch_mode": "rendered",
            "fetch_status": "ok",
            "final_url": payload.get("final_url", url),
            "http_status": 200,
            "content_type": "text/html",
            "error": "",
            "elapsed_ms": int((time.time() - started) * 1000),
        }
        rendered_html = payload.get("html", "")
        rendered_text = payload.get("text", "")
        quality = classify_document_quality(rendered_text)
        meta["document_quality"] = quality
        if quality != "full":
            meta["fetch_status"] = "shell"
        return rendered_html, rendered_text, meta
    except Exception as exc:
        return "", "", {
            "fetch_mode": "rendered",
            "fetch_status": "render-error",
            "final_url": url,
            "http_status": None,
            "content_type": "",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": int((time.time() - started) * 1000),
        }


def fetch_page_snapshot(url, timeout=20):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", UA)
    req.add_header("Accept-Language", "en-CA,en;q=0.9")
    started = time.time()
    meta = {
        "fetch_mode": "http",
        "requested_url": url,
        "final_url": url,
        "http_status": None,
        "fetch_status": "error",
        "error": "",
        "content_type": "",
        "elapsed_ms": 0,
    }
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw_bytes = response.read()
            raw_html = raw_bytes.decode("utf-8", errors="ignore")
            meta["http_status"] = getattr(response, "status", 200)
            meta["final_url"] = response.geturl()
            meta["content_type"] = response.headers.get("Content-Type", "")
            clean_text = strip_html_to_text(raw_html)
            if looks_like_shell_page(url, raw_html, clean_text):
                rendered_html, rendered_text, rendered_meta = render_page_snapshot(url, timeout=timeout)
                if rendered_html:
                    rendered_meta["requested_url"] = url
                    return rendered_html, rendered_text.strip(), rendered_meta
                meta["fetch_status"] = "shell"
                meta["document_quality"] = classify_document_quality(clean_text)
                meta["error"] = rendered_meta.get("error", "")
            else:
                meta["fetch_status"] = "ok"
                meta["document_quality"] = classify_document_quality(clean_text)
            meta["elapsed_ms"] = int((time.time() - started) * 1000)
            return raw_html, clean_text, meta
    except urllib.error.HTTPError as exc:
        meta["http_status"] = exc.code
        meta["error"] = str(exc)
    except Exception as exc:
        meta["error"] = f"{type(exc).__name__}: {exc}"
    meta["elapsed_ms"] = int((time.time() - started) * 1000)
    meta["document_quality"] = "error"
    return "", "", meta


def snapshot_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def latest_snapshot_meta(job_id):
    snap_root = snapshots_dir(job_id)
    if not snap_root.exists():
        return None
    candidates = sorted([p / "meta.json" for p in snap_root.iterdir() if (p / "meta.json").exists()])
    if not candidates:
        return None
    with candidates[-1].open() as f:
        return json.load(f)


def build_queue(db, statuses=("active",), require_source=True):
    jobs = db.get("jobs", [])
    queue = []
    for job in jobs:
        if statuses and job.get("status") not in statuses:
            continue
        if require_source and not job.get("source_url"):
            continue
        queue.append(str(job.get("id")))
    queue.sort(key=lambda raw: int(raw) if str(raw).isdigit() else raw)
    return queue


def load_state(path, default_payload):
    if path.exists():
        with path.open() as f:
            return json.load(f)
    return default_payload


def save_state(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def existing_jobs_by_id(db):
    return {str(job.get("id")): job for job in db.get("jobs", [])}
