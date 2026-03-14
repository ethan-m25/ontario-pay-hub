#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import time
import urllib.request

from archive_lib import (
    ARCHIVE_INDEX_FILE,
    ARCHIVE_JOBS_DIR,
    ARCHIVE_STATE_DIR,
    ensure_archive_dirs,
    extractions_dir,
    load_archive_index,
    load_state,
    save_state,
    snapshots_dir,
    utc_now,
    write_json,
)
from category_classifier import normalize_category


STATE_FILE = ARCHIVE_STATE_DIR / "extract-run.json"
OLLAMA_API = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "qwen3:4b"
VENV_PYTHON = "/Users/clawii/ontario-pay-hub/.venv/bin/python"


if os.path.exists(VENV_PYTHON) and os.path.realpath(sys.executable) != os.path.realpath(VENV_PYTHON):
    os.execv(VENV_PYTHON, [VENV_PYTHON] + sys.argv)


WORK_MODE_PROMPT = """Classify the work mode for this Ontario job posting.

Return ONLY JSON:
{{"work_mode":"remote|hybrid|onsite|unknown","confidence":"high|medium|low","evidence":["short quote 1","short quote 2"]}}

Rules:
- remote = clearly fully remote
- hybrid = a mix of remote and office
- onsite = office or site presence is required
- unknown = not clear enough from the text
- Prefer explicit text from the posting over assumptions from the title

Job text:
{text}
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Run derived-field extraction against archived job pages.")
    parser.add_argument("--field", default="work_mode", choices=["work_mode"], help="Field to extract.")
    parser.add_argument("--limit", type=int, default=25, help="Max archived jobs to process in this invocation.")
    parser.add_argument("--resume", action="store_true", help="Resume the previous extraction run.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model to use for low-confidence extraction.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing extraction files.")
    return parser.parse_args()


def call_ollama(model, prompt, num_predict=96):
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": num_predict},
    }).encode()
    req = urllib.request.Request(OLLAMA_API, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read()).get("response", "").strip()


def infer_work_mode_fast(text):
    lowered = text.lower()
    if any(k in lowered for k in (
        "hybrid", "remote and in-office", "remote and onsite", "in office 2-3 days",
        "flexible work model", "mix of remote", "split between home and office"
    )):
        return {"value": "hybrid", "confidence": "high", "evidence": ["explicit hybrid wording"]}
    if any(k in lowered for k in (
        "fully remote", "100% remote", "work from home", "work-from-home", "remote-first", "remote role"
    )):
        return {"value": "remote", "confidence": "high", "evidence": ["explicit remote wording"]}
    if re.search(r'(^|\W)remote(\W|$)', lowered) and "hybrid" not in lowered:
        return {"value": "remote", "confidence": "medium", "evidence": ["standalone remote label in posting text"]}
    if any(k in lowered for k in (
        "onsite", "on-site", "on site", "in office", "in-office", "office based",
        "must be in office", "primary work location"
    )):
        return {"value": "onsite", "confidence": "high", "evidence": ["explicit onsite wording"]}
    return {"value": "unknown", "confidence": "low", "evidence": []}


def latest_snapshot_text(job_id, snapshot_id):
    path = snapshots_dir(job_id) / snapshot_id / "clean.txt"
    if not path.exists():
        return ""
    return path.read_text()


def extract_work_mode(job_id, snapshot_id, text, model):
    rule = infer_work_mode_fast(text)
    if rule["value"] != "unknown":
        return {
            "field": "work_mode",
            "value": rule["value"],
            "confidence": rule["confidence"],
            "evidence": rule["evidence"],
            "source_snapshot_id": snapshot_id,
            "extractor_version": "work_mode.v1",
            "model": "rule-only",
            "extracted_at": utc_now(),
        }

    prompt = WORK_MODE_PROMPT.format(text=text[:8000])
    value = "unknown"
    confidence = "low"
    evidence = []
    for attempt in range(2):
        try:
            output = call_ollama(model, prompt)
            match = re.search(r'\{[^{}]*"work_mode"[^{}]*\}', output)
            if not match:
                break
            data = json.loads(match.group())
            value = str(data.get("work_mode", "unknown")).lower()
            if value not in ("remote", "hybrid", "onsite", "unknown"):
                value = "unknown"
            confidence = str(data.get("confidence", "low")).lower()
            if confidence not in ("high", "medium", "low"):
                confidence = "low"
            evidence = [str(item).strip() for item in data.get("evidence", []) if str(item).strip()][:4]
            break
        except Exception:
            if attempt == 0:
                time.sleep(2)

    return {
        "field": "work_mode",
        "value": value,
        "confidence": confidence,
        "evidence": evidence,
        "source_snapshot_id": snapshot_id,
        "extractor_version": "work_mode.v1",
        "model": model,
        "extracted_at": utc_now(),
    }


def main():
    args = parse_args()
    ensure_archive_dirs()
    index = load_archive_index()
    job_items = sorted(index.get("jobs", {}).items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else item[0])

    if args.resume:
        state = load_state(STATE_FILE, {
            "run_id": utc_now(),
            "field": args.field,
            "cursor": 0,
            "processed": 0,
            "written": 0,
            "skipped": 0,
            "queue": [job_id for job_id, meta in job_items if meta.get("latest_snapshot_id") and meta.get("latest_document_quality") == "full"],
        })
    else:
        state = {
            "run_id": utc_now(),
            "field": args.field,
            "cursor": 0,
            "processed": 0,
            "written": 0,
            "skipped": 0,
            "queue": [job_id for job_id, meta in job_items if meta.get("latest_snapshot_id") and meta.get("latest_document_quality") == "full"],
        }
        save_state(STATE_FILE, state)

    queue = state["queue"]
    cursor = int(state.get("cursor", 0))
    processed_this_run = 0

    while cursor < len(queue) and processed_this_run < args.limit:
        job_id = str(queue[cursor])
        meta = index["jobs"].get(job_id, {})
        snapshot_id = meta.get("latest_snapshot_id")
        cursor += 1
        processed_this_run += 1
        state["processed"] += 1

        if not snapshot_id:
            state["skipped"] += 1
            continue

        output_path = extractions_dir(job_id) / f"{args.field}.v1.json"
        if output_path.exists() and not args.force:
            state["skipped"] += 1
            state["cursor"] = cursor
            save_state(STATE_FILE, state)
            continue

        text = latest_snapshot_text(job_id, snapshot_id)
        if not text.strip():
            state["skipped"] += 1
            state["cursor"] = cursor
            save_state(STATE_FILE, state)
            continue

        if args.field == "work_mode":
            result = extract_work_mode(job_id, snapshot_id, text, args.model)
        else:
            raise ValueError(f"Unsupported field: {args.field}")

        write_json(output_path, result)
        meta["latest_extractions"] = meta.get("latest_extractions", {})
        meta["latest_extractions"][args.field] = {
            "version": result["extractor_version"],
            "value": result["value"],
            "confidence": result["confidence"],
            "extracted_at": result["extracted_at"],
        }
        index["jobs"][job_id] = meta
        state["written"] += 1
        state["cursor"] = cursor
        save_state(STATE_FILE, state)

    save_state(STATE_FILE, state)
    from archive_lib import save_archive_index
    save_archive_index(index)
    print(json.dumps({
        "run_id": state["run_id"],
        "field": args.field,
        "processed_total": state["processed"],
        "written_total": state["written"],
        "skipped_total": state["skipped"],
        "cursor": state["cursor"],
        "queue_size": len(queue),
        "completed": state["cursor"] >= len(queue),
    }, indent=2))


if __name__ == "__main__":
    main()
