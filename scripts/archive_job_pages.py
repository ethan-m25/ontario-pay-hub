#!/usr/bin/env python3
import argparse
import json
import os
import sys

from archive_lib import (
    ARCHIVE_INDEX_FILE,
    ARCHIVE_JOBS_DIR,
    ARCHIVE_STATE_DIR,
    build_queue,
    ensure_archive_dirs,
    existing_jobs_by_id,
    fetch_page_snapshot,
    job_dir,
    latest_snapshot_meta,
    load_archive_index,
    load_jobs,
    load_state,
    normalize_job_source,
    save_archive_index,
    save_state,
    sha256_text,
    snapshot_id,
    snapshots_dir,
    source_file,
    utc_now,
    write_json,
)


STATE_FILE = ARCHIVE_STATE_DIR / "archive-run.json"
VENV_PYTHON = "/Users/clawii/ontario-pay-hub/.venv/bin/python"


if os.path.exists(VENV_PYTHON) and os.path.realpath(sys.executable) != os.path.realpath(VENV_PYTHON):
    os.execv(VENV_PYTHON, [VENV_PYTHON] + sys.argv)


def parse_args():
    parser = argparse.ArgumentParser(description="Archive Ontario Pay Hub source pages locally.")
    parser.add_argument("--resume", action="store_true", help="Resume the last queued archive run.")
    parser.add_argument("--limit", type=int, default=25, help="Max jobs to process in this invocation.")
    parser.add_argument("--status", action="append", dest="statuses", help="Job status to include (default: active).")
    parser.add_argument("--job-id", action="append", dest="job_ids", help="Archive only specific job ids.")
    parser.add_argument("--job-ids-file", help="Optional file containing job ids (one per line or CSV with id column).")
    parser.add_argument("--force", action="store_true", help="Capture a new snapshot even if content hash is unchanged.")
    return parser.parse_args()


def should_skip_unchanged(job_id, content_hash, force):
    if force:
        return False
    latest = latest_snapshot_meta(job_id)
    return bool(latest and latest.get("content_hash") == content_hash)


def main():
    args = parse_args()
    ensure_archive_dirs()

    db = load_jobs()
    jobs_by_id = existing_jobs_by_id(db)
    statuses = tuple(args.statuses or ["active"])

    file_job_ids = []
    if args.job_ids_file:
        if args.job_ids_file.endswith(".csv"):
            import csv
            with open(args.job_ids_file, newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    job_id = str(row.get("id", "")).strip()
                    if job_id:
                        file_job_ids.append(job_id)
        else:
            with open(args.job_ids_file) as fh:
                for line in fh:
                    job_id = line.strip()
                    if job_id:
                        file_job_ids.append(job_id)

    if args.resume:
        default_state = {
            "run_id": snapshot_id(),
            "started_at": utc_now(),
            "updated_at": utc_now(),
            "statuses": list(statuses),
            "cursor": 0,
            "queue": [],
            "processed": 0,
            "captured": 0,
            "unchanged": 0,
            "shell": 0,
            "failed": 0,
        }
        state = load_state(STATE_FILE, default_state)
        if not state.get("queue"):
            state["queue"] = build_queue(db, statuses=statuses)
    else:
        explicit_ids = [str(job_id) for job_id in (args.job_ids or [])] + file_job_ids
        queue = explicit_ids or build_queue(db, statuses=statuses)
        state = {
            "run_id": snapshot_id(),
            "started_at": utc_now(),
            "updated_at": utc_now(),
            "statuses": list(statuses),
            "cursor": 0,
            "queue": queue,
            "processed": 0,
            "captured": 0,
            "unchanged": 0,
            "shell": 0,
            "failed": 0,
        }
        save_state(STATE_FILE, state)

    index = load_archive_index()
    queue = state.get("queue", [])
    cursor = int(state.get("cursor", 0))
    processed_this_run = 0

    while cursor < len(queue) and processed_this_run < args.limit:
        job_id = str(queue[cursor])
        job = jobs_by_id.get(job_id)
        cursor += 1
        processed_this_run += 1
        state["processed"] += 1

        if not job or not job.get("source_url"):
            state["failed"] += 1
            state["cursor"] = cursor
            state["updated_at"] = utc_now()
            save_state(STATE_FILE, state)
            continue

        raw_html, clean_text, fetch_meta, aux_payloads = fetch_page_snapshot(job.get("source_url", ""))
        fetch_meta["job_id"] = job_id
        fetch_meta["fetched_at"] = utc_now()
        fetch_meta["role"] = job.get("role", "")
        fetch_meta["company"] = job.get("company", "")
        fetch_meta["status"] = job.get("status", "")

        job_home = job_dir(job_id)
        write_json(source_file(job_id), normalize_job_source(job))

        if fetch_meta["fetch_status"] == "error":
            index["jobs"][job_id] = {
                "role": job.get("role", ""),
                "company": job.get("company", ""),
                "source_url": job.get("source_url", ""),
                "status": job.get("status", ""),
                "latest_fetch_status": fetch_meta["fetch_status"],
                "latest_http_status": fetch_meta["http_status"],
                "latest_fetched_at": fetch_meta["fetched_at"],
                "latest_document_quality": fetch_meta.get("document_quality", "error"),
                "snapshots_count": index.get("jobs", {}).get(job_id, {}).get("snapshots_count", 0),
            }
            state["failed"] += 1
            state["cursor"] = cursor
            state["updated_at"] = utc_now()
            save_archive_index(index)
            save_state(STATE_FILE, state)
            continue

        content_hash = sha256_text(raw_html)
        if should_skip_unchanged(job_id, content_hash, args.force):
            state["unchanged"] += 1
            current = index["jobs"].get(job_id, {})
            current.update({
                "role": job.get("role", ""),
                "company": job.get("company", ""),
                "source_url": job.get("source_url", ""),
                "status": job.get("status", ""),
                "latest_fetch_status": "unchanged",
                "latest_http_status": fetch_meta["http_status"],
                "latest_fetched_at": fetch_meta["fetched_at"],
                "latest_content_hash": content_hash,
                "latest_document_quality": fetch_meta.get("document_quality", "unknown"),
            })
            index["jobs"][job_id] = current
            state["cursor"] = cursor
            state["updated_at"] = utc_now()
            save_archive_index(index)
            save_state(STATE_FILE, state)
            continue

        snap_id = snapshot_id()
        snap_dir = snapshots_dir(job_id) / snap_id
        snap_dir.mkdir(parents=True, exist_ok=True)
        raw_path = snap_dir / "raw.html"
        text_path = snap_dir / "clean.txt"
        meta_path = snap_dir / "meta.json"

        raw_path.write_text(raw_html)
        text_path.write_text(clean_text)
        for name, payload in aux_payloads.items():
            write_json(snap_dir / name, payload)
        fetch_meta.update({
            "snapshot_id": snap_id,
            "raw_html_path": str(raw_path.relative_to(ARCHIVE_JOBS_DIR.parent)),
            "clean_text_path": str(text_path.relative_to(ARCHIVE_JOBS_DIR.parent)),
            "content_hash": content_hash,
            "text_chars": len(clean_text),
            "html_chars": len(raw_html),
            "auxiliary_files": sorted(aux_payloads.keys()),
        })
        write_json(meta_path, fetch_meta)

        current = index["jobs"].get(job_id, {})
        current.update({
            "role": job.get("role", ""),
            "company": job.get("company", ""),
            "source_url": job.get("source_url", ""),
            "status": job.get("status", ""),
            "latest_snapshot_id": snap_id,
            "latest_fetch_status": fetch_meta["fetch_status"],
            "latest_http_status": fetch_meta["http_status"],
            "latest_fetched_at": fetch_meta["fetched_at"],
            "latest_content_hash": content_hash,
            "latest_clean_text_chars": len(clean_text),
            "latest_document_quality": fetch_meta.get("document_quality", "unknown"),
            "snapshots_count": int(current.get("snapshots_count", 0)) + 1,
        })
        index["jobs"][job_id] = current
        state["captured"] += 1
        if fetch_meta["fetch_status"] == "shell":
            state["shell"] += 1
        state["cursor"] = cursor
        state["updated_at"] = utc_now()
        save_archive_index(index)
        save_state(STATE_FILE, state)

    state["completed"] = cursor >= len(queue)
    state["cursor"] = cursor
    state["updated_at"] = utc_now()
    save_state(STATE_FILE, state)

    summary = {
        "run_id": state["run_id"],
        "processed_total": state["processed"],
        "captured_total": state["captured"],
        "unchanged_total": state["unchanged"],
        "shell_total": state.get("shell", 0),
        "failed_total": state["failed"],
        "cursor": state["cursor"],
        "queue_size": len(queue),
        "completed": state["completed"],
        "archive_index": str(ARCHIVE_INDEX_FILE),
        "archive_root": str(ARCHIVE_JOBS_DIR.parent),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
