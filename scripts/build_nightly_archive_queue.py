#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
JOBS_FILE = REPO / "data" / "jobs.json"
ARCHIVE_INDEX_FILE = REPO / "data" / "job-archive" / "index.json"
ARCHIVE_JOBS_DIR = REPO / "data" / "job-archive" / "jobs"


def parse_args():
    parser = argparse.ArgumentParser(description="Build an incremental nightly archive queue.")
    parser.add_argument("--today", required=True, help="Current YYYY-MM-DD date.")
    parser.add_argument("--backlog-limit", type=int, default=25, help="How many older active unknown work_mode jobs to include.")
    parser.add_argument("--output", help="Optional path to write newline-delimited job ids.")
    return parser.parse_args()


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open() as fh:
        return json.load(fh)


def source_url_changed(job_id: str, current_url: str) -> bool:
    source_path = ARCHIVE_JOBS_DIR / str(job_id) / "source.json"
    if not source_path.exists():
        return True
    try:
        payload = load_json(source_path, {})
    except Exception:
        return True
    return (payload.get("source_url") or "") != (current_url or "")


def main():
    args = parse_args()
    db = load_json(JOBS_FILE, {"jobs": []})
    archive_index = load_json(ARCHIVE_INDEX_FILE, {"jobs": {}})

    jobs = db.get("jobs", [])
    index_jobs = archive_index.get("jobs", {})

    priority_ids = []
    backlog_ids = []
    seen = set()

    for job in jobs:
        if job.get("status") == "archived":
            continue
        job_id = str(job.get("id"))
        if not job.get("source_url"):
            continue
        meta = index_jobs.get(job_id, {})
        extraction = (meta.get("latest_extractions") or {}).get("work_mode") or {}

        needs_archive = (
            not meta
            or meta.get("latest_document_quality") != "full"
            or source_url_changed(job_id, job.get("source_url", ""))
        )

        is_new_today = job.get("scraped") == args.today
        if is_new_today or needs_archive:
            if job_id not in seen:
                priority_ids.append(job_id)
                seen.add(job_id)
            continue

        if extraction.get("value") == "unknown" and len(backlog_ids) < args.backlog_limit:
            if job_id not in seen:
                backlog_ids.append(job_id)
                seen.add(job_id)

    queue = priority_ids + backlog_ids

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(queue) + ("\n" if queue else ""))

    print(json.dumps({
        "today": args.today,
        "priority_count": len(priority_ids),
        "backlog_count": len(backlog_ids),
        "queue_count": len(queue),
        "queue": queue,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
