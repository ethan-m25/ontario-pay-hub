#!/usr/bin/env python3
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
DATA_FILE = REPO / "data" / "jobs.json"
ARCHIVE_JOBS = REPO / "data" / "job-archive" / "jobs"
REVIEW_DIR = REPO / "data" / "job-archive" / "review"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path):
    with path.open() as fh:
        return json.load(fh)


def main():
    db = load_json(DATA_FILE)
    jobs = db.get("jobs", [])

    extracted = {}
    for path in ARCHIVE_JOBS.glob("*/extractions/work_mode.v1.json"):
        try:
            payload = load_json(path)
        except Exception:
            continue
        job_id = path.parents[1].name
        value = payload.get("value")
        if not value:
            continue
        extracted[job_id] = payload

    synced = 0
    changed = 0
    non_unknown = 0
    preserved_existing = 0
    unknown_review_rows = []

    for job in jobs:
        payload = extracted.get(str(job.get("id")))
        if not payload:
            continue

        old = job.get("work_mode") or "unknown"
        extracted_value = payload.get("value") or "unknown"

        # Never let a low-signal archive extraction erase an existing explicit label.
        new = extracted_value
        if extracted_value == "unknown" and old != "unknown":
            new = old
            preserved_existing += 1

        job["work_mode"] = new
        synced += 1
        if old != new:
            changed += 1
        if new != "unknown":
            non_unknown += 1
        else:
            unknown_review_rows.append({
                "id": job.get("id"),
                "role": job.get("role", ""),
                "company": job.get("company", ""),
                "category": job.get("category", ""),
                "location": job.get("location", ""),
                "status": job.get("status", ""),
                "source_url": job.get("source_url", ""),
                "confidence": payload.get("confidence", ""),
                "evidence": " | ".join(payload.get("evidence", []) or []),
                "snapshot_id": payload.get("source_snapshot_id", ""),
                "model": payload.get("model", ""),
            })

    active_jobs = [j for j in jobs if j.get("status") != "archived"]
    active_counts = Counter((j.get("work_mode") or "unknown") for j in active_jobs)

    meta = db.setdefault("meta", {})
    meta["updated"] = utc_now()
    meta["work_modes_synced_from_archive"] = synced
    meta["work_modes_changed_from_archive"] = changed
    meta["work_modes_preserved_existing_labels"] = preserved_existing
    meta["work_modes_backfilled"] = non_unknown
    meta["work_modes_unknown_after_archive_sync"] = active_counts.get("unknown", 0)
    meta["work_mode_distribution_active"] = {
        "remote": active_counts.get("remote", 0),
        "hybrid": active_counts.get("hybrid", 0),
        "onsite": active_counts.get("onsite", 0),
        "unknown": active_counts.get("unknown", 0),
    }
    meta["work_mode_archive_sync_run"] = utc_now()

    with DATA_FILE.open("w") as fh:
        json.dump(db, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REVIEW_DIR / "work-mode-unknown-after-local-backfill.csv"
    json_path = REVIEW_DIR / "work-mode-unknown-after-local-backfill.json"
    fieldnames = [
        "id",
        "role",
        "company",
        "category",
        "location",
        "status",
        "source_url",
        "confidence",
        "evidence",
        "snapshot_id",
        "model",
    ]
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unknown_review_rows)

    with json_path.open("w") as fh:
        json.dump(
            {
                "generated_at": utc_now(),
                "unknown_count": len(unknown_review_rows),
                "rows": unknown_review_rows,
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )
        fh.write("\n")

    print(
        json.dumps(
            {
                "synced": synced,
                "changed": changed,
                "preserved_existing": preserved_existing,
                "non_unknown": non_unknown,
                "active_distribution": meta["work_mode_distribution_active"],
                "unknown_review_csv": str(csv_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
