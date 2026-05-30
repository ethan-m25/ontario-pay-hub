#!/usr/bin/env python3
"""Audit job-archive numbered folders for completeness."""

import json
import os
from pathlib import Path
from datetime import datetime

JOBS_DIR = Path(__file__).parent.parent / "data" / "job-archive" / "jobs"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "job-archive" / "review"

REQUIRED_FILE = "source.json"
REQUIRED_DIRS = ["extractions", "snapshots"]


def audit_job(job_path: Path) -> dict:
    job_id = job_path.name
    result = {
        "id": job_id,
        "has_source_json": (job_path / REQUIRED_FILE).is_file(),
        "extractions_count": 0,
        "snapshots_count": 0,
        "complete": False,
        "issues": [],
    }

    for d in REQUIRED_DIRS:
        dir_path = job_path / d
        if not dir_path.is_dir():
            result["issues"].append(f"missing {d}/")
        else:
            count = len(list(dir_path.iterdir()))
            result[f"{d}_count"] = count
            if count == 0:
                result["issues"].append(f"empty {d}/")

    if not result["has_source_json"]:
        result["issues"].append("missing source.json")

    result["complete"] = len(result["issues"]) == 0
    return result


def main():
    if not JOBS_DIR.is_dir():
        print(f"ERROR: {JOBS_DIR} not found")
        return

    jobs = sorted(
        [d for d in JOBS_DIR.iterdir() if d.is_dir() and d.name.isdigit()],
        key=lambda p: int(p.name),
    )

    results = [audit_job(j) for j in jobs]

    total = len(results)
    complete = sum(1 for r in results if r["complete"])
    incomplete = total - complete

    # --- Markdown ---
    lines = [
        "# Job Archive Audit",
        f"\n_Generated: {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}_\n",
        f"**Total jobs:** {total} | **Complete:** {complete} | **Incomplete:** {incomplete}\n",
        "| Job ID | source.json | extractions | snapshots | Status | Issues |",
        "|--------|-------------|-------------|-----------|--------|--------|",
    ]

    for r in results:
        src = "✓" if r["has_source_json"] else "✗"
        ext = str(r["extractions_count"]) if r["extractions_count"] > 0 else "✗ (0)"
        snp = str(r["snapshots_count"]) if r["snapshots_count"] > 0 else "✗ (0)"
        status = "✅ complete" if r["complete"] else "❌ incomplete"
        issues = "; ".join(r["issues"]) if r["issues"] else "—"
        lines.append(f"| {r['id']} | {src} | {ext} | {snp} | {status} | {issues} |")

    md_path = OUTPUT_DIR / "audit.md"
    md_path.write_text("\n".join(lines) + "\n")
    print(f"Markdown saved → {md_path}")

    # --- JSON ---
    summary = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": total,
        "complete": complete,
        "incomplete": incomplete,
        "jobs": results,
    }
    json_path = OUTPUT_DIR / "audit.json"
    json_path.write_text(json.dumps(summary, indent=2))
    print(f"JSON saved    → {json_path}")

    # Console summary
    print(f"\nSummary: {complete}/{total} complete ({incomplete} incomplete)")
    if incomplete:
        bad = [r for r in results if not r["complete"]]
        print(f"Incomplete IDs: {[r['id'] for r in bad[:20]]}"
              + (" …" if len(bad) > 20 else ""))


if __name__ == "__main__":
    main()
