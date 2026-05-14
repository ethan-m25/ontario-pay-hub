#!/usr/bin/env python3
"""
One-shot slow audit: check all active Greenhouse jobs in jobs.json
and archive any that return "no longer open" in body.

Runs sequentially with 2s delay to avoid rate-limiting.
Safe to interrupt — saves progress after every 50 checks.
"""
import json
import time
import urllib.request
import urllib.error
from datetime import date

JOBS_FILE = "/Users/clawii/ontario-pay-hub/data/jobs.json"
TODAY = date.today().isoformat()

GH_DEAD = (
    "job you are looking for is no longer open",
    "no longer accepting applications",
    "this job is no longer available",
    "position is no longer available",
    "job has been filled",
    "posting is closed",
)

def check(url: str) -> str:
    """Returns 'archived', 'active', or 'skip'."""
    req = urllib.request.Request(url, method="GET")
    req.add_header("User-Agent", "Mozilla/5.0 (compatible; OntarioPayHub-Validator/1.1)")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore").lower()
        return "archived" if any(m in body for m in GH_DEAD) else "active"
    except urllib.error.HTTPError as e:
        return "archived" if e.code == 404 else "skip"
    except Exception:
        return "skip"

with open(JOBS_FILE) as f:
    data = json.load(f)

all_jobs = data["jobs"]
candidates = [
    j for j in all_jobs
    if j.get("status", "active") == "active"
    and ("greenhouse.io" in (j.get("source_url") or "")
         or "careers.hootsuite.com" in (j.get("source_url") or ""))
]

print(f"Active Greenhouse URLs to check: {len(candidates)}")

n_archived = n_active = n_skip = 0

for i, job in enumerate(candidates, 1):
    url = job["source_url"]
    result = check(url)
    if result == "archived":
        job["status"] = "archived"
        n_archived += 1
        print(f"  [ARCHIVED] {job['role'][:50]} | {url[:60]}")
    elif result == "active":
        job["last_seen"] = TODAY
        n_active += 1
    else:
        n_skip += 1

    if i % 10 == 0:
        print(f"  {i}/{len(candidates)} — archived={n_archived} active={n_active} skip={n_skip}")

    # Save progress every 50 checks
    if i % 50 == 0:
        active_count = sum(1 for j in all_jobs if j.get("status") != "archived")
        archived_count = len(all_jobs) - active_count
        data["jobs"] = all_jobs
        data["meta"]["active"] = active_count
        data["meta"]["archived"] = archived_count
        with open(JOBS_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"  [SAVED] progress checkpoint at {i}")

    time.sleep(2)

# Final save
active_count = sum(1 for j in all_jobs if j.get("status") != "archived")
archived_count = len(all_jobs) - active_count
data["jobs"] = all_jobs
data["meta"]["active"] = active_count
data["meta"]["archived"] = archived_count
with open(JOBS_FILE, "w") as f:
    json.dump(data, f, ensure_ascii=False)

print(f"\nNewly archived: {n_archived} | Active confirmed: {n_active} | Skipped (error): {n_skip}")
print(f"Saved. Active: {active_count}, Archived: {archived_count}")
