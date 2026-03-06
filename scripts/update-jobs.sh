#!/usr/bin/env bash
# =============================================================================
# ontario-pay-hub/scripts/update-jobs.sh
# Daily job data updater — run by kisame via OpenClaw cron
#
# Flow:
#   1. Search for new Ontario job postings with salary ranges (via zetsu/web_search)
#   2. Parse & deduplicate against existing data/jobs.json
#   3. Append new entries
#   4. Commit & push to GitHub → triggers Cloudflare Pages auto-deploy
#   5. Report to Discord #command-center
# =============================================================================

set -euo pipefail

REPO_DIR="$HOME/ontario-pay-hub"
DATA_FILE="$REPO_DIR/data/jobs.json"
LOG_FILE="$REPO_DIR/scripts/update.log"
DISCORD_CHANNEL="channel:1476773906038919168"
TODAY=$(date +%Y-%m-%d)
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

notify_discord() {
  local msg="$1"
  /Users/clawii/.npm-global/bin/openclaw message send \
    --channel discord \
    --target "$DISCORD_CHANNEL" \
    --message "$msg" 2>/dev/null || true
}

log "=== Ontario Pay Hub update started ==="

# ---- 1. Backup current data ----
cp "$DATA_FILE" "${DATA_FILE}.bak" 2>/dev/null || true
PREV_COUNT=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(len(d.get('jobs',[])))" 2>/dev/null || echo "0")
log "Previous job count: $PREV_COUNT"

# ---- 2. Search for new postings (results piped in by zetsu via stdin or temp file) ----
# zetsu writes search results to: /tmp/ontario-jobs-raw-$TODAY.txt
# Format expected: one JSON object per line:
# {"role":"...","company":"...","min":N,"max":N,"location":"...","source_url":"...","posted":"YYYY-MM-DD"}
RAW_FILE="/tmp/ontario-jobs-raw-$TODAY.txt"

if [[ ! -f "$RAW_FILE" ]]; then
  log "No raw search results found at $RAW_FILE — zetsu may not have run yet"
  notify_discord "⚠️ Ontario Pay Hub daily update: no raw data from zetsu. Check zetsu search cron."
  exit 1
fi

# ---- 3. Parse & merge ----
python3 - <<PYEOF
import json, sys, os, hashlib

data_file = "$DATA_FILE"
raw_file = "$RAW_FILE"
today = "$TODAY"

# Load existing data
with open(data_file) as f:
    db = json.load(f)

existing = db.get("jobs", [])
# Build dedup key set: role+company+posted
existing_keys = set(
    f"{j['role'].lower()}|{j['company'].lower()}|{j.get('posted','')}"
    for j in existing
)

# Determine next ID
max_id = max((int(j.get("id", 0)) for j in existing), default=0)

new_jobs = []
errors = 0

with open(raw_file) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            j = json.loads(line)
        except json.JSONDecodeError:
            errors += 1
            continue

        # Validate required fields
        required = ["role", "company", "min", "max"]
        if not all(k in j for k in required):
            errors += 1
            continue

        # Validate salary range is reasonable (CAD, Ontario)
        if not (30000 <= j["min"] <= 700000) or not (j["min"] < j["max"]):
            errors += 1
            continue

        # Validate source_url is a specific job posting page (not a career homepage)
        import re
        url = j.get("source_url", "")
        # Reject if URL is a bare career/jobs homepage (no job-specific path)
        generic_pattern = re.compile(
            r'^https?://[^/]+/(careers?|jobs?|en/careers?|en/jobs?)/?$',
            re.IGNORECASE
        )
        if not url or generic_pattern.match(url):
            errors += 1
            continue

        # Dedup check
        key = f"{j['role'].lower()}|{j['company'].lower()}|{j.get('posted', today)}"
        if key in existing_keys:
            continue

        max_id += 1
        new_entry = {
            "id": str(max_id),
            "role": j["role"],
            "company": j["company"],
            "min": int(j["min"]),
            "max": int(j["max"]),
            "location": j.get("location", "Ontario, ON"),
            "source_url": j.get("source_url", ""),
            "posted": j.get("posted", today),
            "scraped": today,
            "status": "active",
            "last_seen": today
        }
        new_jobs.append(new_entry)
        existing_keys.add(key)

# Mark existing active jobs as archived if posted > 90 days ago
from datetime import date, datetime
today_date = date.fromisoformat(today)
for job in existing:
    posted_str = job.get("posted", "")
    if posted_str:
        try:
            posted_date = date.fromisoformat(posted_str)
            age_days = (today_date - posted_date).days
            if age_days > 90 and job.get("status") != "archived":
                job["status"] = "archived"
        except ValueError:
            pass
    # Ensure status field exists on old entries
    if "status" not in job:
        job["status"] = "active"

# Merge
all_jobs = existing + new_jobs

# Update metadata
db["jobs"] = all_jobs
db["meta"] = {
    "updated": "$TIMESTAMP",
    "source": "Ontario Pay Transparency Act 2026 — public job postings",
    "count": len(all_jobs),
    "scraper_version": "1.1",
    "last_run": today,
    "new_today": len(new_jobs),
    "parse_errors": errors
}

with open(data_file, "w") as f:
    json.dump(db, f, indent=2, ensure_ascii=False)

print(f"RESULT: added={len(new_jobs)} total={len(all_jobs)} errors={errors}")
PYEOF

# ---- 4. Read result ----
RESULT=$(tail -1 "$LOG_FILE" 2>/dev/null || echo "")
NEW_COUNT=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(len(d.get('jobs',[])))" 2>/dev/null || echo "?")
NEW_TODAY=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(d.get('meta',{}).get('new_today',0))" 2>/dev/null || echo "?")

log "New count: $NEW_COUNT (+$NEW_TODAY new today)"

# ---- 5. Git commit & push ----
cd "$REPO_DIR"
git add data/jobs.json
if git diff --cached --quiet; then
  log "No changes to commit"
  notify_discord "ℹ️ Ontario Pay Hub [$TODAY]: no new postings found ($NEW_COUNT total)"
  exit 0
fi

git commit -m "data: daily update $TODAY (+$NEW_TODAY new postings, $NEW_COUNT total)"
git push origin main

log "Pushed to GitHub → Cloudflare Pages rebuilding"

# ---- 6. Discord notification ----
notify_discord "✅ Ontario Pay Hub updated [$TODAY]
📊 +$NEW_TODAY new postings | $NEW_COUNT total
🔄 Cloudflare Pages rebuilding now (~2 min)
🌐 Live at: https://ontario-pay-hub.pages.dev"

# ---- 7. Cleanup ----
rm -f "$RAW_FILE"
log "=== Update complete ==="
