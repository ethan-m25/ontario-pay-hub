#!/usr/bin/env bash
# =============================================================================
# ontario-pay-hub/scripts/update-jobs.sh
# Daily job data updater — run by kisame via OpenClaw cron
#
# DATA RETENTION RULES (enforced in code):
#   - NEVER delete or overwrite existing records
#   - ONLY append new entries; dedup by role+company+posted
#   - Archiving is ONLY via link validation (HTTP check), NOT by age
#   - Archived = link dead, but data is preserved as historical record
#   - Only a human admin may manually delete a record
#
# Flow:
#   1. Search for new Ontario job postings with salary ranges (via zetsu/web_search)
#   2. Parse & deduplicate against existing data/jobs.json
#   3. Append new entries
#   3.5. Validate links for all active jobs — archive if 404/closed
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
# zetsu writes search results to shared dir OR /tmp
# Format expected: one JSON object per line:
# {"role":"...","company":"...","min":N,"max":N,"location":"...","source_url":"...","posted":"YYYY-MM-DD"}
SHARED_RAW_FILE="$HOME/.openclaw/shared/ontario-jobs-raw-$TODAY.txt"
TMP_RAW_FILE="/tmp/ontario-jobs-raw-$TODAY.txt"

# Prefer shared/ (written by zetsu write tool), fall back to /tmp
if [[ -f "$SHARED_RAW_FILE" ]]; then
  RAW_FILE="$SHARED_RAW_FILE"
  log "Using shared raw file: $SHARED_RAW_FILE"
elif [[ -f "$TMP_RAW_FILE" ]]; then
  RAW_FILE="$TMP_RAW_FILE"
  log "Using /tmp raw file: $TMP_RAW_FILE"
else
  log "No raw search results found (checked shared/ and /tmp) — zetsu may not have run yet"
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

# Ensure status field exists on any old entries missing it
for job in existing:
    if "status" not in job:
        job["status"] = "active"

# ---- 3.2. Auto-classify work_mode + salary_type for NEW jobs via local model ----
import subprocess as _sp

_CLASSIFY_PROMPT = """Classify this Ontario, Canada job posting.

Role: {role}
Company: {company}
Location: {location}
Salary: \${min_s} - \${max_s} CAD/year
URL: {url}

Return ONLY JSON, no other text:
{{"work_mode": "remote|hybrid|onsite|unknown", "salary_type": "base|total_comp|unknown"}}

Rules:
- work_mode: remote=fully remote; hybrid=mix of remote+office; onsite=office required; unknown=unclear
- salary_type: base=base salary only; total_comp=bundled base+equity+bonus as one figure; unknown=unclear
- Canadian government/public sector → work_mode=onsite, salary_type=base
- Canadian banks (TD,BMO,RBC,CIBC,Scotiabank) → salary_type=base (bonus always separate in Canada)
- Most Canadian job postings list base salary only → default salary_type to base
- Only total_comp if range explicitly bundles base+equity together as one number"""

def _classify_new_job(job):
    prompt = _CLASSIFY_PROMPT.format(
        role=job["role"], company=job["company"],
        location=job.get("location", "Ontario, ON"),
        min_s=f"{job['min']:,}", max_s=f"{job['max']:,}",
        url=job.get("source_url", "")[:80]
    )
    try:
        r = _sp.run(["/Users/clawii/.local/bin/ollama", "run", "qwen2.5:14b"],
                    input=prompt, capture_output=True, text=True, timeout=90)
        import re as _re
        m = _re.search(r'\{[^{}]*"work_mode"[^{}]*\}', r.stdout)
        if m:
            d = json.loads(m.group())
            wm = d.get("work_mode", "unknown").lower()
            st = d.get("salary_type", "unknown").lower()
            if wm not in ("remote", "hybrid", "onsite", "unknown"): wm = "unknown"
            if st not in ("base", "total_comp", "unknown"): st = "unknown"
            return wm, st
    except Exception:
        pass
    return "unknown", "unknown"

if new_jobs:
    print(f"Classifying {len(new_jobs)} new jobs...")
    for job in new_jobs:
        wm, st = _classify_new_job(job)
        job["work_mode"] = wm
        job["salary_type"] = st
        print(f"  CLASSIFY [{job['id']}] {job['role'][:35]} → work_mode={wm} salary_type={st}")

# Ensure existing jobs have work_mode/salary_type fields (schema consistency)
for job in existing:
    if "work_mode" not in job:
        job["work_mode"] = "unknown"
    if "salary_type" not in job:
        job["salary_type"] = "unknown"

# Merge (append-only — existing records are NEVER deleted or overwritten)
all_jobs = existing + new_jobs

# ---- 3.5. Link validation — HTTP check all active jobs ----
# Rules:
#   - Already-archived jobs: skip (preserve state, do not re-check)
#   - New jobs added this run: skip (just scraped, assume active)
#   - Workday (*.myworkdayjobs.com): always returns 200 SPA shell,
#     cannot detect closed jobs without JS rendering → mark as "unverifiable"
#   - Lever (jobs.lever.co): 404 = job closed → archive
#   - Greenhouse (job-boards.greenhouse.io / boards.greenhouse.io): 404 = closed → archive
#   - jobs.toronto.ca: 200 but body contains "posting has ended" → archive
#   - Any connection error / timeout: do NOT change status (assume transient)
import urllib.request
import urllib.error

new_job_ids = {e["id"] for e in new_jobs}

def _fetch(url, method="HEAD", timeout=8):
    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", "Mozilla/5.0 (compatible; OntarioPayHub-Validator/1.1)")
    return urllib.request.urlopen(req, timeout=timeout)

def validate_url(url):
    """Returns: 'active', 'archived', or 'skip'"""
    if not url:
        return "skip"
    # Workday: unverifiable without browser JS
    if "myworkdayjobs.com" in url:
        return "skip"
    try:
        if "jobs.toronto.ca" in url:
            # Must check body — 200 even for ended postings
            with _fetch(url, method="GET", timeout=10) as r:
                body = r.read().decode("utf-8", errors="ignore").lower()
            if "posting has ended" in body or "job posting has ended" in body:
                return "archived"
            return "active"
        else:
            # Lever, Greenhouse, others: HEAD is sufficient
            with _fetch(url, method="HEAD", timeout=8) as r:
                return "active" if r.status < 400 else "archived"
    except urllib.error.HTTPError as e:
        return "archived" if e.code == 404 else "skip"
    except Exception:
        return "skip"  # Timeout / connection error — do not change status

val_active = 0
val_archived = 0
val_skipped = 0

for job in all_jobs:
    if job.get("status") == "archived":
        continue  # Already archived, never touch
    if job.get("id") in new_job_ids:
        continue  # Freshly added this run, skip validation
    result = validate_url(job.get("source_url", ""))
    if result == "active":
        job["last_seen"] = today
        val_active += 1
    elif result == "archived":
        job["status"] = "archived"
        val_archived += 1
    else:
        val_skipped += 1

print(f"VALIDATION: confirmed_active={val_active} newly_archived={val_archived} unverifiable={val_skipped}")

# Update metadata
db["jobs"] = all_jobs
active_count = sum(1 for j in all_jobs if j.get("status") != "archived")
archived_count = sum(1 for j in all_jobs if j.get("status") == "archived")

db["meta"] = {
    "updated": "$TIMESTAMP",
    "source": "Ontario Pay Transparency Act 2026 — public job postings",
    "count": len(all_jobs),
    "active": active_count,
    "archived": archived_count,
    "scraper_version": "1.2",
    "last_run": today,
    "new_today": len(new_jobs),
    "parse_errors": errors,
    "links_validated": val_active,
    "links_newly_archived": val_archived,
    "links_unverifiable": val_skipped
}

with open(data_file, "w") as f:
    json.dump(db, f, indent=2, ensure_ascii=False)

print(f"RESULT: added={len(new_jobs)} total={len(all_jobs)} errors={errors}")
PYEOF

# ---- 4. Read result ----
NEW_COUNT=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(d.get('meta',{}).get('count',0))" 2>/dev/null || echo "?")
ACTIVE_COUNT=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(d.get('meta',{}).get('active',0))" 2>/dev/null || echo "?")
NEW_TODAY=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(d.get('meta',{}).get('new_today',0))" 2>/dev/null || echo "?")
NEWLY_ARCHIVED=$(python3 -c "import json; d=json.load(open('$DATA_FILE')); print(d.get('meta',{}).get('links_newly_archived',0))" 2>/dev/null || echo "?")

log "Total: $NEW_COUNT | Active: $ACTIVE_COUNT | +$NEW_TODAY new | $NEWLY_ARCHIVED links newly archived"

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
📊 +$NEW_TODAY new | $ACTIVE_COUNT active | $NEW_COUNT total in DB
🔗 $NEWLY_ARCHIVED links newly archived (dead links detected)
🔄 Cloudflare Pages rebuilding now (~2 min)
🌐 Live at: https://ontario-pay-hub.pages.dev"

# ---- 7. Cleanup ----
rm -f "$RAW_FILE"
log "=== Update complete ==="
