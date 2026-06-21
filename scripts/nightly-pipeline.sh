#!/usr/bin/env bash
# =============================================================================
# ontario-pay-hub/scripts/nightly-pipeline.sh
# Nightly pipeline — called directly by crontab at 02:00 ET.
# Does NOT rely on any LLM / OpenClaw agent to orchestrate.
#
# Steps:
#   1. search-jobs.py               — Exa + ollama → Lever/Greenhouse/Jobvite/Indeed
#   2. search-workday.py            — Workday CXS API (no LLM)
#   2b. search-greenhouse.py        — Greenhouse boards JSON API + Scrapling (no LLM)
#   2c. search-lever.py             — Lever postings JSON API + Scrapling (no LLM)
#   2d. search-amazon.py            — Amazon Jobs JSON API (no LLM)
#   2e. search-ashby.py             — Ashby job boards (server-rendered HTML, no LLM)
#   2f. search-google.py            — Google Careers (Playwright list + Scrapling pages)
#   3. search-browser.py            — Playwright + Exa + ollama → SuccessFactors/Phenom/etc.
#   4. update-jobs.sh               — dedup, classify, link-validate, merge into jobs.json
#   5. build_nightly_archive_queue  — decide which jobs need fresh local archiving
#   6. archive_job_pages.py         — archive raw html + clean text locally
#   7. archive_extract.py           — derive work_mode locally from archived pages
#   8. sync_work_modes_from_archive — sync derived work_mode back into jobs.json
#   8b. monitor_major_employers.py  — track 26 major employers, Discord alerts
#   8c. salary_qa.py               — detect/correct wide salary ranges via LLM
#   9. publish_jobs.sh              — single git push + Discord notify
#
# Logs: ~/ontario-pay-hub/scripts/pipeline.log
# =============================================================================

set -uo pipefail

# Ensure Homebrew Python (with scrapling, exa_py, etc.) is used by cron
export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:$PATH"
# Source user env so launchd agents get EXA_API_KEY etc.
[[ -f "$HOME/.zshenv" ]] && source "$HOME/.zshenv"

SCRIPTS_DIR="$HOME/ontario-pay-hub/scripts"
LOG_FILE="$SCRIPTS_DIR/pipeline.log"
LOCK_FILE="$SCRIPTS_DIR/.nightly-pipeline.lock"
LOCK_TTL=14400

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# Wait until the system memory gate is OPEN or WARN before starting a heavy step.
# If gate stays BLOCK after 20 min, skip the step rather than hang forever.
# Returns 0 if safe to proceed, 1 if timed out (caller should skip).
check_gate() {
  local step="${1:-unknown}"
  local i
  for i in $(seq 1 20); do
    local STATUS FREE
    STATUS=$(python3 -c "
import json, sys
try:
    d = json.load(open('/tmp/payhub_mem_gate.json'))
    print(d.get('status','OPEN'))
except Exception:
    print('OPEN')
" 2>/dev/null)
    FREE=$(python3 -c "
import json, sys
try:
    d = json.load(open('/tmp/payhub_mem_gate.json'))
    print(int(d.get('free_mb',9999)))
except Exception:
    print(9999)
" 2>/dev/null)
    if [[ "$STATUS" == "OPEN" || "$STATUS" == "WARN" ]]; then
      log "[$step] gate $STATUS (free_swap=${FREE}M), proceeding"
      return 0
    fi
    log "[$step] gate BLOCK (free_swap=${FREE}M), waiting 60s ($i/20)…"
    sleep 60
  done
  log "[$step] gate still BLOCK after 20min — SKIPPING step to protect system"
  return 1
}

cleanup_lock() {
  rm -f "$LOCK_FILE"
}

acquire_lock() {
  if [[ -f "$LOCK_FILE" ]]; then
    local old_pid old_ts now age
    old_pid=$(awk 'NR==1{print $1}' "$LOCK_FILE" 2>/dev/null || true)
    old_ts=$(awk 'NR==2{print $1}' "$LOCK_FILE" 2>/dev/null || true)
    now=$(date +%s)

    if [[ -n "$old_ts" && "$old_ts" =~ ^[0-9]+$ ]]; then
      age=$((now - old_ts))
    else
      age=$((LOCK_TTL + 1))
    fi

    if [[ -n "$old_pid" && "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
      if (( age <= LOCK_TTL )); then
        log "Another instance is already running (PID $old_pid, age ${age}s). Exiting."
        return 1
      fi
      log "Stale live lock detected (PID $old_pid, age ${age}s > TTL ${LOCK_TTL}s) - removing."
    else
      log "Stale lock file detected - removing."
    fi
    rm -f "$LOCK_FILE"
  fi

  printf '%s\n%s\n' "$$" "$(date +%s)" > "$LOCK_FILE"
  trap cleanup_lock EXIT INT TERM
  return 0
}

if ! acquire_lock; then
  exit 0
fi

log "=== Nightly pipeline started ==="

# 1. search-jobs.py (Exa + ollama)
log "--- Step 1: search-jobs.py ---"
cd "$SCRIPTS_DIR"
if check_gate "step1-search-jobs"; then
  python3 search-jobs.py >> "$LOG_FILE" 2>&1
  log "Step 1 done (exit $?)"
else
  log "Step 1 SKIPPED (gate timeout)"
fi

# 2. search-workday.py (Workday CXS, pure API)
log "--- Step 2: search-workday.py ---"
python3 search-workday.py >> "$LOG_FILE" 2>&1
log "Step 2 done (exit $?)"

# 2b. search-greenhouse.py (Greenhouse boards API + Scrapling)
log "--- Step 2b: search-greenhouse.py ---"
python3 search-greenhouse.py >> "$LOG_FILE" 2>&1
log "Step 2b done (exit $?)"

# 2c. search-lever.py (Lever postings API + Scrapling)
log "--- Step 2c: search-lever.py ---"
python3 search-lever.py >> "$LOG_FILE" 2>&1
log "Step 2c done (exit $?)"

# 2d. search-amazon.py (Amazon Jobs JSON API, no auth needed)
log "--- Step 2d: search-amazon.py ---"
python3 search-amazon.py >> "$LOG_FILE" 2>&1
log "Step 2d done (exit $?)"

# 2e. search-ashby.py (Ashby server-rendered boards, no auth needed)
log "--- Step 2e: search-ashby.py ---"
python3 search-ashby.py >> "$LOG_FILE" 2>&1
log "Step 2e done (exit $?)"

# 2f. search-google.py (Google Careers, Playwright + Scrapling, no LLM)
log "--- Step 2f: search-google.py ---"
python3 search-google.py >> "$LOG_FILE" 2>&1
log "Step 2f done (exit $?)"

# 2g. search-sap.py (SAP Jobs portal, static HTML, no LLM)
log "--- Step 2g: search-sap.py ---"
python3 search-sap.py >> "$LOG_FILE" 2>&1
log "Step 2g done (exit $?)"

# 2h. search-kpmg.py (KPMG Canada, Playwright listing + Scrapling detail pages, no LLM)
log "--- Step 2h: search-kpmg.py ---"
python3 search-kpmg.py >> "$LOG_FILE" 2>&1
log "Step 2h done (exit $?)"

# 2i. search-successfactors.py (SAP SF portals: Telus, OPG, Scotiabank, Deloitte CA, EY CA)
log "--- Step 2i: search-successfactors.py ---"
python3 search-successfactors.py >> "$LOG_FILE" 2>&1
log "Step 2i done (exit $?)"

# 3. search-browser.py (Playwright + Ollama, heavy memory)
log "--- Step 3: search-browser.py ---"
if check_gate "step3-search-browser"; then
  python3 search-browser.py >> "$LOG_FILE" 2>&1
  log "Step 3 done (exit $?)"
else
  log "Step 3 SKIPPED (gate timeout)"
fi

# 4. update-jobs.sh (dedup + classify + link-validate, no publish)
# === Phase 5 classify — hub: ontario (added 2026-05-28) ===
log "--- Phase 5 classify_step ---"
python3 "$HOME/shared-scripts/region_classifier/classify_step.py" \
    --hub "ontario" --no-llm >> "$LOG_FILE" 2>&1 || true
log "classify_step done (exit $?)"

# === Phase 5.5 publish gate — hub: ontario (added 2026-05-28) ===
# Filters today's raw to classifier-claimed Ontario rows only.
# Safety belts inside apply_classifier_gate.py guarantee the raw file
# is left untouched if anything looks wrong (missing pending_jobs,
# zero matches, or drop% > 5). On refusal, gate exits non-zero —
# update-jobs.sh continues with the original raw so the day's data is
# never lost; healthcheck picks up the non-zero rc and alerts.
GATE_RAW="$HOME/.openclaw/shared/ontario-jobs-raw-$(date +%Y-%m-%d).txt"
if [[ -f "$GATE_RAW" ]]; then
    log "--- Phase 5.5 publish gate ---"
    python3 "$HOME/shared-scripts/region_classifier/apply_classifier_gate.py" \
        --hub "ontario" \
        --raw "$GATE_RAW" \
        --max-drop-pct 5.0 >> "$LOG_FILE" 2>&1
    GATE_RC=$?
    log "publish gate done (exit $GATE_RC)"
fi

log "--- Step 4: update-jobs.sh ---"
SKIP_GIT_PUBLISH=1 bash "$SCRIPTS_DIR/update-jobs.sh" >> "$LOG_FILE" 2>&1
log "Step 4 done (exit $?)"

# 5. Build archive queue
QUEUE_FILE="$SCRIPTS_DIR/nightly-archive-queue-$(date +%Y-%m-%d).txt"
log "--- Step 5: build_nightly_archive_queue.py ---"
python3 "$SCRIPTS_DIR/build_nightly_archive_queue.py" --today "$(date +%Y-%m-%d)" --backlog-limit 25 --output "$QUEUE_FILE" >> "$LOG_FILE" 2>&1
QUEUE_COUNT=$(wc -l < "$QUEUE_FILE" 2>/dev/null || echo "0")
log "Step 5 done (queue $QUEUE_COUNT jobs)"

# 6. Archive selected pages locally
if [[ "$QUEUE_COUNT" -gt 0 ]]; then
  log "--- Step 6: archive_job_pages.py ---"
  python3 "$SCRIPTS_DIR/archive_job_pages.py" --job-ids-file "$QUEUE_FILE" --limit 9999 >> "$LOG_FILE" 2>&1
  log "Step 6 done (exit $?)"

  # 7. Derive work_mode from archived pages
  log "--- Step 7: archive_extract.py (work_mode) ---"
  python3 "$HOME/shared-scripts/run_locked.py" /tmp/ollama-model.lock \
    python3 "$SCRIPTS_DIR/archive_extract.py" --field work_mode --job-ids-file "$QUEUE_FILE" --limit 9999 --force --model gemma4:12b >> "$LOG_FILE" 2>&1
  log "Step 7 done (exit $?)"

  # 7b. Layer 1 extraction (skills, summary, seniority, red_flags) for newly archived jobs
  log "--- Step 7b: dir2_layer1_batch.py ---"
  python3 "$HOME/shared-scripts/run_locked.py" /tmp/ollama-model.lock \
    python3 /Users/clawii/cc-workspace/scripts/dir2_layer1_batch.py --force-run >> "$LOG_FILE" 2>&1
  log "Step 7b done (exit $?)"
else
  log "Skipping archive/extract steps — empty queue"
fi

# 8. Sync local derived work modes back into main data file
log "--- Step 8: sync_work_modes_from_archive.py ---"
python3 "$SCRIPTS_DIR/sync_work_modes_from_archive.py" >> "$LOG_FILE" 2>&1
log "Step 8 done (exit $?)"

# 8b. Monitor major employer compliance changes
log "--- Step 8b: monitor_major_employers.py ---"
python3 "$SCRIPTS_DIR/monitor_major_employers.py" >> "$LOG_FILE" 2>&1
log "Step 8b done (exit $?)"

# 8c. Salary QA — detect and correct suspiciously wide salary ranges via LLM
log "--- Step 8c: salary_qa.py ---"
python3 "$HOME/shared-scripts/run_locked.py" /tmp/ollama-model.lock \
    python3 "$SCRIPTS_DIR/salary_qa.py" >> "$LOG_FILE" 2>&1
log "Step 8c done (exit $?)"

# 9. Publish once
# Step 8d: normalize employer names (added 2026-06-10 — ON was never hooked;
# the "ONTARIO" code-derivation bug also disabled ON regional rules until today)
log "--- Step 8d: hub_normalize_companies ---"
python3 "$HOME/shared-scripts/hub_normalize_companies.py" --hub ontario-pay-hub >> "$LOG_FILE" 2>&1
log "Step 8d done (exit $?)"

log "--- Step 9: publish_jobs.sh ---"
bash "$SCRIPTS_DIR/publish_jobs.sh" >> "$LOG_FILE" 2>&1
STEP_9_RC=$?  # Phase 2.1
log "Step 9 done (exit $STEP_9_RC)"

# 9b. Rebuild job_enrichment.json (Layer 1 + cluster context for detail panel)
log "--- Step 9b: build_job_enrichment.py ---"
python3 /Users/clawii/cc-workspace/scripts/build_job_enrichment.py >> "$LOG_FILE" 2>&1
STEP_9B_RC=$?  # Phase 2.1
if [[ $STEP_9B_RC -eq 0 ]]; then
  # Y9: embeddings + same-hub KNN neighbors baked into enrichment.
  # build_job_enrichment.py REBUILDS the file fresh — it WIPES neighbors — so
  # KNN must re-bake them before this enrichment is committed/deployed.
  # embed is best-effort and time-boxed (30min): a stuck Ollama (memory
  # pressure) must not block KNN, which is pure numpy and reuses the last-good
  # vectors when embed is stale/failed.
  # portable 30-min timeout (macOS has no `timeout`/`gtimeout` binary): run
  # embed in the background and kill it if it overruns, so a stuck Ollama can
  # never block the KNN step below.
  python3 "$HOME/shared-scripts/hub_embed_jobs.py" --hub on >> "$LOG_FILE" 2>&1 &
  EMBED_PID=$!
  EMBED_WAITED=0
  while kill -0 "$EMBED_PID" 2>/dev/null; do
    sleep 30
    EMBED_WAITED=$((EMBED_WAITED + 30))
    if [[ $EMBED_WAITED -ge 1800 ]]; then
      kill "$EMBED_PID" 2>/dev/null
      log "Y9 embed killed after 30min timeout (KNN will use last-good vectors)"
      break
    fi
  done
  wait "$EMBED_PID" 2>/dev/null || log "Y9 embed failed (KNN will use last-good vectors)"
  python3 "$HOME/shared-scripts/hub_knn_neighbors.py" --hub on >> "$LOG_FILE" 2>&1 || log "Y9 knn failed"
  cd "$HOME/ontario-pay-hub"
  # GUARD (2026-06-20): never deploy a neighbor-less enrichment over a good one.
  # If KNN was skipped/failed — e.g. a crash between build and KNN (07:53 on
  # 2026-06-20), or any KNN error — the rebuilt file has 0 neighbors. Committing
  # it would silently regress similar-role recommendations to category-distance
  # fallback site-wide. In that case, revert the working file to the last
  # deployed (neighbor-bearing) version and skip the push.
  NBR_COUNT=$(python3 -c "import json;d=json.load(open('data/job_enrichment.json'));print(sum(1 for v in d.get('jobs',{}).values() if v.get('neighbors')))" 2>/dev/null || echo 0)
  if [[ "$NBR_COUNT" -gt 0 ]]; then
    git add data/job_enrichment.json
    git diff --cached --quiet || git commit -m "data: rebuild job_enrichment.json ($(date +%Y-%m-%d), ${NBR_COUNT} neighbors)"
    git push origin main >> "$LOG_FILE" 2>&1
    log "Step 9b done — job_enrichment.json rebuilt with ${NBR_COUNT} neighbors and pushed"
  else
    git checkout -- data/job_enrichment.json 2>/dev/null || true
    log "Step 9b ABORTED COMMIT — rebuilt enrichment had 0 neighbors (KNN missed); reverted to last deployed version, live recommendations preserved"
  fi
else
  log "Step 9b FAILED — job_enrichment.json not updated"
fi

rm -f "$QUEUE_FILE"

# 9b2. Refresh main portal with ON's final-state numbers (E1, approved
# 2026-06-10). Without this, payhub.fyi's last deploy of the day (06:55
# master) predates ON's ~11:00 finish, so the flagship always showed
# yesterday's ON data on the portal.
log "--- Step 9b2: update-regions + portal deploy (E1) ---"
PORTAL_DIR="$HOME/payhub-portal"
if [[ -f "$PORTAL_DIR/scripts/update-regions.py" ]]; then
  python3 "$PORTAL_DIR/scripts/update-regions.py" >> "$LOG_FILE" 2>&1 || log "E1: update-regions failed"
  export PATH="/Users/clawii/.npm-global/bin:$PATH"
  (cd "$PORTAL_DIR" && npx wrangler pages deploy . --project-name payhub-portal --branch main 2>&1 | tail -2) >> "$LOG_FILE" 2>&1 || log "E1: portal deploy failed"
  log "Step 9b2 done"
fi

# 9c. Weekly skill salary data export (Sundays only)
# Runs AFTER build_intelligence_db (via daily discovery_engine at 9am) has already
# populated intelligence.db, so category_stats and extractions are fresh.
DOW=$(date +%u)  # 1=Mon … 7=Sun
if [[ "$DOW" == "7" ]]; then
  log "--- Step 9c: export_skill_data.py (weekly Sunday) ---"
  python3 -c "
import sys, logging
sys.path.insert(0, '/Users/clawii/cc-workspace/scripts')
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from export_skill_data import export_and_push
from pathlib import Path
ok = export_and_push(
    site_path=Path('/Users/clawii/ontario-pay-hub'),
)
sys.exit(0 if ok else 1)
" >> "$LOG_FILE" 2>&1
  log "Step 9c done (exit $?)"
fi

# ── Pipeline health check + Discord alert ────────────────────────────────────
PIPELINE_DATE=$(date +%Y-%m-%d)
read NEW_TODAY WORKDAY_FAILURES < <(python3 - "$PIPELINE_DATE" \
    "$SCRIPTS_DIR/update.log" \
    "$SCRIPTS_DIR/workday.log" << 'PYEOF'
import re, sys
date_str, update_log, workday_log = sys.argv[1], sys.argv[2], sys.argv[3]

new_today = 0
try:
    for line in open(update_log):
        if date_str in line:
            m = re.search(r'\+(\d+) new', line)
            if m:
                new_today = int(m.group(1))
except OSError:
    pass

wd_failures = 0
try:
    for line in open(workday_log):
        if date_str in line:
            m = re.search(r'api_failures=(\d+)', line)
            if m:
                wd_failures = int(m.group(1))
except OSError:
    pass

print(new_today, wd_failures)
PYEOF
)

ALERT_MSG=""
if (( NEW_TODAY < 20 )) && (( NEW_TODAY >= 0 )); then
  ALERT_MSG="⚠️ **Pipeline 异常 [$PIPELINE_DATE]**: 今天新增仅 ${NEW_TODAY} 个职位（阈值 <20）"
fi
if (( WORKDAY_FAILURES > 10 )); then
  ALERT_MSG="${ALERT_MSG:+$ALERT_MSG\n}⚠️ **Workday API 异常 [$PIPELINE_DATE]**: ${WORKDAY_FAILURES} 个 tenant 失败"
fi

if [[ -n "$ALERT_MSG" ]]; then
  DISCORD_WEBHOOK="https://discord.com/api/webhooks/1496112180704051259/bGcHy1oDkDWgQVKClowYdaZCxcI4L0GoPVd4Rtqcfmp4FV2l15cLQLWrVD8ga4QmOL1A"
  python3 -c "
import http.client, ssl, json, sys
ctx = ssl.create_default_context()
conn = http.client.HTTPSConnection('discord.com', context=ctx, timeout=15)
path = '$DISCORD_WEBHOOK'.replace('https://discord.com', '')
payload = json.dumps({'content': sys.stdin.read()}).encode()
conn.request('POST', path, body=payload, headers={'Content-Type': 'application/json'})
resp = conn.getresponse()
conn.close()
sys.exit(0 if resp.status in (200, 204) else 1)
" <<< "$(printf '%b' "$ALERT_MSG")" >> "$LOG_FILE" 2>&1 \
    && log "Health alert sent to Discord" \
    || log "Health alert Discord send failed"
fi

log "=== Nightly pipeline complete ==="


# === Phase 2 healthcheck (added 2026-05-27, polished by Phase 2.1) ===
# PUBLISH_RC is set right after the publish step (see above). PIPELINE_RC
# falls back to $? for crash/kill paths where PUBLISH_RC is unset.
# Reports daily-new shortfall + active-stock benchmark alerts via Discord;
# --pipeline-exit-code surfaces explicit non-zero exits to a 🚨🚨 PIPELINE FAILED alert.
PUBLISH_RC=$(( STEP_9_RC > STEP_9B_RC ? STEP_9_RC : STEP_9B_RC ))  # Phase 2.1
PIPELINE_RC=${PUBLISH_RC:-$?}
python3 "$HOME/shared-scripts/hub_pipeline_healthcheck.py" --failure-only \
  --hub on --pipeline-exit-code "$PIPELINE_RC" || true
exit "$PIPELINE_RC"
