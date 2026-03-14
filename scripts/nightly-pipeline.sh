#!/usr/bin/env bash
# =============================================================================
# ontario-pay-hub/scripts/nightly-pipeline.sh
# Nightly pipeline — called directly by crontab at 02:00 ET.
# Does NOT rely on any LLM / OpenClaw agent to orchestrate.
#
# Steps:
#   1. search-jobs.py               — Exa + ollama → Lever/Greenhouse/Jobvite/Indeed
#   2. search-workday.py            — Workday CXS API (no LLM)
#   3. search-browser.py            — Playwright + Exa + ollama → SuccessFactors/Phenom/etc.
#   4. update-jobs.sh               — dedup, classify, link-validate, merge into jobs.json
#   5. build_nightly_archive_queue  — decide which jobs need fresh local archiving
#   6. archive_job_pages.py         — archive raw html + clean text locally
#   7. archive_extract.py           — derive work_mode locally from archived pages
#   8. sync_work_modes_from_archive — sync derived work_mode back into jobs.json
#   9. publish_jobs.sh              — single git push + Discord notify
#
# Logs: ~/ontario-pay-hub/scripts/pipeline.log
# =============================================================================

set -uo pipefail

SCRIPTS_DIR="$HOME/ontario-pay-hub/scripts"
LOG_FILE="$SCRIPTS_DIR/pipeline.log"
LOCK_FILE="$SCRIPTS_DIR/.nightly-pipeline.lock"
LOCK_TTL=14400

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

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
python3 search-jobs.py >> "$LOG_FILE" 2>&1
log "Step 1 done (exit $?)"

# 2. search-workday.py (Workday CXS, pure API)
log "--- Step 2: search-workday.py ---"
python3 search-workday.py >> "$LOG_FILE" 2>&1
log "Step 2 done (exit $?)"

# 3. search-browser.py (Playwright, requires Chromium)
log "--- Step 3: search-browser.py ---"
python3 search-browser.py >> "$LOG_FILE" 2>&1
log "Step 3 done (exit $?)"

# 4. update-jobs.sh (dedup + classify + link-validate, no publish)
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
  python3 "$SCRIPTS_DIR/archive_extract.py" --field work_mode --job-ids-file "$QUEUE_FILE" --limit 9999 --force --model qwen3:4b >> "$LOG_FILE" 2>&1
  log "Step 7 done (exit $?)"
else
  log "Skipping archive/extract steps — empty queue"
fi

# 8. Sync local derived work modes back into main data file
log "--- Step 8: sync_work_modes_from_archive.py ---"
python3 "$SCRIPTS_DIR/sync_work_modes_from_archive.py" >> "$LOG_FILE" 2>&1
log "Step 8 done (exit $?)"

# 9. Publish once
log "--- Step 9: publish_jobs.sh ---"
bash "$SCRIPTS_DIR/publish_jobs.sh" >> "$LOG_FILE" 2>&1
log "Step 9 done (exit $?)"

rm -f "$QUEUE_FILE"

log "=== Nightly pipeline complete ==="
