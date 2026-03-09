#!/usr/bin/env bash
# =============================================================================
# ontario-pay-hub/scripts/nightly-pipeline.sh
# Nightly pipeline — called directly by crontab at 02:00 ET.
# Does NOT rely on any LLM / OpenClaw agent to orchestrate.
#
# Steps:
#   1. search-jobs.py      — Exa + ollama → Lever/Greenhouse/Jobvite/Indeed
#   2. search-workday.py   — Workday CXS API (no LLM)
#   3. search-browser.py   — Playwright + Exa + ollama → SuccessFactors/Phenom/etc.
#   4. update-jobs.sh      — dedup, classify, link-validate, git push, Discord notify
#
# Logs: ~/ontario-pay-hub/scripts/pipeline.log
# =============================================================================

set -uo pipefail

SCRIPTS_DIR="$HOME/ontario-pay-hub/scripts"
LOG_FILE="$SCRIPTS_DIR/pipeline.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

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

# 4. update-jobs.sh (dedup + classify + git push + Discord)
log "--- Step 4: update-jobs.sh ---"
bash "$SCRIPTS_DIR/update-jobs.sh" >> "$LOG_FILE" 2>&1
log "Step 4 done (exit $?)"

log "=== Nightly pipeline complete ==="
