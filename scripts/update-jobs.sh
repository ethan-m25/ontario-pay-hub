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
OVERRIDES_FILE="$REPO_DIR/data/manual-status-overrides.json"
CATEGORY_OVERRIDES_FILE="$REPO_DIR/data/manual-category-overrides.json"
LOG_FILE="$REPO_DIR/scripts/update.log"
DISCORD_CHANNEL="channel:1476773906038919168"
TODAY=$(date +%Y-%m-%d)
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
ALLOW_EMPTY_RAW="${ALLOW_EMPTY_RAW:-0}"
SKIP_GIT_PUBLISH="${SKIP_GIT_PUBLISH:-0}"
SKIP_NOTIFY="${SKIP_NOTIFY:-0}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

notify_discord() {
  if [[ "$SKIP_NOTIFY" == "1" ]]; then
    return 0
  fi
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
  if [[ "$ALLOW_EMPTY_RAW" == "1" ]]; then
    RAW_FILE="/tmp/ontario-pay-hub-empty-raw-$TODAY.txt"
    : > "$RAW_FILE"
    log "No raw search results found — continuing in backfill-only mode"
  else
    log "No raw search results found (checked shared/ and /tmp) — zetsu may not have run yet"
    notify_discord "⚠️ Ontario Pay Hub daily update: no raw data from zetsu. Check zetsu search cron."
    exit 1
  fi
fi

# ---- 3. Parse & merge ----
python3 - <<PYEOF
import json, os, re, urllib.request, urllib.error, html, time, sys

data_file = "$DATA_FILE"
overrides_file = "$OVERRIDES_FILE"
category_overrides_file = "$CATEGORY_OVERRIDES_FILE"
raw_file = "$RAW_FILE"
today = "$TODAY"
repo_dir = "$REPO_DIR"

sys.path.insert(0, os.path.join(repo_dir, "scripts"))

from category_classifier import (
    CATEGORY_TO_TAG,
    normalize_category as _normalize_category,
    classify_category as _classify_category_rule,
)

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
_CLASSIFY_PROMPT = """Classify this Ontario, Canada job posting.

Role: {role}
Company: {company}
Location: {location}
Salary: \${min_s} - \${max_s} CAD/year
URL: {url}

Return ONLY JSON, no other text:
{{"work_mode": "remote|hybrid|onsite|unknown", "salary_type": "base|total_comp|unknown", "category": "Engineering|Data & Analytics|Finance|Product & Project|Sales & Mktg|People & HR|Operations|Legal|IT & Infra|Leadership|Other"}}

Rules:
- work_mode: remote=fully remote; hybrid=mix of remote+office; onsite=office required; unknown=unclear
- salary_type: base=base salary only; total_comp=bundled base+equity+bonus as one figure; unknown=unclear
- category must use this taxonomy exactly:
  - Engineering: software/app/ML/AI/product engineering, QA automation, code-first builder roles
  - IT & Infra: cloud/platform/infrastructure/SRE/database/security/architecture/enterprise tools/support
  - Data & Analytics: data science/BI/analytics/reporting/research/data governance
  - Finance: banking/wealth/investment/accounting/tax/actuarial/underwriting/treasury/credit
  - Product & Project: product/program/project/release/PMO/business analysis
  - Sales & Mktg: marketing/brand/comms/growth/business development/sales/investor relations
  - People & HR: recruiting/HRBP/talent/compensation/L&D/people ops
  - Operations: operations/admin/support/client service/procurement/logistics/general business support
  - Legal: legal/compliance/privacy/regulatory/investigations
  - Leadership: org-level heads, VPs, directors with broad ownership
  - Other: only if none fit
- Canadian government/public sector → work_mode=onsite, salary_type=base
- Canadian banks (TD,BMO,RBC,CIBC,Scotiabank) → salary_type=base (bonus always separate in Canada)
- Most Canadian job postings list base salary only → default salary_type to base
- Only total_comp if range explicitly bundles base+equity together as one number"""

_PAGE_WORK_MODE_PROMPT = """Classify the work arrangement for this Ontario job posting.

Role: {role}
Company: {company}
Location: {location}
URL: {url}
Page text: {page_text}

Return ONLY valid JSON:
{{"work_mode":"remote|hybrid|onsite|unknown"}}

Rules:
- remote = fully remote / work from home / remote-first
- hybrid = split between home and office
- onsite = office/site/location presence is expected
- unknown = not clear enough from the posting
- Prefer explicit wording in the page text over assumptions from the title
- If the posting says multiple possible arrangements, choose hybrid
"""

_CATEGORY_TIEBREAKER_PROMPT = """Classify this Ontario job into exactly one category.

Role: {role}
Company: {company}
Location: {location}
Top rule candidates: {candidates}
Normalized title: {normalized_title}
Rule signals: {signals}

Return ONLY JSON:
{{"category":"Engineering|Data & Analytics|Finance|Product & Project|Sales & Mktg|People & HR|Operations|Legal|IT & Infra|Leadership|Other"}}

Rules:
- Prioritize actual job function over seniority
- Leadership only if the role is clearly senior and the function is still unclear
- Other only as a last resort
- Business Analyst defaults to Product & Project unless the title clearly says data/analytics or finance
- Risk / compliance / audit / governance / AML / controls should usually map to Legal
- Platform / cloud / security / architecture should usually map to IT & Infra
- Delivery / transformation / change / implementation should usually map to Product & Project
"""

OLLAMA_API = os.environ.get("OLLAMA_API", "http://127.0.0.1:11434/api/generate")

def _call_ollama(prompt, num_predict=128):
    payload = json.dumps({
        "model": "qwen2.5:14b",
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": num_predict},
    }).encode()
    req = urllib.request.Request(OLLAMA_API, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read()).get("response", "").strip()

def _infer_work_mode_fast(job):
    text = " ".join([
        str(job.get("role", "")),
        str(job.get("company", "")),
        str(job.get("location", "")),
        str(job.get("source_url", "")),
    ]).lower()
    if any(k in text for k in ("remote", "teletravail", "work from home", "work-from-home", "wfh", "/remote")):
        return "remote"
    if "hybrid" in text:
        return "hybrid"
    if any(k in text for k in ("onsite", "on-site", "on site", "in-office", "in office")):
        return "onsite"
    return "unknown"

def _extract_plain_text(raw_html):
    if not raw_html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', html.unescape(text)).strip()

def _fetch_page_html(url, timeout=12):
    if not url:
        return ""
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        req.add_header("Accept-Language", "en-CA,en;q=0.9")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""

def _infer_work_mode_from_text(text, url=""):
    combined = f"{text} {url}".lower()
    if any(k in combined for k in (
        "hybrid", "hybride", "flexible work model", "flexible working model",
        "partially remote", "mix of remote", "remote and in-office",
        "remote and onsite", "remote / office", "remote/office"
    )):
        return "hybrid"
    if any(k in combined for k in (
        "fully remote", "100% remote", "work from home", "work-from-home",
        "remote-first", "remote position", "remote role", "telecommute", "teletravail"
    )):
        return "remote"
    if any(k in combined for k in (
        "on-site", "onsite", "in office", "in-office", "office based",
        "must be in office", "must work on site", "must work onsite",
        "must work on-site", "position is located in", "primary work location"
    )):
        return "onsite"
    return "unknown"

def _classify_category_with_tiebreak(job):
    rule = _classify_category_rule(job)
    category = rule["predicted_category"]
    if rule["confidence_level"] != "low":
        return category, rule

    candidates = [category]
    alt = rule.get("alternative_category_candidate", "")
    if alt and alt not in candidates:
        candidates.append(alt)
    extra = [cat for cat, score in sorted(rule["scores"].items(), key=lambda item: -item[1]) if score > 0 and cat not in candidates]
    candidates.extend(extra[:2])

    prompt = _CATEGORY_TIEBREAKER_PROMPT.format(
        role=job.get("role", ""),
        company=job.get("company", ""),
        location=job.get("location", "Ontario, ON"),
        candidates=", ".join(candidates) if candidates else "Other",
        normalized_title=rule.get("normalized_title", ""),
        signals=", ".join(rule.get("matched_signals", [])[:8]) or "none",
    )
    for attempt in range(2):
        try:
            output = _call_ollama(prompt, num_predict=48)
            m = re.search(r'\{[^{}]*"category"[^{}]*\}', output)
            if not m:
                break
            d = json.loads(m.group())
            llm_cat = _normalize_category(d.get("category", category))
            if llm_cat in CATEGORY_TO_TAG:
                if llm_cat != category:
                    rule["alternative_category_candidate"] = category
                    rule["matched_signals"] = (rule.get("matched_signals", []) + [f"llm:{llm_cat}"])[:8]
                    rule["confidence_level"] = "medium" if rule["confidence_level"] == "low" else rule["confidence_level"]
                return llm_cat, rule
            break
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return category, rule

def _classify_job(job):
    fast_wm = _infer_work_mode_fast(job)
    category, rule = _classify_category_with_tiebreak(job)
    if fast_wm != "unknown":
        salary_type = str(job.get("salary_type", "unknown")).lower()
        if salary_type not in ("base", "total_comp", "unknown"):
            salary_type = "unknown"
        if salary_type == "unknown" and job.get("company", "").lower() in {"td bank", "bmo", "rbc", "cibc", "scotiabank"}:
            salary_type = "base"
        return fast_wm, salary_type, category, rule

    prompt = _CLASSIFY_PROMPT.format(
        role=job["role"], company=job["company"],
        location=job.get("location", "Ontario, ON"),
        min_s=f"{job['min']:,}", max_s=f"{job['max']:,}",
        url=job.get("source_url", "")[:80]
    )
    for attempt in range(2):
        try:
            output = _call_ollama(prompt, num_predict=128)
            m = re.search(r'\{[^{}]*"work_mode"[^{}]*\}', output)
            if m:
                d = json.loads(m.group())
                wm = d.get("work_mode", "unknown").lower()
                st = d.get("salary_type", "unknown").lower()
                cat = _normalize_category(d.get("category", category))
                if wm not in ("remote", "hybrid", "onsite", "unknown"): wm = "unknown"
                if st not in ("base", "total_comp", "unknown"): st = "unknown"
                return wm, st, cat, rule
            break
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return "unknown", "unknown", category, rule

def _classify_work_mode_from_page(job, page_text):
    if not page_text:
        return "unknown"
    prompt = _PAGE_WORK_MODE_PROMPT.format(
        role=job.get("role", ""),
        company=job.get("company", ""),
        location=job.get("location", "Ontario, ON"),
        url=job.get("source_url", "")[:160],
        page_text=page_text[:6000],
    )
    for attempt in range(2):
        try:
            output = _call_ollama(prompt, num_predict=48)
            m = re.search(r'\{[^{}]*"work_mode"[^{}]*\}', output)
            if not m:
                break
            d = json.loads(m.group())
            wm = str(d.get("work_mode", "unknown")).lower()
            if wm in ("remote", "hybrid", "onsite", "unknown"):
                return wm
            break
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return "unknown"

if new_jobs:
    print(f"Classifying {len(new_jobs)} new jobs...")
    for job in new_jobs:
        wm, st, cat, rule = _classify_job(job)
        job["work_mode"] = wm
        job["salary_type"] = st
        job["category"] = cat
        job["category_tag"] = CATEGORY_TO_TAG.get(cat, "other")
        job["category_confidence"] = rule.get("confidence_level", "low")
        job["category_signals"] = rule.get("matched_signals", [])
        job["category_alt"] = rule.get("alternative_category_candidate", "")
        job["normalized_title"] = rule.get("normalized_title", "")
        print(f"  CLASSIFY [{job['id']}] {job['role'][:35]} → category={cat} ({job['category_confidence']}) work_mode={wm} salary_type={st}")

# Ensure existing jobs have work_mode/salary_type/category fields (schema consistency)
for job in existing:
    if "work_mode" not in job:
        job["work_mode"] = "unknown"
    if "salary_type" not in job:
        job["salary_type"] = "unknown"
    if "category" not in job:
        job["category"] = "Other"
    if "category_tag" not in job:
        job["category_tag"] = "other"
    if "category_confidence" not in job:
        job["category_confidence"] = ""
    if "category_signals" not in job:
        job["category_signals"] = []
    if "category_alt" not in job:
        job["category_alt"] = ""
    if "normalized_title" not in job:
        job["normalized_title"] = ""

# ---- 3.3. Backfill work_mode for historical active jobs with unknown mode ----
BACKFILL_LIMIT = max(1, int(os.environ.get("WORK_MODE_BACKFILL_LIMIT", "120")))
backfilled_work_modes = 0
backfill_attempted = 0
backfill_candidates = [
    job for job in existing
    if job.get("status") != "archived"
    and job.get("source_url")
    and job.get("work_mode", "unknown") == "unknown"
]
backfill_cursor = int(db.get("meta", {}).get("work_mode_backfill_cursor", 0) or 0)
if backfill_candidates:
    print(f"Backfilling work_mode for up to {BACKFILL_LIMIT} active historical jobs...")
    backfill_candidates.sort(key=lambda j: int(j.get("id", 0)))
    if backfill_cursor >= len(backfill_candidates):
        backfill_cursor = 0
    batch = (
        backfill_candidates[backfill_cursor:backfill_cursor + BACKFILL_LIMIT]
        if backfill_cursor + BACKFILL_LIMIT <= len(backfill_candidates)
        else backfill_candidates[backfill_cursor:] + backfill_candidates[: (backfill_cursor + BACKFILL_LIMIT) % len(backfill_candidates)]
    )
    for job in batch:
        backfill_attempted += 1
        wm, st, cat, rule = _classify_job(job)
        if wm == "unknown":
            page_html = _fetch_page_html(job.get("source_url", ""))
            page_text = _extract_plain_text(page_html)[:12000]
            text_wm = _infer_work_mode_from_text(page_text, job.get("source_url", ""))
            if text_wm != "unknown":
                wm = text_wm
            else:
                llm_wm = _classify_work_mode_from_page(job, page_text)
                if llm_wm != "unknown":
                    wm = llm_wm
        if wm != "unknown":
            job["work_mode"] = wm
            backfilled_work_modes += 1
        if job.get("salary_type", "unknown") == "unknown" and st in ("base", "total_comp"):
            job["salary_type"] = st
        if job.get("category", "Other") in ("", "Other", None):
            job["category"] = cat
            job["category_tag"] = CATEGORY_TO_TAG.get(cat, "other")
        if not job.get("category_confidence"):
            job["category_confidence"] = rule.get("confidence_level", "")
            job["category_signals"] = rule.get("matched_signals", [])
            job["category_alt"] = rule.get("alternative_category_candidate", "")
            job["normalized_title"] = rule.get("normalized_title", "")
    backfill_cursor = (backfill_cursor + len(batch)) % len(backfill_candidates)
    print(f"  BACKFILL work_mode updated: {backfilled_work_modes} / attempted {backfill_attempted}")
else:
    backfill_cursor = 0

# Merge (append-only — existing records are NEVER deleted or overwritten)
all_jobs = existing + new_jobs

# ---- 3.3b. Category normalization + manual category overrides ----
category_overrides = []
if os.path.exists(category_overrides_file):
    try:
        with open(category_overrides_file) as f:
            category_overrides = json.load(f).get("jobs", [])
    except Exception:
        category_overrides = []

category_override_map = {
    str(row.get("id", "")).strip(): _normalize_category(row.get("category", "Other"))
    for row in category_overrides
    if str(row.get("id", "")).strip()
}

category_overrides_applied = 0
for job in all_jobs:
    override = category_override_map.get(str(job.get("id", "")).strip())
    base_rule = _classify_category_rule(job)
    if not job.get("normalized_title"):
        job["normalized_title"] = base_rule.get("normalized_title", "")
    if not job.get("category_confidence"):
        job["category_confidence"] = base_rule.get("confidence_level", "")
    if not job.get("category_signals"):
        job["category_signals"] = base_rule.get("matched_signals", [])
    if not job.get("category_alt"):
        job["category_alt"] = base_rule.get("alternative_category_candidate", "")
    if override:
        if job.get("category") != override:
            category_overrides_applied += 1
        job["category"] = override
        job["category_tag"] = CATEGORY_TO_TAG.get(override, "other")
        continue
    current = _normalize_category(job.get("category", "Other"))
    if current == "Other":
        current = base_rule.get("predicted_category", "Other")
    job["category"] = current
    job["category_tag"] = CATEGORY_TO_TAG.get(current, "other")

# ---- 3.4. Manual status overrides — human-reviewed exceptions ----
manual_overrides = []
if os.path.exists(overrides_file):
    try:
        with open(overrides_file) as f:
            manual_overrides = json.load(f).get("jobs", [])
    except Exception:
        manual_overrides = []

overrides_applied = 0
if manual_overrides:
    by_id = {str(j.get("id")): j for j in all_jobs}
    for rule in manual_overrides:
        job_id = str(rule.get("id", "")).strip()
        status = str(rule.get("status", "")).strip().lower()
        if not job_id or status not in {"active", "archived"}:
            continue
        job = by_id.get(job_id)
        if not job:
            continue
        job["status"] = status
        if status == "active":
            job["last_seen"] = today
        note = str(rule.get("reason") or rule.get("note") or "").strip()
        if note:
            job["manual_status_note"] = note
        overrides_applied += 1

# ---- 3.5. Link validation — HTTP check all active jobs ----
# Rules:
#   - Already-archived jobs: skip (preserve state, do not re-check)
#   - New jobs added this run: skip (just scraped, assume active)
#   - Workday (*.myworkdayjobs.com): often returns a 200 SPA shell even for dead links.
#     Try a GET and look for expired/not-found copy; otherwise leave as unverifiable.
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
    """Returns: 'active', 'archived', or 'skip'."""
    if not url:
        return "skip"
    try:
        if "myworkdayjobs.com" in url:
            with _fetch(url, method="GET", timeout=10) as r:
                body = r.read().decode("utf-8", errors="ignore").lower()
            dead_markers = (
                "job posting is no longer available",
                "this job is no longer available",
                "job requisition is no longer available",
                "the job has been filled",
                "page not found",
                "error 404",
                "not found"
            )
            if any(marker in body for marker in dead_markers):
                return "archived"
            return "skip"
        if "jobs.toronto.ca" in url:
            # Must check body — returns 200 even for ended postings
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
    "work_modes_backfilled": backfilled_work_modes,
    "work_modes_backfill_attempted": backfill_attempted,
    "work_mode_backfill_cursor": backfill_cursor,
    "category_overrides_applied": category_overrides_applied,
    "manual_overrides_applied": overrides_applied,
    "links_validated": val_active,
    "links_newly_archived": val_archived,
    "links_unverifiable": val_skipped
}

with open(data_file, "w") as f:
    json.dump(db, f, indent=2, ensure_ascii=False)

print(f"RESULT: added={len(new_jobs)} total={len(all_jobs)} errors={errors}")
PYEOF

# ---- 4. Read result (single python3 invocation reads all fields at once) ----
read NEW_COUNT ACTIVE_COUNT NEW_TODAY NEWLY_ARCHIVED < <(python3 -c "
import json
m = json.load(open('$DATA_FILE')).get('meta', {})
print(m.get('count',0), m.get('active',0), m.get('new_today',0), m.get('links_newly_archived',0))
" 2>/dev/null || echo "0 0 0 0")

log "Total: $NEW_COUNT | Active: $ACTIVE_COUNT | +$NEW_TODAY new | $NEWLY_ARCHIVED links newly archived"

# ---- 5. Git commit & push ----
if [[ "$SKIP_GIT_PUBLISH" == "1" ]]; then
  log "SKIP_GIT_PUBLISH=1 — merge/update complete, deferring publish to outer pipeline"
  rm -f "$RAW_FILE"
  log "=== Update complete ==="
  exit 0
fi

cd "$REPO_DIR"
bash "$REPO_DIR/scripts/publish_jobs.sh"

# ---- 6. Cleanup ----
rm -f "$RAW_FILE"
log "=== Update complete ==="
