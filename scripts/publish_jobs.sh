#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$HOME/ontario-pay-hub"
DATA_FILE="$REPO_DIR/data/jobs.json"
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1496112180704051259/bGcHy1oDkDWgQVKClowYdaZCxcI4L0GoPVd4Rtqcfmp4FV2l15cLQLWrVD8ga4QmOL1A"
TODAY="${TODAY:-$(date +%Y-%m-%d)}"
SKIP_NOTIFY="${SKIP_NOTIFY:-1}"

notify_discord() {
  local msg="$1"
  if [[ "$SKIP_NOTIFY" == "1" ]]; then
    return 0
  fi
  # Direct webhook — does not depend on OpenClaw gateway being up
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
" <<< "$msg" || echo "[publish_jobs] Discord notify failed"
}

# Guard: never commit/push/deploy a corrupt or suspiciously-shrunk jobs.json
# (2026-09-13: a mem-fuse SIGKILL mid non-atomic-write truncated jobs.json to
# ~3k of 14.5k jobs, and this script committed+pushed+deployed it anyway,
# showing "0 tracked" live for >24h. Writers are now atomic, but this gate
# is the last line of defense against any future bad write.)
python3 -c "
import json, sys
try:
    with open('$DATA_FILE') as f:
        data = json.load(f)
except Exception as e:
    print(f'[publish_jobs] ABORT: jobs.json is not valid JSON: {e}', file=sys.stderr)
    sys.exit(1)
jobs = data.get('jobs')
if not isinstance(jobs, list) or len(jobs) < 1000:
    print(f'[publish_jobs] ABORT: jobs.json has {len(jobs) if isinstance(jobs, list) else \"no\"} jobs — refusing to publish', file=sys.stderr)
    sys.exit(1)
" || { notify_discord "🚨 Ontario Pay Hub [$TODAY]: publish ABORTED — jobs.json failed validation (corrupt or suspiciously small). Left previous published data untouched. Check pipeline.log."; exit 1; }

read NEW_COUNT ACTIVE_COUNT NEW_TODAY NEWLY_ARCHIVED < <(python3 -c "
import json
m = json.load(open('$DATA_FILE')).get('meta', {})
print(m.get('count',0), m.get('active',0), m.get('new_today',0), m.get('links_newly_archived',0))
" 2>/dev/null || echo "0 0 0 0" )

HUB_CODE="$(basename "$REPO_DIR" | sed 's/-pay-hub$//; s/^ontario$/on/')"
SYNC_FRONTEND="$HOME/shared-scripts/sync_frontend_counts.py"
if [[ -f "$SYNC_FRONTEND" ]]; then
  python3 "$SYNC_FRONTEND" --hub "$HUB_CODE" || echo "[publish] frontend count sync failed for $HUB_CODE"
fi

cd "$REPO_DIR"
git add data/jobs.json
for f in index.html insights.html skills.html compliance.html methodology.html disclaimer.html jobs.html robots.txt sitemap.xml _headers; do
  [[ -f "$f" ]] && git add "$f"
done
if git diff --cached --quiet; then
  echo "No publishable changes in data/jobs.json"
  notify_discord "ℹ️ Ontario Pay Hub [$TODAY]: no publishable changes ($NEW_COUNT total)"
  exit 0
fi

git commit -m "data: daily update $TODAY (+$NEW_TODAY new postings, $NEW_COUNT total)"
git push origin main

NEW_JOBS_LIST=""
if [ "$NEW_TODAY" -gt 0 ] 2>/dev/null; then
  NEW_JOBS_LIST=$(python3 -c "
import json
d=json.load(open('$DATA_FILE'))
jobs=d.get('jobs',[])
new_ones=[j for j in jobs if j.get('status')!='archived'][-${NEW_TODAY}:]
lines=[]
for j in new_ones:
    wm={'remote':'🏠','hybrid':'🔀','onsite':'🏢'}.get(j.get('work_mode',''),'')
    lines.append(f\"  • {j['role']} @ {j['company']} — \${j['min']:,}–\${j['max']:,} CAD {wm}\")
print('\n'.join(lines))
" 2>/dev/null || echo "")
fi

DISCORD_MSG="✅ Ontario Pay Hub updated [$TODAY]
📊 +$NEW_TODAY new | $ACTIVE_COUNT active | $NEW_COUNT total in DB
🔗 $NEWLY_ARCHIVED links newly archived (dead links detected)
🔄 Cloudflare Pages rebuilding now (~2 min)
🌐 https://ontariopayhub.fyi"

if [ -n "$NEW_JOBS_LIST" ]; then
  DISCORD_MSG="$DISCORD_MSG

🆕 New today:
$NEW_JOBS_LIST"
fi

notify_discord "$DISCORD_MSG"
echo "Published data/jobs.json to origin/main"


PORTAL_DIR="$HOME/payhub-portal"
if [[ -f "$PORTAL_DIR/scripts/update-regions.py" ]]; then
  python3 "$PORTAL_DIR/scripts/update-regions.py" || echo "[publish] main portal region sync failed"
  export PATH="/Users/clawii/.npm-global/bin:$PATH"
  npx wrangler pages deploy "$PORTAL_DIR" --project-name payhub-portal --branch main 2>&1 | tail -3 || true
fi
