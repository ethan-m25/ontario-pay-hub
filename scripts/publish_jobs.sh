#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$HOME/ontario-pay-hub"
DATA_FILE="$REPO_DIR/data/jobs.json"
DISCORD_CHANNEL="channel:1476773906038919168"
TODAY="${TODAY:-$(date +%Y-%m-%d)}"
SKIP_NOTIFY="${SKIP_NOTIFY:-0}"

notify_discord() {
  local msg="$1"
  if [[ "$SKIP_NOTIFY" == "1" ]]; then
    return 0
  fi
  /Users/clawii/.npm-global/bin/openclaw message send \
    --channel discord \
    --target "$DISCORD_CHANNEL" \
    --message "$msg" 2>/dev/null || true
}

read NEW_COUNT ACTIVE_COUNT NEW_TODAY NEWLY_ARCHIVED < <(python3 -c "
import json
m = json.load(open('$DATA_FILE')).get('meta', {})
print(m.get('count',0), m.get('active',0), m.get('new_today',0), m.get('links_newly_archived',0))
" 2>/dev/null || echo "0 0 0 0")

cd "$REPO_DIR"
git add data/jobs.json
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
