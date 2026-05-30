#!/usr/bin/env bash
# setup_outreach_mcp.sh
# One-time setup: install Reddit + Gmail MCP servers for Ethan's outreach.
# Run after setting the required environment variables below.
#
# Required inputs:
#   REDDIT_CLIENT_ID     — from reddit.com/prefs/apps (Script app)
#   REDDIT_CLIENT_SECRET — from reddit.com/prefs/apps
#   REDDIT_USERNAME      — Ethan's Reddit username (without u/)
#   REDDIT_PASSWORD      — Ethan's Reddit password
#   GMAIL_CLIENT_ID      — Google Cloud Console OAuth2 Client ID
#   GMAIL_CLIENT_SECRET  — Google Cloud Console OAuth2 Client Secret
#
# Usage:
#   REDDIT_CLIENT_ID=xxx REDDIT_CLIENT_SECRET=yyy ... bash setup_outreach_mcp.sh

set -euo pipefail

SETTINGS_FILE="$HOME/.claude/settings.json"
PYTHON=/opt/homebrew/bin/python3

echo "=== Ontario Pay Hub Outreach MCP Setup ==="

# Validate required vars
for var in REDDIT_CLIENT_ID REDDIT_CLIENT_SECRET REDDIT_USERNAME REDDIT_PASSWORD GMAIL_CLIENT_ID GMAIL_CLIENT_SECRET; do
  if [[ -z "${!var:-}" ]]; then
    echo "ERROR: $var is not set"
    exit 1
  fi
done

# Install MCP packages globally
echo "Installing mcp-reddit..."
npm install -g mcp-reddit

echo "Installing gmail-mcp-server..."
npm install -g gmail-mcp-server

# Inject mcpServers into ~/.claude/settings.json
echo "Configuring Claude Code MCP servers..."
$PYTHON - <<PYEOF
import json, os

settings_path = os.path.expanduser("~/.claude/settings.json")
with open(settings_path) as f:
    s = json.load(f)

reddit_user_agent = f"mcp-reddit:1.0.0 (by /u/{os.environ['REDDIT_USERNAME']})"

s["mcpServers"] = s.get("mcpServers", {})
s["mcpServers"]["mcp-reddit"] = {
    "command": "mcp-reddit",
    "env": {
        "REDDIT_CLIENT_ID":     os.environ["REDDIT_CLIENT_ID"],
        "REDDIT_CLIENT_SECRET": os.environ["REDDIT_CLIENT_SECRET"],
        "REDDIT_USERNAME":      os.environ["REDDIT_USERNAME"],
        "REDDIT_PASSWORD":      os.environ["REDDIT_PASSWORD"],
        "REDDIT_USER_AGENT":    reddit_user_agent,
    }
}
s["mcpServers"]["gmail"] = {
    "command": "gmail-mcp-server",
    "args": [
        "--client-id",     os.environ["GMAIL_CLIENT_ID"],
        "--client-secret", os.environ["GMAIL_CLIENT_SECRET"],
    ]
}

with open(settings_path, "w") as f:
    json.dump(s, f, indent=2)

print("  settings.json updated")
PYEOF

# Add permissions for new MCP tools
$PYTHON - <<PYEOF
import json, os

settings_path = os.path.expanduser("~/.claude/settings.json")
with open(settings_path) as f:
    s = json.load(f)

new_perms = [
    "mcp__mcp-reddit__*(*)",
    "mcp__gmail__*(*)",
]
existing = s.get("permissions", {}).get("allow", [])
for p in new_perms:
    if p not in existing:
        existing.append(p)
s.setdefault("permissions", {})["allow"] = existing

with open(settings_path, "w") as f:
    json.dump(s, f, indent=2)
print("  permissions updated")
PYEOF

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next step: restart Claude Code, then first use of Gmail will open a browser"
echo "for one-time Google OAuth authorization."
echo ""
echo "Reddit is ready immediately (script-type app, no browser auth needed)."
