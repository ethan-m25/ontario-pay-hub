#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/monitor_major_employers.py
Monitor Ontario's largest employers for pay transparency compliance changes.

Tracks two alert conditions:
  1. NEW DISCLOSURE  — employer had 0 active salary postings, now has ≥1
  2. LOW COUNT       — large employer has >0 but fewer than LOW_THRESHOLD postings
                       (suspicious for a company their size; already tracked)

State is persisted in data/major_employer_state.json so we detect changes
between nightly runs. Discord notification is sent on any status change.

Usage:
  python3 monitor_major_employers.py [--report-only] [--notify]

  --report-only    Print status table without sending Discord notification
  --notify         Force notification even when there are no changes

Run from nightly-pipeline.sh after step 8 (sync_work_modes_from_archive.py).
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTS_DIR  = Path(__file__).parent
REPO_DIR     = SCRIPTS_DIR.parent
JOBS_FILE    = REPO_DIR / "data" / "jobs.json"
STATE_FILE   = REPO_DIR / "data" / "major_employer_state.json"
TODAY        = date.today().isoformat()

# Threshold below which a non-zero count is flagged as suspiciously low
LOW_THRESHOLD = 10

# OpenClaw Discord channel for alerts
DISCORD_CHANNEL = "channel:1476773906038919168"
OPENCLAW_BIN    = "/Users/clawii/.npm-global/bin/openclaw"

# ── Major employer registry ───────────────────────────────────────────────────
# Mirrors MAJOR_EMPLOYERS in index.html; keep in sync.
# Each entry: (display_name, regex_pattern, is_federal)
# federal=True means not legally required under Ontario ESA — still worth tracking.

MAJOR_EMPLOYERS = [
    ("RBC",                   r"\brbc\b|royal bank",                              False),
    ("TD Bank",               r"\btd bank\b|\btd\s+(canada|financial)\b",         False),
    ("Scotiabank",            r"scotiabank|bank of nova scotia",                   False),
    ("BMO",                   r"\bbmo\b|bank of montreal",                         False),
    ("CIBC",                  r"\bcibc\b",                                         False),
    ("Bell Canada",           r"bell canada|bell mobility|\bbell inc\b",           True),
    ("Rogers",                r"rogers communications|rogers media",               True),
    ("Telus",                 r"\btelus\b",                                        True),
    ("Loblaws",               r"loblaws|loblaw companies",                         False),
    ("Canadian Tire",         r"canadian tire",                                    False),
    ("Metro Inc.",            r"\bmetro inc\b|\bmetro stores\b",                   False),
    ("Shopify",               r"shopify",                                          False),
    ("OpenText",              r"opentext|open text corp",                          False),
    ("Magna International",   r"magna international",                              False),
    ("Manulife",              r"manulife",                                         False),
    ("Sun Life",              r"sun life",                                         False),
    ("Hydro One",             r"hydro one",                                        False),
    ("Ontario Power Gen.",    r"ontario power generation|\bopg\b",                 False),
    ("Deloitte",              r"deloitte",                                         False),
    ("KPMG",                  r"\bkpmg\b",                                         False),
    ("SickKids",              r"sick\s*kids|hospital for sick children",           False),
    ("Sunnybrook",            r"sunnybrook",                                       False),
    ("Air Canada",            r"air canada",                                       True),
    ("Brookfield",            r"brookfield",                                       False),
    ("Intact Financial",      r"intact financial|intact insurance",                False),
    ("Ontario Teachers'",     r"ontario teachers",                                 False),
]

# Compiled regexes
_COMPILED = [(name, re.compile(pattern, re.IGNORECASE), fed)
             for name, pattern, fed in MAJOR_EMPLOYERS]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_jobs():
    with open(JOBS_FILE) as f:
        data = json.load(f)
    return data.get("jobs", [])


def count_employer_jobs(jobs):
    """Return dict: employer_name → count of active jobs with salary."""
    counts = {name: 0 for name, _, _ in MAJOR_EMPLOYERS}
    for job in jobs:
        if job.get("status") == "archived":
            continue
        company = job.get("company", "")
        min_sal = job.get("min", 0)
        max_sal = job.get("max", 0)
        if not (min_sal and max_sal):
            continue
        for name, pattern, _ in _COMPILED:
            if pattern.search(company):
                counts[name] += 1
                break  # match first employer only
    return counts


# ── State management ──────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_checked": None, "employers": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Notification ──────────────────────────────────────────────────────────────

def send_discord(message):
    try:
        subprocess.run(
            [OPENCLAW_BIN, "message", "send",
             "--channel", "discord",
             "--target", DISCORD_CHANNEL,
             "--message", message],
            capture_output=True, timeout=15
        )
    except Exception as e:
        print(f"  Discord notify failed: {e}", file=sys.stderr)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-only", action="store_true",
                        help="Print status without sending Discord notification")
    parser.add_argument("--notify", action="store_true",
                        help="Send Discord notification even if no changes")
    args = parser.parse_args()

    jobs  = load_jobs()
    state = load_state()
    prev  = state.get("employers", {})
    counts = count_employer_jobs(jobs)

    new_disclosures   = []  # employer just went from 0 → >0
    newly_low         = []  # employer went from 0 or was low, still low but now tracked
    dropped_to_zero   = []  # employer went from >0 → 0 (possible compliance rollback)
    low_count         = []  # employer has 1–9 postings (suspicious)
    not_disclosing    = []  # employer has 0 postings
    disclosing        = []  # employer has ≥LOW_THRESHOLD postings

    for name, _, federal in MAJOR_EMPLOYERS:
        current = counts.get(name, 0)
        previous = prev.get(name, {}).get("count", -1)  # -1 = never seen

        if current >= LOW_THRESHOLD:
            disclosing.append((name, current, federal))
        elif 0 < current < LOW_THRESHOLD:
            low_count.append((name, current, federal))
        else:
            not_disclosing.append((name, federal))

        # Detect changes
        if previous == -1:
            # First time we've seen this employer — no change to report
            pass
        elif previous == 0 and current > 0:
            new_disclosures.append((name, current, federal))
        elif previous > 0 and current == 0:
            dropped_to_zero.append((name, previous, federal))

    # Print status table
    print(f"\n{'='*60}")
    print(f"Major Employer Pay Transparency Monitor — {TODAY}")
    print(f"{'='*60}")

    print(f"\n✅ DISCLOSING (≥{LOW_THRESHOLD} postings): {len(disclosing)}")
    for name, count, federal in sorted(disclosing, key=lambda x: -x[1]):
        tag = " [federal]" if federal else ""
        print(f"  {name}{tag}: {count} jobs")

    print(f"\n⚠️  LOW COUNT (<{LOW_THRESHOLD} postings): {len(low_count)}")
    for name, count, federal in sorted(low_count, key=lambda x: x[1]):
        tag = " [federal]" if federal else ""
        print(f"  {name}{tag}: {count} jobs — unusual for a major employer")

    print(f"\n❌ NOT DISCLOSING (0 postings): {len(not_disclosing)}")
    for name, federal in not_disclosing:
        tag = " [federal — not legally required]" if federal else ""
        print(f"  {name}{tag}")

    # Change alerts
    if new_disclosures:
        print(f"\n🎉 NEW DISCLOSURES TODAY:")
        for name, count, federal in new_disclosures:
            print(f"  {name}: {count} postings (was 0)")

    if dropped_to_zero:
        print(f"\n🚨 DROPPED TO ZERO (possible compliance rollback):")
        for name, prev_count, federal in dropped_to_zero:
            print(f"  {name}: was {prev_count}, now 0")

    print()

    # Update state
    new_employer_state = {}
    for name, _, _ in MAJOR_EMPLOYERS:
        current = counts.get(name, 0)
        old_entry = prev.get(name, {})
        first_seen = old_entry.get("first_seen")
        if current > 0 and not first_seen:
            first_seen = TODAY
        new_employer_state[name] = {
            "count":      current,
            "first_seen": first_seen,
            "checked":    TODAY,
        }

    state["last_checked"] = TODAY
    state["employers"]    = new_employer_state
    save_state(state)

    # Discord notification
    if args.report_only:
        return 0

    if not (new_disclosures or dropped_to_zero or args.notify):
        print("No changes to report — skipping Discord notification")
        return 0

    lines = [f"📊 **Major Employer Pay Transparency Update** [{TODAY}]", ""]

    if new_disclosures:
        lines.append("🎉 **New disclosures started:**")
        for name, count, federal in new_disclosures:
            tag = " *(federal)*" if federal else ""
            lines.append(f"  • **{name}**{tag} — {count} postings now live")
        lines.append("")

    if dropped_to_zero:
        lines.append("🚨 **Dropped to zero (investigate):**")
        for name, prev_count, federal in dropped_to_zero:
            lines.append(f"  • **{name}** — was {prev_count}, now 0")
        lines.append("")

    if low_count:
        lines.append(f"⚠️ **Still low (<{LOW_THRESHOLD} postings):**")
        for name, count, federal in low_count:
            tag = " *(federal)*" if federal else ""
            lines.append(f"  • {name}{tag}: {count}")
        lines.append("")

    lines.append(f"**Full status**: {len(disclosing)} disclosing | "
                 f"{len(low_count)} low | {len(not_disclosing)} silent")

    message = "\n".join(lines)
    print("Sending Discord notification...")
    send_discord(message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
