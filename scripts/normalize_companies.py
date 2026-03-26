#!/usr/bin/env python3
"""
Company name normalization for Ontario Pay Hub jobs.json
Merges duplicate/variant company names into canonical forms.
"""
import json
import re
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data" / "jobs.json"

# Normalization rules: (canonical_name, [variants to merge])
# Order matters — more specific rules first
COMPANY_RULES = [
    # ── RBC ──────────────────────────────────────────────────────────
    ("RBC", [
        "RBC",
        "RBC Dominion Securities",
        r"0+\d+ RBC Dominion Securities.*",   # strip numeric ATS prefix
        r"0+\d+ RBC Life Insurance.*",
        r"0+\d+ RBC.*",
        "RBC Life Insurance Company",
        "RBC Insurance Services Inc.",
        "RBC Investor Services Trust",
    ]),
    # ── TD Bank ───────────────────────────────────────────────────────
    ("TD Bank", [
        "TD Bank",
        "The Toronto-Dominion Bank (Canada)",
        "The Toronto-Dominion Bank",
        "Toronto-Dominion Bank",
        "TD",
    ]),
    # ── BMO / Bank of Montreal ────────────────────────────────────────
    ("Bank of Montreal (BMO)", [
        "BMO",
        "Bank of Montreal",
        "BMO Nesbitt Burns Inc.",
        "BMO Nesbitt Burns",
        "BMO Financial Group",
        "BMO Bank N.A.",
    ]),
    # ── CIBC ─────────────────────────────────────────────────────────
    ("CIBC", [
        "CIBC",
        "CIBC World Markets Inc (Canada)",
        "CIBC World Markets Inc.",
        "CIBC World Markets",
        "CIBC Asset Management Inc",
        "CIBC Asset Management",
        "CIBC Investor Services Inc",
    ]),
    # ── Brookfield (all entities) ─────────────────────────────────────
    ("Brookfield", [
        "Brookfield",
        "Brookfield Asset Management LLC",
        "Brookfield Asset Management",
        "Brookfield Asset Management Inc.",
        "Brookfield Asset Management ULC",
        "Brookfield Corporation",
        "Brookfield Investment Management (Canada) Inc.",
        r"Brookfield.*",
    ]),
    # ── PointClickCare ────────────────────────────────────────────────
    ("PointClickCare", [
        "PointClickCare",
        "Pointclickcare",
        "pointclickcare",
    ]),
    # ── OMERS ────────────────────────────────────────────────────────
    ("OMERS", [
        "OMERS",
        "OMERS Administration Corporation",
        "OMERS Administration Corp",
    ]),
    # ── Ontario Teachers' Pension Plan ───────────────────────────────
    ("Ontario Teachers' Pension Plan", [
        "Ontario Teachers' Pension Plan Board",
        "Ontario Teachers' Pension Plan",
        "OTPPB",
    ]),
    # ── Intact ───────────────────────────────────────────────────────
    ("Intact Financial Corporation", [
        "Intact",
        "Intact Financial Corporation",
    ]),
    # ── Tim Hortons / TDL Group ──────────────────────────────────────
    ("Tim Hortons", [
        "Tim Hortons",
        r"2105 The TDL Group.*",
        r"\d+ The TDL Group.*",
        r"\d+ Tim Horton.*",
        "The TDL Group Corp.",
        "TDL Group",
    ]),
    # ── Arc'teryx ────────────────────────────────────────────────────
    ("Arc'teryx", [
        "Arcteryx Com",
        "Arcteryx",
        "Arc'teryx",
    ]),
    # ── Smile Digital Health ─────────────────────────────────────────
    ("Smile Digital Health", [
        "Smiledigitalhealth",
        "Smile Digital Health",
        "smiledigitalhealth",
    ]),
]

def build_lookup(rules):
    """Build a flat dict: variant → canonical"""
    lookup = {}
    patterns = []  # list of (compiled_regex, canonical)
    # Regex pattern marker: string contains .* or \d etc.
    REGEX_MARKERS = ('.*', r'\d', r'\w', r'\s', r'\b', r'(?', r'[')
    for canonical, variants in rules:
        for v in variants:
            is_regex = any(m in v for m in REGEX_MARKERS)
            if is_regex:
                patterns.append((re.compile(r'^' + v + r'$', re.IGNORECASE), canonical))
            else:
                lookup[v.strip()] = canonical
                lookup[v.strip().lower()] = canonical
    return lookup, patterns

def normalize_company(name, lookup, patterns):
    """Return canonical company name, or the original if no rule matches."""
    name = name.strip()

    # 1. Direct lookup (exact or lower-case)
    if name in lookup:
        return lookup[name]
    if name.lower() in lookup:
        return lookup[name.lower()]

    # 2. Regex patterns
    for pattern, canonical in patterns:
        if pattern.match(name):
            return canonical

    # 3. Strip leading numeric ATS prefixes like "0000012345 CompanyName"
    stripped = re.sub(r'^\d{6,}\s+', '', name).strip()
    if stripped != name:
        if stripped in lookup:
            return lookup[stripped]
        if stripped.lower() in lookup:
            return lookup[stripped.lower()]
        for pattern, canonical in patterns:
            if pattern.match(stripped):
                return canonical
        # Return stripped name even if no rule — the numeric prefix is clearly noise
        return stripped

    return name

def main():
    with open(DATA_PATH) as f:
        raw = json.load(f)

    jobs = raw["jobs"]
    lookup, patterns = build_lookup(COMPANY_RULES)

    changed = 0
    name_changes = {}  # old → new for reporting

    for job in jobs:
        old = job.get("company", "")
        new = normalize_company(old, lookup, patterns)
        if new != old:
            job["company"] = new
            name_changes[old] = new
            changed += 1

    raw["jobs"] = jobs

    with open(DATA_PATH, "w") as f:
        json.dump(raw, f, separators=(',', ':'))

    print(f"Normalized {changed} job records")
    print("\nName changes applied:")
    for old, new in sorted(name_changes.items(), key=lambda x: x[0]):
        count = sum(1 for j in jobs if j.get("company") == new)
        print(f"  {old!r:50s} → {new!r}")
    print(f"\nTotal unique companies after: {len(set(j.get('company','') for j in jobs))}")

if __name__ == "__main__":
    main()
