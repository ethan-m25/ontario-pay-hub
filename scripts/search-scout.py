#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/search-scout.py
Scout scraper — discovers new Ontario employers and ATS platforms.

Purpose:
  The nightly scrapers cover known employers/platforms. This script finds jobs
  we're MISSING by searching broadly, then reverse-engineers which ATS platform
  each new employer uses, and auto-adds them to the appropriate seed list.

Strategy:
  1. Run varied Exa queries targeting Ontario salary disclosures (not platform-specific)
  2. For each discovered URL: detect the underlying ATS platform from the URL pattern
  3. Compare against existing seeds across all scrapers
  4. Auto-add new employer slugs/tenants to the right script's SEED list
  5. Log jobs from unknown platforms for manual review
  6. Send Discord report with discoveries

Run: Tuesdays at 06:00 AM (after nightly pipeline + Layer 1 batch are done)
     Also safe to run manually at any time.

Outputs:
  - Auto-patches: search-workday.py, search-greenhouse.py, search-lever.py, search-ashby.py
  - Log: ~/ontario-pay-hub/scripts/scout.log
  - Discovery archive: ~/ontario-pay-hub/scripts/scout_discoveries.json
"""

import json
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import (
    make_logger, acquire_lock, load_existing_keys,
    collect_candidates, exa_search, fetch_html_text,
    TODAY, OUTPUT_FILE, write_job, is_job_page,
)

LOG_FILE      = os.path.expanduser("~/ontario-pay-hub/scripts/scout.log")
LOCK_FILE     = os.path.expanduser("~/ontario-pay-hub/scripts/.scout.lock")
DISCOVERIES_FILE = Path("~/ontario-pay-hub/scripts/scout_discoveries.json").expanduser()
SCRIPTS_DIR   = Path("~/ontario-pay-hub/scripts").expanduser()

LOOKBACK_DATE = (date.today() - timedelta(days=7)).isoformat() + "T00:00:00.000Z"

log = make_logger(LOG_FILE)

# ── Platform detection ────────────────────────────────────────────────────────
# Map URL patterns → (platform_name, key_extractor_regex)
PLATFORM_PATTERNS = [
    ("workday",     r"https?://(\w[\w-]*)\.(wd\d+)\.myworkdayjobs\.com"),
    ("workday",     r"https?://(\w[\w-]*)\.(wd\d+)\.myworkdaysite\.com"),
    ("greenhouse",  r"boards\.greenhouse\.io/([^/\?#]+)"),
    ("greenhouse",  r"job-boards\.greenhouse\.io/([^/\?#]+)"),
    ("lever",       r"jobs\.lever\.co/([^/\?#]+)"),
    ("ashby",       r"jobs\.ashby\.com/([^/\?#]+)"),
    ("icims",       r"careers\.icims\.com/jobs/\d+/([^/\?#]+)"),
    ("jobvite",     r"jobs\.jobvite\.com/([^/\?#]+)"),
    ("smartrecruiters", r"jobs\.smartrecruiters\.com/([^/\?#]+)"),
    ("taleo",       r"(?:tbe|career)\.taleo\.net/careersection/([^/\?#]+)"),
    ("breezy",      r"app\.breezy\.hr/p/([^/\?#-]+)"),
    ("jazz",        r"app\.jazz\.co/apply/([^/\?#]+)"),
    ("bamboohr",    r"([^.]+)\.bamboohr\.com/careers"),
    ("successfactors", r"([^.]+\.sapsf\.com|[^.]+\.successfactors\.com)"),
    ("rippling",    r"jobs\.rippling\.com/([^/\?#]+)"),
]


def detect_platform(url: str) -> tuple[str, str | None]:
    """Return (platform_name, key) for a job URL. key is the tenant/slug."""
    for platform, pattern in PLATFORM_PATTERNS:
        m = re.search(pattern, url, re.IGNORECASE)
        if m:
            key = m.group(1).lower().strip("/")
            return platform, key
    return "unknown", None


# ── Load existing seeds from each scraper ────────────────────────────────────
def _extract_seed_list(script_path: Path, var_name: str) -> set[str]:
    """Parse a Python list literal from a script file. Returns a set of string values."""
    if not script_path.exists():
        return set()
    text = script_path.read_text()
    # Find the variable assignment and extract quoted strings from the list
    m = re.search(rf'{var_name}\s*=\s*\[(.+?)\]', text, re.DOTALL)
    if not m:
        return set()
    block = m.group(1)
    return {s.strip().strip('"\'') for s in re.findall(r'["\']([^"\']+)["\']', block)}


def load_all_seeds() -> dict[str, set[str]]:
    """Return all known seeds/tenants per platform."""
    return {
        "workday":    _extract_seed_list(SCRIPTS_DIR / "search-workday.py",    "SEED_TENANTS"),
        "greenhouse": _extract_seed_list(SCRIPTS_DIR / "search-greenhouse.py", "SEED_SLUGS"),
        "lever":      _extract_seed_list(SCRIPTS_DIR / "search-lever.py",      "SEED_SLUGS"),
        "ashby":      _extract_seed_list(SCRIPTS_DIR / "search-ashby.py",      "SEED_SLUGS"),
    }


# ── Auto-inject new seeds into scraper scripts ────────────────────────────────
def _inject_seed(script_path: Path, var_name: str, new_key: str, comment: str = "") -> bool:
    """Insert new_key into the SEED_SLUGS / SEED_TENANTS list in a script file.

    Inserts as a new line just before the closing bracket ']' of the list.
    Returns True if successfully injected, False if already present or file issue.
    """
    if not script_path.exists():
        return False
    text = script_path.read_text()
    # Find the list block
    m = re.search(rf'({var_name}\s*=\s*\[)(.+?)(\])', text, re.DOTALL)
    if not m:
        return False
    block_content = m.group(2)
    # Already present?
    if f'"{new_key}"' in block_content or f"'{new_key}'" in block_content:
        return False
    # Find last non-empty line in block for indentation reference
    lines = block_content.rstrip().split('\n')
    last_line = lines[-1] if lines else '    '
    indent = re.match(r'^(\s*)', last_line).group(1) if last_line.strip() else '    '
    # Ensure the last item ends with comma
    stripped = block_content.rstrip()
    if stripped and not stripped.endswith(','):
        block_content = stripped + ','
    # Build new entry
    comment_str = f"  # {comment}" if comment else ""
    new_entry = f'\n{indent}"{new_key}",{comment_str}'
    new_block = block_content + new_entry + '\n'
    new_text = text[:m.start(2)] + new_block + text[m.end(2):]
    script_path.write_text(new_text)
    return True


# ── Exa queries designed to find jobs we DON'T already have ──────────────────
SCOUT_QUERIES = [
    # Broad salary search — intentionally avoids known platforms
    'Ontario Canada 2026 "salary range" "$" CAD new job posting -site:myworkdayjobs.com -site:greenhouse.io -site:lever.co -site:ashby.com analyst OR manager OR engineer OR coordinator',
    'Toronto OR Ottawa OR Waterloo OR Mississauga 2026 "compensation" "$" CAD hiring -site:glassdoor.com -site:linkedin.com -site:indeed.com site:*.com/careers OR site:*.ca/careers',

    # Catch newer Ontario employer entrants with salary disclosure
    'Ontario employer 2026 "pay range" "$" CAD job -site:glassdoor.com -site:linkedin.com engineer OR analyst OR developer OR director',
    '"Ontario" "annual salary" "$" CAD 2026 job posting -site:glassdoor.com -site:payscale.com manager OR specialist OR coordinator',

    # Target specific known-salary platforms not well-covered
    'site:jobs.jobvite.com Ontario Canada salary "$" CAD 2026',
    'site:careers.icims.com Ontario Canada salary range "$" CAD 2026',
    'site:jobs.smartrecruiters.com Ontario Canada "salary" "$" CAD 2026',

    # Find new Workday tenants (Ontario companies on Workday we haven't seen)
    'site:myworkdayjobs.com Ontario Canada "salary range" "$" CAD 2026 -rbc -bmo -td -cibc -scotiabank',

    # Healthcare / hospital sector (often missed)
    'Ontario hospital OR "health sciences centre" OR "health network" 2026 "salary range" "$" CAD job',
    '"University Health Network" OR "Sinai Health" OR "Holland Bloorview" OR "Sunnybrook" 2026 salary "$" CAD job',

    # Municipal government (City of Toronto, Hamilton, Ottawa, Brampton)
    '"City of Toronto" OR "City of Ottawa" OR "City of Hamilton" OR "City of Brampton" 2026 "salary range" "$" CAD job posting',
    'Ontario municipality 2026 annual "salary range" "$" CAD job -"Province of" manager OR analyst OR coordinator',

    # Education sector (school boards, universities)
    'Ontario "school board" OR "district school board" 2026 "salary range" "$" CAD job principal OR coordinator OR manager',
    'Ontario university OR college 2026 "salary range" "$" CAD job analyst OR manager OR director -"professor" -"faculty"',

    # Rotation: fresh random angles to catch emerging employers
    f'Ontario 2026 "starting salary" OR "base salary" "$" CAD job -site:glassdoor.com -site:salary.com engineer OR developer OR analyst',
    f'Ontario "salary: $" OR "salary range: $" 2026 job posting CAD -site:glassdoor.com -site:monster.com manager OR specialist',
]


# ── Load discovery archive ────────────────────────────────────────────────────
def load_discoveries() -> dict:
    if DISCOVERIES_FILE.exists():
        try:
            return json.loads(DISCOVERIES_FILE.read_text())
        except Exception:
            pass
    return {"runs": [], "new_seeds_added": [], "unknown_platforms": []}


def save_discoveries(data: dict):
    DISCOVERIES_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


# ── Discord notification ──────────────────────────────────────────────────────
def _send_discord(msg: str):
    webhook = "https://discord.com/api/webhooks/1496112180704051259/bGcHy1oDkDWgQVKClowYdaZCxcI4L0GoPVd4Rtqcfmp4FV2l15cLQLWrVD8ga4QmOL1A"
    import http.client, ssl
    try:
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection("discord.com", context=ctx, timeout=15)
        path = webhook.replace("https://discord.com", "")
        payload = json.dumps({"content": msg[:2000]}).encode()
        conn.request("POST", path, body=payload, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        conn.close()
        return resp.status in (200, 204)
    except Exception as e:
        log(f"  Discord error: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not acquire_lock(LOCK_FILE, log):
        return 1

    log("=== Scout scraper started ===")
    log(f"Date: {TODAY} | Lookback: last 7 days")

    existing_job_keys = load_existing_keys()
    all_seeds = load_all_seeds()
    discoveries = load_discoveries()

    log(f"Existing jobs: {len(existing_job_keys)}")
    for platform, seeds in all_seeds.items():
        log(f"  Known {platform} seeds: {len(seeds)}")

    # Run Exa queries
    candidates = collect_candidates(SCOUT_QUERIES, num_results=10, log=log, start_date=LOOKBACK_DATE)
    log(f"Unique URLs from Exa: {len(candidates)}")

    new_seeds: list[dict] = []      # auto-added to scraper seed lists
    unknown_platforms: list[dict] = []  # platforms we don't have a scraper for
    new_jobs_found = 0

    for i, (url, snippet) in enumerate(candidates.items(), 1):
        platform, key = detect_platform(url)
        log(f"[{i:3d}/{len(candidates)}] {url[:70]}")
        log(f"  platform={platform} key={key}")

        # ── Check if this is a new employer for a known platform ──────────────
        if platform in all_seeds and key:
            known = all_seeds[platform]
            if key not in known:
                # New employer on a platform we already scrape!
                script_map = {
                    "greenhouse": (SCRIPTS_DIR / "search-greenhouse.py", "SEED_SLUGS"),
                    "lever":      (SCRIPTS_DIR / "search-lever.py",      "SEED_SLUGS"),
                    "ashby":      (SCRIPTS_DIR / "search-ashby.py",      "SEED_SLUGS"),
                    "workday":    (SCRIPTS_DIR / "search-workday.py",     "SEED_TENANTS"),
                }
                if platform in script_map:
                    script_path, var_name = script_map[platform]
                    injected = _inject_seed(script_path, var_name, key,
                                           comment=f"scout {TODAY}")
                    if injected:
                        all_seeds[platform].add(key)  # avoid double-add
                        new_seeds.append({"platform": platform, "key": key, "url": url})
                        log(f"  ✓ AUTO-ADDED {key} to {platform} seed list")
                    else:
                        log(f"  (already in {platform} seeds)")
            else:
                log(f"  (known {platform} employer, skipping)")
            continue

        # ── Unknown platform — log for review and try to extract jobs ─────────
        if platform == "unknown":
            unknown_platforms.append({"url": url, "snippet": snippet[:200], "date": TODAY})

        # Try to extract the job (even from unknown platforms — writes to raw file)
        page_text = fetch_html_text(url, max_chars=4000)
        if not page_text or not is_job_page(page_text):
            log(f"  → skip (no/non-job content)")
            continue

        # Quick salary presence check before LLM
        if not any(c in page_text for c in ["$", "CAD", "salary", "Salary"]):
            log(f"  → skip (no salary signals)")
            continue

        # Defer LLM extraction — scout doesn't call ollama to keep it fast.
        # Jobs from new platforms land in unknown_platforms for manual review.
        log(f"  → queued for manual review (unknown platform)")

    # ── Persist discoveries ───────────────────────────────────────────────────
    run_summary = {
        "date": TODAY,
        "urls_scanned": len(candidates),
        "new_seeds_added": len(new_seeds),
        "unknown_platform_urls": len(unknown_platforms),
        "new_jobs_found": new_jobs_found,
        "new_seeds": new_seeds,
    }
    discoveries["runs"].append(run_summary)
    discoveries["new_seeds_added"].extend(new_seeds)
    # Keep only last 50 unknown platform entries
    discoveries["unknown_platforms"] = (discoveries["unknown_platforms"] + unknown_platforms)[-50:]
    save_discoveries(discoveries)

    # ── Discord report ────────────────────────────────────────────────────────
    msg_parts = [f"🔍 **Scout Report [{TODAY}]**"]
    msg_parts.append(f"Scanned {len(candidates)} URLs across {len(SCOUT_QUERIES)} queries")

    if new_seeds:
        by_platform: dict[str, list[str]] = {}
        for s in new_seeds:
            by_platform.setdefault(s["platform"], []).append(s["key"])
        msg_parts.append(f"\n✅ **{len(new_seeds)} new employers auto-added:**")
        for platform, keys in by_platform.items():
            msg_parts.append(f"  • {platform}: {', '.join(keys[:5])}"
                             + (f" +{len(keys)-5} more" if len(keys) > 5 else ""))
    else:
        msg_parts.append("\nNo new employer seeds found this run.")

    if unknown_platforms:
        msg_parts.append(f"\n⚠️ {len(unknown_platforms)} URLs on unrecognised platforms — check `scout_discoveries.json`")
        # Show first 3 examples
        for u in unknown_platforms[:3]:
            msg_parts.append(f"  • {u['url'][:80]}")

    _send_discord("\n".join(msg_parts))

    log(f"=== Scout complete: {len(new_seeds)} new seeds added, "
        f"{len(unknown_platforms)} unknown-platform URLs logged ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
