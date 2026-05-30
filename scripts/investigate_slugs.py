#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/investigate_slugs.py
Investigate Greenhouse / Lever slugs that are consistently failing (0 jobs).

For each slug, uses Playwright to:
  1. Visit the company's job board URL
  2. Detect whether the page loads, redirects, or 404s
  3. Look for signs of ATS platform migration (Workday, Ashby, BambooHR, etc.)
  4. Check if jobs exist but have no Ontario/salary data
  5. Output a human-review report

Usage:
  # Investigate specific slugs
  python3 investigate_slugs.py --platform greenhouse --slugs acme,widget-corp,foo

  # Read slugs from a file (one per line)
  python3 investigate_slugs.py --platform lever --slugs-file failing-lever-slugs.txt

  # Investigate all slugs that returned 0 jobs in the last greenhouse.log
  python3 investigate_slugs.py --platform greenhouse --from-log

Output: ~/ontario-pay-hub/scripts/slug_investigation_YYYY-MM-DD.md
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTS_DIR = Path(__file__).parent
REPORT_DIR  = SCRIPTS_DIR
TODAY       = date.today().isoformat()

GH_BOARD_URL    = "https://job-boards.greenhouse.io/{slug}"
GH_API_URL      = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
LEVER_BOARD_URL = "https://jobs.lever.co/{slug}"
LEVER_API_URL   = "https://api.lever.co/v0/postings/{slug}?mode=json"

# Known ATS migration signals in page content / redirect URLs
ATS_SIGNALS = {
    "workday":       ["myworkdayjobs.com", "workday.com", "wd1.myworkdayjobs", "wd3.myworkdayjobs"],
    "ashby":         ["ashbyhq.com", "jobs.ashbyhq.com"],
    "bamboohr":      ["bamboohr.com", ".bamboohr.com/careers"],
    "icims":         ["icims.com", "careers.icims.com"],
    "successfactors":["successfactors.com", "sap.com/careers"],
    "taleo":         ["taleo.net", ".taleo.net/careersection"],
    "smartrecruiters":["smartrecruiters.com"],
    "jobvite":       ["jobvite.com"],
    "breezy":        ["breezy.hr"],
    "rippling":      ["rippling.com/jobs"],
    "teamtailor":    ["teamtailor.com"],
    "personio":      ["personio.de", "personio.com"],
    "comeet":        ["comeet.com"],
}

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "kitchener", "windsor",
    "canada", ", on",
]

SALARY_RE = re.compile(
    r'(?:[A-Z]{0,3}\s*)?\$\s*[\d,]+\s*(?:CAD)?\s*[-–—]\s*(?:[A-Z]{0,3}\s*)?\$\s*[\d,]+',
    re.IGNORECASE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def detect_ats_from_text(text_or_url):
    """Return (platform_name, evidence_string) or (None, None)."""
    s = (text_or_url or "").lower()
    for platform, signals in ATS_SIGNALS.items():
        for sig in signals:
            if sig.lower() in s:
                return platform, sig
    return None, None


def load_failing_slugs_from_log(log_path, platform):
    """Parse greenhouse.log or lever.log to find slugs that returned 'no jobs or API error'."""
    if not log_path.exists():
        return []
    slugs = []
    pattern = re.compile(r'── ([a-zA-Z0-9._-]+): no jobs or API error')
    for line in log_path.read_text().splitlines():
        m = pattern.search(line)
        if m:
            slugs.append(m.group(1))
    # Return unique, preserving order
    seen = set()
    result = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


# ── Playwright investigation ──────────────────────────────────────────────────

async def investigate_slug(page, slug, platform):
    """Investigate a single slug. Returns a dict with findings."""
    result = {
        "slug":        slug,
        "platform":    platform,
        "board_url":   "",
        "api_status":  None,
        "api_job_count": 0,
        "board_status": None,
        "final_url":   "",
        "redirected_to_ats": None,
        "ats_evidence": None,
        "ontario_jobs": 0,
        "has_salary": False,
        "verdict":     "unknown",
        "notes":       "",
    }

    if platform == "greenhouse":
        api_url   = GH_API_URL.format(slug=slug)
        board_url = GH_BOARD_URL.format(slug=slug)
    else:
        api_url   = LEVER_API_URL.format(slug=slug)
        board_url = LEVER_BOARD_URL.format(slug=slug)

    result["board_url"] = board_url

    # ── Step 1: Check API response ────────────────────────────────────────────
    try:
        api_resp = await page.goto(api_url, wait_until="domcontentloaded", timeout=20000)
        result["api_status"] = api_resp.status if api_resp else None
        if api_resp and api_resp.status == 200:
            try:
                body = await page.content()
                # Extract JSON from page source
                json_match = re.search(r'\{.*\}|\[.*\]', body, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    if platform == "greenhouse":
                        jobs = data.get("jobs", []) if isinstance(data, dict) else []
                    else:
                        jobs = data if isinstance(data, list) else []
                    result["api_job_count"] = len(jobs)
                    # Check for Ontario jobs
                    for job in jobs[:50]:  # sample first 50
                        if platform == "greenhouse":
                            loc = (job.get("location") or {}).get("name", "")
                            text = ""
                        else:
                            cats = job.get("categories") or {}
                            loc = cats.get("location", "") or ""
                            text = job.get("descriptionPlain") or ""
                        loc_lower = loc.lower()
                        if any(t in loc_lower for t in ONTARIO_TERMS) or "ontario" in text.lower():
                            result["ontario_jobs"] += 1
                            if SALARY_RE.search(text):
                                result["has_salary"] = True
            except Exception:
                pass
    except Exception as e:
        result["notes"] += f"API fetch error: {e}. "

    # ── Step 2: Visit board page in browser ───────────────────────────────────
    try:
        resp = await page.goto(board_url, wait_until="networkidle", timeout=30000)
        result["board_status"] = resp.status if resp else None
        result["final_url"]    = page.url

        # Check if we were redirected to a different ATS
        ats, evidence = detect_ats_from_text(page.url)
        if not ats:
            # Check page content for migration links
            content = await page.content()
            ats, evidence = detect_ats_from_text(content)
            if not ats:
                # Look for "apply" links pointing to other ATS
                links = await page.evaluate("""
                    () => Array.from(document.querySelectorAll('a[href]'))
                              .map(a => a.href).slice(0, 100)
                """)
                for link in links:
                    ats, evidence = detect_ats_from_text(link)
                    if ats:
                        break

        if ats:
            result["redirected_to_ats"] = ats
            result["ats_evidence"]      = evidence

        # Check page title / body text for "no jobs", "404", company gone
        title = await page.title()
        body_text = (await page.evaluate("() => document.body.innerText or ''"))[:2000]

        no_jobs_signals = [
            "no open positions", "no current openings", "no jobs",
            "we don't have any", "not found", "404", "page not found",
            "this page doesn't exist",
        ]
        company_closed = any(s in body_text.lower() for s in no_jobs_signals)

    except Exception as e:
        result["notes"] += f"Board visit error: {e}. "
        title = ""
        body_text = ""
        company_closed = False

    # ── Verdict ───────────────────────────────────────────────────────────────
    if result["redirected_to_ats"]:
        result["verdict"] = f"MIGRATED_TO_{result['redirected_to_ats'].upper()}"
        result["notes"] += f"Company moved to {result['redirected_to_ats']} (found: {result['ats_evidence']}). "
    elif result["api_status"] == 404 or result["board_status"] == 404:
        result["verdict"] = "DEAD_SLUG"
        result["notes"] += "404 on both API and board. Slug likely invalid or company left platform. "
    elif result["api_job_count"] == 0 and company_closed:
        result["verdict"] = "NO_JOBS_POSTED"
        result["notes"] += "Company is on the platform but has zero open positions. "
    elif result["api_job_count"] > 0 and result["ontario_jobs"] == 0:
        result["verdict"] = "NO_ONTARIO_JOBS"
        result["notes"] += f"{result['api_job_count']} total jobs but none in Ontario. May be worth monitoring. "
    elif result["api_job_count"] > 0 and result["ontario_jobs"] > 0 and not result["has_salary"]:
        result["verdict"] = "ONTARIO_JOBS_NO_SALARY"
        result["notes"] += f"{result['ontario_jobs']} Ontario jobs found but no salary ranges disclosed. Non-compliant. "
    elif result["api_job_count"] > 0 and result["ontario_jobs"] > 0 and result["has_salary"]:
        result["verdict"] = "SHOULD_BE_WORKING"
        result["notes"] += "Jobs with Ontario+salary found — scraper may have a bug or rate-limit issue. "
    elif result["api_status"] and result["api_status"] >= 500:
        result["verdict"] = "API_ERROR"
        result["notes"] += f"Server error {result['api_status']}. Transient — retry. "
    else:
        result["verdict"] = "NEEDS_MANUAL_REVIEW"

    return result


async def run_investigations(slugs, platform):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36",
            locale="en-CA",
        )
        page = await context.new_page()

        for i, slug in enumerate(slugs, 1):
            print(f"[{i}/{len(slugs)}] Investigating {platform}/{slug}...")
            r = await investigate_slug(page, slug, platform)
            results.append(r)
            print(f"         → {r['verdict']}  (API: {r['api_job_count']} jobs, Ontario: {r['ontario_jobs']})")
            await asyncio.sleep(2)  # polite pause

        await browser.close()

    return results


# ── Report generation ─────────────────────────────────────────────────────────

def generate_report(results, platform, report_path):
    lines = [
        f"# Slug Investigation Report — {platform.title()} — {TODAY}",
        f"",
        f"Investigated {len(results)} slugs. Generated by `investigate_slugs.py`.",
        f"",
    ]

    # Group by verdict
    by_verdict = {}
    for r in results:
        by_verdict.setdefault(r["verdict"], []).append(r)

    verdict_order = [
        "MIGRATED_TO_WORKDAY", "MIGRATED_TO_ASHBY", "MIGRATED_TO_BAMBOOHR",
        "MIGRATED_TO_ICIMS", "MIGRATED_TO_SUCCESSFACTORS", "MIGRATED_TO_TALEO",
        "MIGRATED_TO_SMARTRECRUITERS", "MIGRATED_TO_JOBVITE",
        "DEAD_SLUG",
        "ONTARIO_JOBS_NO_SALARY",
        "NO_ONTARIO_JOBS",
        "NO_JOBS_POSTED",
        "SHOULD_BE_WORKING",
        "API_ERROR",
        "NEEDS_MANUAL_REVIEW",
        "unknown",
    ]

    recommendation_map = {
        "DEAD_SLUG":               "**Action**: Remove from SEED_SLUGS.",
        "NO_JOBS_POSTED":          "**Action**: Keep in SEED_SLUGS, monitor monthly.",
        "NO_ONTARIO_JOBS":         "**Action**: Keep in SEED_SLUGS if company has Ontario presence.",
        "ONTARIO_JOBS_NO_SALARY":  "**Action**: Keep. Flag company as 'non-compliant' in employer tracker.",
        "SHOULD_BE_WORKING":       "**Action**: Debug scraper — manual rerun to confirm.",
        "API_ERROR":               "**Action**: Retry next pipeline run. Remove if persistent.",
        "NEEDS_MANUAL_REVIEW":     "**Action**: Manual review needed.",
    }

    # Add migrated verdicts to recommendation map
    for ats in ATS_SIGNALS:
        key = f"MIGRATED_TO_{ats.upper()}"
        recommendation_map[key] = (
            f"**Action**: Remove from {platform} SEED_SLUGS. "
            f"Add slug to search-{ats}.py SEED_SLUGS if supported, "
            f"or add to search-browser.py target list."
        )

    for verdict in verdict_order:
        group = by_verdict.get(verdict, [])
        if not group:
            continue

        lines.append(f"## {verdict} ({len(group)} slugs)")
        lines.append("")
        rec = recommendation_map.get(verdict, "")
        if rec:
            lines.append(rec)
            lines.append("")

        for r in group:
            lines.append(f"### `{r['slug']}`")
            lines.append(f"- Board: [{r['board_url']}]({r['board_url']})")
            if r["final_url"] and r["final_url"] != r["board_url"]:
                lines.append(f"- Redirected to: {r['final_url']}")
            lines.append(f"- API status: {r['api_status']} | Board status: {r['board_status']}")
            lines.append(f"- API jobs: {r['api_job_count']} total | {r['ontario_jobs']} Ontario")
            if r["ats_evidence"]:
                lines.append(f"- ATS migration evidence: `{r['ats_evidence']}`")
            if r["notes"]:
                lines.append(f"- Notes: {r['notes'].strip()}")
            lines.append("")

    report_path.write_text("\n".join(lines))
    print(f"\nReport written to: {report_path}")
    return report_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Investigate failing Greenhouse/Lever slugs")
    parser.add_argument("--platform", choices=["greenhouse", "lever"], required=True)

    slug_group = parser.add_mutually_exclusive_group(required=True)
    slug_group.add_argument("--slugs", help="Comma-separated list of slugs to investigate")
    slug_group.add_argument("--slugs-file", help="File with one slug per line")
    slug_group.add_argument("--from-log", action="store_true",
                            help="Parse {platform}.log and investigate all slugs with API errors")

    parser.add_argument("--output", help="Output report path (default: scripts/slug_investigation_YYYY-MM-DD.md)")
    args = parser.parse_args()

    # Build slug list
    if args.slugs:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    elif args.slugs_file:
        slugs = [l.strip() for l in Path(args.slugs_file).read_text().splitlines() if l.strip()]
    else:  # --from-log
        log_path = SCRIPTS_DIR / f"{args.platform}.log"
        slugs = load_failing_slugs_from_log(log_path, args.platform)
        if not slugs:
            print(f"No failing slugs found in {log_path}")
            return 0

    if not slugs:
        print("No slugs to investigate.")
        return 0

    print(f"Investigating {len(slugs)} {args.platform} slugs...")

    results = asyncio.run(run_investigations(slugs, args.platform))

    report_path = Path(args.output) if args.output else (
        REPORT_DIR / f"slug_investigation_{args.platform}_{TODAY}.md"
    )
    generate_report(results, args.platform, report_path)

    # Summary
    from collections import Counter
    counts = Counter(r["verdict"] for r in results)
    print("\nSummary:")
    for verdict, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {verdict}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
