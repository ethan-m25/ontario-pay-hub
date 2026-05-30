#!/usr/bin/env python3
"""
ontario-pay-hub/scripts/coverage-audit.py
Coverage gap detector — runs independently of the nightly pipeline.

For each target Ontario employer:
  1. Detect ATS platform (Greenhouse slug test, Lever slug test, known mapping)
  2. Count Ontario jobs with salary on that platform
  3. Cross-reference against jobs.json to measure what we're capturing
  4. Print a ranked coverage report

Output legend:
  ✅ COVERED     — jobs present in jobs.json, platform API accessible
  ⚠️  PARTIAL     — we have some jobs but platform has more
  ❌ GAP          — platform accessible, Ontario jobs exist, none in jobs.json
  🔒 AUTH         — platform requires authentication (SuccessFactors, Taleo, etc.)
  🚫 NO SALARY   — platform has no structured salary field in public API
  ❓ UNKNOWN     — couldn't detect ATS or no Ontario jobs found

Run: python3 ~/ontario-pay-hub/scripts/coverage-audit.py
     python3 ~/ontario-pay-hub/scripts/coverage-audit.py --quick   # skip slow page fetches
"""

import argparse
import html as html_mod
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict

DATA_FILE = os.path.expanduser("~/ontario-pay-hub/data/jobs.json")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

ONTARIO_TERMS = [
    "ontario", "toronto", "ottawa", "waterloo", "mississauga",
    "hamilton", "brampton", "markham", "vaughan", "richmond hill",
    "oakville", "kitchener", "windsor", "canada",
]

SALARY_RE = re.compile(
    r'\$?\s*[\d,]+(?:\.\d+)?\s*[-–—]\s*\$?\s*[\d,]+(?:\.\d+)?\s*(?:CAD|CDN)?\b'
    r'|CAD\s*\$?\s*[\d,]+',
    re.IGNORECASE,
)

# ── Target employer list ──────────────────────────────────────────────────────
# (display_name, greenhouse_slug_hint, lever_slug_hint, workday_host_hint, known_ats)
# known_ats: 'greenhouse', 'lever', 'workday', 'successfactors', 'ashby',
#            'taleo', 'smartrecruiters', 'icims', 'custom', None
EMPLOYERS = [
    # ── Banking / Financial ──────────────────────────────────────────────────
    ("RBC",                     None,         None,          "rbc.wd3",       "workday"),
    ("TD Bank",                 None,         None,          "td.wd3",        "workday"),
    ("BMO",                     None,         None,          "bmo.wd3",       "workday"),
    ("CIBC",                    None,         None,          "cibc.wd3",      "workday"),
    ("Scotiabank",              None,         None,          None,            "successfactors"),
    ("Manulife",                None,         None,          "manulife.wd3",  "workday"),
    ("Sun Life",                None,         None,          "sunlife.wd3",   "workday"),
    ("Canada Life",             None,         None,          None,            "workday"),
    ("OMERS",                   None,         None,          "omers.wd3",     "workday"),
    ("CPP Investments",         None,         None,          None,            "workday"),
    ("Ontario Teachers'",       None,         None,          None,            "workday"),
    ("Intact Financial",        None,         None,          "intactfc.wd3",  "workday"),
    ("Wealthsimple",            "wealthsimple", "wealthsimple", None,         "ashby"),
    ("Brookfield",              None,         None,          "brookfield.wd5","workday"),

    # ── Telecom / Media ──────────────────────────────────────────────────────
    ("Rogers Communications",   None,         None,          None,            "successfactors"),
    ("Bell Canada",             None,         None,          None,            "successfactors"),
    ("Telus",                   None,         None,          None,            "successfactors"),
    ("Cogeco",                  None,         None,          "cogeco.wd3",    "workday"),

    # ── Tech (US with Ontario offices) ───────────────────────────────────────
    ("Shopify",                 None,         None,          None,            "custom"),   # Custom system, shopify.com/careers sitemap
    ("Google Canada",           None,         None,          None,            "custom"),
    ("Microsoft Canada",        None,         None,          None,            "icims"),
    ("Amazon Canada",           None,         None,          None,            "amazon"),
    ("Apple Canada",            None,         None,          None,            "custom"),
    ("Meta Canada",             None,         None,          None,            "custom"),
    ("IBM Canada",              None,         None,          None,            "successfactors"),
    ("Oracle Canada",           None,         None,          None,            "taleo"),
    ("SAP Canada",              None,         None,          None,            "successfactors"),
    ("Salesforce",              None,         None,          "salesforce.wd12","workday"),
    ("Databricks",              "databricks", None,          None,            "greenhouse"),
    ("Twilio",                  "twilio",     None,          None,            "greenhouse"),
    ("Okta",                    "okta",       None,          None,            "greenhouse"),
    ("Elastic",                 "elastic",    None,          None,            "greenhouse"),
    ("MongoDB",                 "mongodb",    None,          None,            "greenhouse"),
    ("Datadog",                 "datadog",    None,          None,            "greenhouse"),
    ("GitLab",                  "gitlab",     None,          None,            "greenhouse"),
    ("Dropbox",                 "dropbox",    None,          None,            "greenhouse"),
    ("Affirm",                  "affirm",     None,          None,            "greenhouse"),
    ("Lyft",                    "lyft",       None,          None,            "greenhouse"),
    ("Instacart",               "instacart",  None,          None,            "greenhouse"),
    ("Samsara",                 "samsara",    None,          None,            "greenhouse"),
    ("Cohere",                  "cohere",     "cohere",      None,            "ashby"),
    ("1Password",               None,         None,          None,            "ashby"),
    ("Hootsuite",               "hootsuite",  None,          None,            "greenhouse"),
    ("Tailscale",               "tailscale",  None,          None,            "greenhouse"),
    ("Wattpad",                 "wattpad",    None,          None,            None),
    ("PagerDuty",               "pagerduty",  None,          None,            "greenhouse"),

    # ── Canadian tech startups ────────────────────────────────────────────────
    ("Wealthsimple",            "wealthsimple", None,        None,            "ashby"),
    ("Nuvei",                   "nuvei",      "nuvei",       None,            "greenhouse"),
    ("StackAdapt",              "stackadapt", None,          None,            "greenhouse"),
    ("Achievers",               "achievers",  "achievers",   None,            None),
    ("Clio",                    None,         "clio",        None,            "lever"),
    ("D2L",                     "d2l",        None,          None,            "greenhouse"),
    ("Dayforce",                "dayforce",   None,          None,            "greenhouse"),
    ("Miovision",               "miovision",  "miovision",   None,            None),
    ("BenchSci",                None,         "benchsci",    None,            "lever"),
    ("Wagepoint",               None,         None,          None,            None),
    ("PointClickCare",          None,         "pointclickcare", None,         "lever"),
    ("Tenstorrent",             "tenstorrent", None,         None,            "greenhouse"),
    ("Assent",                  None,         "assent",      None,            "lever"),
    ("Certn",                   None,         "certn",       "certn",         "ashby"),
    ("Validere",                None,         "validere",    None,            "lever"),
    ("Relay",                   None,         "relay",       None,            "lever"),

    # ── Retail / Consumer ─────────────────────────────────────────────────────
    ("Loblaw Companies",        None,         None,          "myview.wd3",    "workday"),
    ("Canadian Tire",           None,         None,          "canadiantirecorporation.wd3", "workday"),
    ("Hudson's Bay Company",    None,         None,          "mywdhr.wd1",    "workday"),
    ("Walmart Canada",          None,         None,          "walmart.wd5",   "workday"),
    ("Sobeys / Empire",         None,         None,          None,            "successfactors"),
    ("Dollarama",               None,         None,          None,            None),

    # ── Professional services ─────────────────────────────────────────────────
    ("Deloitte Canada",         None,         None,          None,            "custom"),      # custom career portal
    ("EY Canada",               None,         None,          None,            "custom"),      # custom career portal
    ("KPMG Canada",             None,         None,          None,            "icims"),       # careers.kpmg.ca — no salary visible
    ("PwC Canada",              None,         None,          "pwc.wd3",       "workday"),  # global Workday — Canada portal at jobs-ca.pwc.com (partial coverage)
    ("BDO Canada",              None,         None,          "bdo.wd3",       "workday"),

    # ── Energy / Utilities / Resources ───────────────────────────────────────
    ("Ontario Power Generation",None,         None,          None,            "workday"),
    ("Hydro One",               None,         None,          None,            "workday"),
    ("Enbridge",                None,         None,          "enbridge.wd3",  "workday"),
    ("TC Energy",               None,         None,          "tcenergy.wd3",  "workday"),
    ("Barrick Gold",            None,         None,          None,            "workday"),
    ("Kinross Gold",            None,         None,          None,            "workday"),

    # ── Healthcare ────────────────────────────────────────────────────────────
    ("UHN",                     None,         None,          None,            "smartrecruiters"),
    ("SickKids",                None,         None,          None,            "smartrecruiters"),
    ("Sunnybrook Health",       None,         None,          None,            None),
    ("CAMH",                    None,         None,          None,            None),

    # ── Government / Public sector ────────────────────────────────────────────
    ("City of Toronto",         None,         None,          None,            "successfactors"),
    ("Government of Ontario",   None,         None,          None,            "custom"),
    ("NAV Canada",              None,         None,          "navcanada.wd10","workday"),
    ("Metrolinx",               None,         None,          None,            None),

    # ── Other ─────────────────────────────────────────────────────────────────
    ("Thomson Reuters",         None,         None,          "thomsonreuters.wd5", "workday"),  # 604 jobs, posts Ontario salary $100K-$145K CAD
    ("Magna International",     None,         None,          "wd3.myworkdaysite", "workday"),  # myworkdaysite.com, now in SEED_TENANTS
    ("Celestica",               None,         None,          None,            "successfactors"),  # not Workday
    ("Restaurant Brands Intl",  None,         None,          "rbi.wd3",       "workday"),
    ("Benevity",                "benevity",   None,          None,            "greenhouse"),
    # AEM / custom portals found manually to have Ontario salary
    ("Accenture Canada",        None,         None,          None,            "custom"),  # accenture.com/ca-en/careers — AEM portal, CSRF-gated list API
]

# Platforms that require auth or have no salary — report but don't try to scrape
NO_SCRAPE_ATS = {
    "successfactors": "🔒 SuccessFactors (OAuth required)",
    "taleo":          "🔒 Oracle Taleo (auth required)",
    "icims":          "🔒 iCIMS (auth required)",
    "custom":         "🔒 Custom careers site (JS-rendered)",
    "ashby":          "🚫 Ashby (JS-rendered SPA — no salary without full browser)",
    "smartrecruiters":"⚠️  SmartRecruiters (hourly pay only, no annual salary)",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_jobs_db():
    """Load jobs.json and return (jobs_list, company_index)."""
    with open(DATA_FILE) as f:
        db = json.load(f)
    jobs = [j for j in db.get("jobs", []) if j.get("status") != "archived"]
    index = defaultdict(list)
    for j in jobs:
        key = j.get("company", "").lower().strip()
        index[key].append(j)
    return jobs, index


def _get(url, timeout=12):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _wd_post(host, company, tenant, timeout=12):
    url = f"https://{host}.myworkdayjobs.com/wday/cxs/{company}/{tenant}/jobs"
    body = json.dumps({"appliedFacets": {}, "limit": 5, "offset": 0, "searchText": ""}).encode()
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", "Content-Type: application/json",
         "-d", body, "--max-time", str(timeout)],
        capture_output=True, text=True, timeout=timeout + 3,
    )
    return json.loads(r.stdout)


def gh_ontario_count(slug, quick=False):
    """Return (total_jobs, ontario_salary_count) for a Greenhouse slug."""
    try:
        d = _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content={'true' if not quick else 'false'}", timeout=15)
    except Exception:
        return None, None
    jobs = d.get("jobs", [])
    if quick:
        ontario = sum(1 for j in jobs
                      if any(t in (j.get("location", {}).get("name", "") or "").lower()
                             for t in ONTARIO_TERMS))
        return len(jobs), ontario  # salary unknown in quick mode

    ontario_salary = 0
    for j in jobs:
        loc = (j.get("location", {}).get("name", "") or "").lower()
        if not any(t in loc for t in ONTARIO_TERMS):
            continue
        text = html_mod.unescape(re.sub(r"<[^>]+>", " ", j.get("content", "") or ""))
        if SALARY_RE.search(text):
            ontario_salary += 1
    return len(jobs), ontario_salary


def lever_ontario_count(slug, quick=False):
    """Return (total_jobs, ontario_salary_count) for a Lever slug."""
    try:
        jobs = _get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=12)
        if not isinstance(jobs, list):
            return None, None
    except Exception:
        return None, None

    ontario = 0
    ontario_salary = 0
    for j in jobs:
        cats = j.get("categories") or {}
        loc = cats.get("location", "") or ""
        if not any(t in loc.lower() for t in ONTARIO_TERMS):
            continue
        ontario += 1
        if quick:
            continue
        sal = j.get("salaryRange")
        if sal and sal.get("currency", "").upper() == "CAD" and sal.get("interval") == "per-year-salary":
            ontario_salary += 1
        else:
            desc = j.get("descriptionPlain") or j.get("salaryDescriptionPlain") or ""
            if SALARY_RE.search(desc):
                ontario_salary += 1

    return len(jobs), ontario_salary if not quick else ontario


def wd_ontario_count(host, tenant_part):
    """Check Workday tenant total jobs."""
    try:
        host_full, company = host.split(".", 1)[0], host.split(".")[0]
        # host is like "rbc.wd3"
        parts = host.split(".")
        company_id = parts[0]
        wd_host = host  # e.g. "rbc.wd3"
        d = _wd_post(wd_host, company_id, tenant_part)
        return d.get("total", 0)
    except Exception:
        return None


def lookup_in_db(display_name, db_index):
    """Find best match in jobs.json index for a display name."""
    name_lower = display_name.lower()
    # Exact match first
    if name_lower in db_index:
        return db_index[name_lower]
    # Partial match
    for key, jobs in db_index.items():
        if name_lower in key or key in name_lower:
            return jobs
        # Check any word overlap
        name_words = set(name_lower.split())
        key_words = set(key.split())
        if len(name_words & key_words) >= 2:
            return jobs
    return []


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="Skip salary extraction (faster)")
    parser.add_argument("--employer", help="Audit only this employer name (partial match)")
    args = parser.parse_args()

    print("Loading jobs.json...")
    all_jobs, db_index = load_jobs_db()
    print(f"  {len(all_jobs)} active jobs in database\n")

    # Deduplicate employer list
    seen_names = set()
    employers = []
    for e in EMPLOYERS:
        if e[0] not in seen_names:
            seen_names.add(e[0])
            employers.append(e)

    if args.employer:
        employers = [e for e in employers if args.employer.lower() in e[0].lower()]

    results = []

    for display_name, gh_slug, lever_slug, wd_host, known_ats in employers:
        db_jobs = lookup_in_db(display_name, db_index)
        db_count = len(db_jobs)

        ats_label = known_ats or "unknown"
        platform_count = None   # total jobs on platform
        ontario_salary = None   # Ontario jobs with salary on platform
        status = None
        note = ""

        # ── Known non-scrapable ATS ──────────────────────────────────────────
        if known_ats in NO_SCRAPE_ATS:
            status = NO_SCRAPE_ATS[known_ats]
            if db_count > 0:
                note = f"  (but {db_count} jobs in DB via other routes)"
            results.append((display_name, ats_label, status, platform_count, ontario_salary, db_count, note))
            continue

        # ── Amazon (special case — our custom scraper) ───────────────────────
        if known_ats == "amazon" or display_name.lower().startswith("amazon"):
            if db_count > 0:
                status = "✅ COVERED"
                note = f"via search-amazon.py"
            else:
                status = "❌ GAP"
                note = "search-amazon.py should cover this"
            results.append((display_name, "amazon", status, None, None, db_count, note))
            continue

        # ── Greenhouse ────────────────────────────────────────────────────────
        if known_ats == "greenhouse" or gh_slug:
            slug = gh_slug or display_name.lower().replace(" ", "").replace("-", "")
            total, on_sal = gh_ontario_count(slug, quick=args.quick)
            if total is None:
                status = "❓ UNKNOWN"
                note = f"Greenhouse slug '{slug}' not found"
            else:
                platform_count = total
                ontario_salary = on_sal
                mode = "~Ontario" if args.quick else "Ontario+salary"
                if on_sal == 0:
                    status = "❓ NO MATCH"
                    note = f"{total} total jobs, 0 {mode}"
                elif db_count >= on_sal * 0.7:
                    status = "✅ COVERED"
                    note = f"{db_count}/{on_sal} {mode} in DB"
                elif db_count > 0:
                    status = "⚠️  PARTIAL"
                    note = f"{db_count}/{on_sal} {mode} in DB"
                else:
                    status = "❌ GAP"
                    note = f"{on_sal} {mode} jobs, 0 in DB — add slug '{slug}'"
            results.append((display_name, f"greenhouse/{slug}", status, platform_count, ontario_salary, db_count, note))
            time.sleep(0.5)
            continue

        # ── Lever ─────────────────────────────────────────────────────────────
        if known_ats == "lever" or lever_slug:
            slug = lever_slug or display_name.lower().replace(" ", "").replace("-", "")
            total, on_sal = lever_ontario_count(slug, quick=args.quick)
            if total is None:
                status = "❓ UNKNOWN"
                note = f"Lever slug '{slug}' not found"
            else:
                platform_count = total
                ontario_salary = on_sal
                mode = "~Ontario" if args.quick else "Ontario+salary"
                if on_sal == 0:
                    status = "❓ NO MATCH"
                    note = f"{total} total, 0 {mode}"
                elif db_count >= on_sal * 0.7:
                    status = "✅ COVERED"
                    note = f"{db_count}/{on_sal} in DB"
                elif db_count > 0:
                    status = "⚠️  PARTIAL"
                    note = f"{db_count}/{on_sal} in DB"
                else:
                    status = "❌ GAP"
                    note = f"{on_sal} {mode} jobs, 0 in DB — add lever slug '{slug}'"
            results.append((display_name, f"lever/{slug}", status, platform_count, ontario_salary, db_count, note))
            time.sleep(0.5)
            continue

        # ── Workday ───────────────────────────────────────────────────────────
        if known_ats == "workday" or wd_host:
            # wd_host is like "rbc.wd3" — need tenant too
            # For now just check if company_id is in SEED_TENANTS (db_count > 0)
            if db_count > 0:
                status = "✅ COVERED"
                note = f"{db_count} jobs in DB"
            else:
                status = "⚠️  CHECK"
                note = f"Workday host {wd_host or '?'} — verify tenant config"
            results.append((display_name, f"workday/{wd_host or '?'}", status, None, None, db_count, note))
            continue

        # ── Unknown ATS — try both Greenhouse and Lever ───────────────────────
        found = False
        for slug in [display_name.lower().replace(" ", "").replace("-", ""),
                     display_name.lower().replace(" ", "-"),
                     display_name.lower().split()[0]]:
            total, on_sal = gh_ontario_count(slug, quick=True)
            if total is not None and total > 0:
                platform_count = total
                ontario_salary = on_sal or 0
                if on_sal and on_sal > 0:
                    status = "⚠️  NEW FIND" if db_count == 0 else "✅ COVERED"
                    note = f"Greenhouse/{slug}: {on_sal} Ontario jobs — add to seed slugs!"
                else:
                    status = "❓ GH-EXISTS"
                    note = f"Greenhouse/{slug}: {total} jobs but 0 Ontario+salary"
                results.append((display_name, f"greenhouse/{slug}", status, platform_count, ontario_salary, db_count, note))
                found = True
                time.sleep(0.5)
                break

            total, on_sal = lever_ontario_count(slug, quick=True)
            if total is not None and total > 0:
                platform_count = total
                ontario_salary = on_sal or 0
                status = "⚠️  NEW FIND" if db_count == 0 else "✅ COVERED"
                note = f"Lever/{slug}: {on_sal} Ontario jobs — add to seed slugs!"
                results.append((display_name, f"lever/{slug}", status, platform_count, ontario_salary, db_count, note))
                found = True
                time.sleep(0.5)
                break

        if not found:
            status = "❓ UNKNOWN"
            note = f"ATS not detected — manual check needed"
            if db_count > 0:
                status = "✅ COVERED"
                note = f"{db_count} jobs in DB (ATS unknown)"
            results.append((display_name, "unknown", status, None, None, db_count, note))

    # ── Print report ──────────────────────────────────────────────────────────
    print("=" * 80)
    print("ONTARIO PAY HUB — COVERAGE AUDIT REPORT")
    print(f"Generated: {__import__('datetime').date.today()}")
    print("=" * 80)

    # Group by status
    groups = {
        "❌ GAP":         [],
        "⚠️  NEW FIND":   [],
        "⚠️  PARTIAL":    [],
        "⚠️  CHECK":      [],
        "🔒 SuccessFactors (OAuth required)": [],
        "🔒 Oracle Taleo (auth required)": [],
        "🔒 iCIMS (auth required)": [],
        "🔒 Custom careers site (JS-rendered)": [],
        "🚫 Ashby (no salary in public API)": [],
        "⚠️  SmartRecruiters (hourly pay only, no annual salary)": [],
        "✅ COVERED":     [],
        "❓ UNKNOWN":     [],
        "❓ NO MATCH":    [],
        "❓ GH-EXISTS":   [],
    }

    for r in results:
        display_name, ats_label, status, platform_count, ontario_salary, db_count, note = r
        groups.setdefault(status, []).append(r)

    section_order = [
        "❌ GAP",
        "⚠️  NEW FIND",
        "⚠️  PARTIAL",
        "⚠️  CHECK",
        "✅ COVERED",
        "🔒 SuccessFactors (OAuth required)",
        "🔒 Oracle Taleo (auth required)",
        "🔒 iCIMS (auth required)",
        "🔒 Custom careers site (JS-rendered)",
        "🚫 Ashby (no salary in public API)",
        "⚠️  SmartRecruiters (hourly pay only, no annual salary)",
        "❓ UNKNOWN",
        "❓ NO MATCH",
        "❓ GH-EXISTS",
    ]

    total_gap = len(groups.get("❌ GAP", [])) + len(groups.get("⚠️  NEW FIND", []))
    total_covered = len(groups.get("✅ COVERED", []))
    print(f"\nSummary: {total_covered} covered, {total_gap} actionable gaps\n")

    for section in section_order:
        entries = groups.get(section, [])
        if not entries:
            continue
        print(f"\n{'─' * 60}")
        print(f"  {section}  ({len(entries)})")
        print(f"{'─' * 60}")
        for display_name, ats_label, status, platform_count, ontario_salary, db_count, note in sorted(entries, key=lambda x: x[0]):
            on_str = f"{ontario_salary}" if ontario_salary is not None else "?"
            db_str = str(db_count)
            print(f"  {display_name:<30} [{ats_label}]")
            print(f"    platform_on_salary={on_str:>4}  db={db_str:>4}  {note}")

    print(f"\n{'=' * 80}")

    # ── Action items ─────────────────────────────────────────────────────────
    gaps = groups.get("❌ GAP", []) + groups.get("⚠️  NEW FIND", [])
    if gaps:
        print("\nACTION ITEMS — add these to seed lists:")
        for display_name, ats_label, _, _, on_sal, _, note in sorted(gaps, key=lambda x: -(x[4] or 0)):
            print(f"  {display_name}: {note}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
