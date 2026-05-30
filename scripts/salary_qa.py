#!/usr/bin/env python3
"""
salary_qa.py — Salary Quality Assurance
Pipeline step 8c: detect suspiciously wide salary ranges, re-extract from
archived job text via Ollama, and correct obvious typos in jobs.json.

Flags:
  --ratio-threshold FLOAT   flag jobs where max/min >= this (default: 4.0)
  --model MODEL             Ollama model to use (default: qwen3:4b)
  --dry-run                 print proposed changes without writing
  --force                   re-review jobs already marked salary_qa_reviewed
"""

import argparse
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

from archive_lib import (
    ARCHIVE_JOBS_DIR,
    JOBS_FILE,
    load_jobs,
    snapshots_dir,
    utc_now,
)

VENV_PYTHON = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable).resolve() != VENV_PYTHON.resolve():
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON)] + sys.argv)

OLLAMA_API = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "qwen3:4b"
DEFAULT_RATIO = 4.0

SALARY_PROMPT = """Extract the salary range from this job posting. Fix obvious typos (e.g. "$95,0000" → 95000, "$1,500,00" → 150000). If the range is genuinely wide on purpose, say so.

Return ONLY this JSON (no explanation, no markdown):
{{"min": <integer or null>, "max": <integer or null>, "confidence": "high|medium|low", "note": "<one sentence>"}}

Job posting text:
{text}
"""


def call_ollama(prompt: str, model: str, timeout: int = 300) -> str:
    import subprocess
    payload = json.dumps({"model": model, "prompt": prompt, "stream": False, "think": False})
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", OLLAMA_API,
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True, timeout=timeout
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr}")
    data = json.loads(result.stdout)
    return data.get("response", "").strip()


def warmup_ollama(model: str) -> None:
    """Send a tiny request to ensure the model is loaded before the main loop."""
    import subprocess
    payload = json.dumps({"model": model, "prompt": "hi", "stream": False, "think": False})
    subprocess.run(
        ["curl", "-s", "-X", "POST", OLLAMA_API,
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True, timeout=300
    )


def _extract_salary_section(text: str, window: int = 800) -> str:
    """Return a focused snippet around salary/compensation keywords."""
    keywords = ["compensation", "salary", "pay range", "total compensation", "$"]
    best_idx = -1
    for kw in keywords:
        idx = text.lower().find(kw.lower())
        if idx != -1:
            # Prefer earlier matches but only compensation/salary keywords
            if kw != "$" and (best_idx == -1 or idx < best_idx):
                best_idx = idx
    if best_idx == -1:
        # fallback: just use first window chars
        return text[:window * 2]
    start = max(0, best_idx - 100)
    return text[start: start + window]


def get_latest_clean_text(job_id: str) -> str | None:
    snaps = snapshots_dir(job_id)
    if not snaps.exists():
        return None
    # Sort snapshot dirs descending (they're named by timestamp)
    dirs = sorted(snaps.iterdir(), reverse=True)
    for d in dirs:
        clean = d / "clean.txt"
        if clean.exists():
            return clean.read_text(errors="replace")
    return None


def parse_llm_response(raw: str) -> dict | None:
    """Extract JSON from LLM response, tolerating preamble/postamble text.
    Tries the LAST JSON object first (model often reasons then outputs JSON)."""
    matches = list(re.finditer(r'\{[^{}]+\}', raw, re.DOTALL))
    # Try last match first (most likely the final JSON answer)
    for m in reversed(matches):
        try:
            parsed = json.loads(m.group())
            if "min" in parsed or "max" in parsed:
                return parsed
        except json.JSONDecodeError:
            continue
    # Fallback: any match with salary-looking keys
    for m in matches:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            continue
    return None


def is_reasonable_correction(orig_min: int, orig_max: int, new_min: int, new_max: int) -> bool:
    """Return True if the corrected values look like a plausible fix."""
    if new_min <= 0 or new_max <= 0:
        return False
    new_ratio = new_max / new_min
    # New ratio should be narrower and more normal
    if new_ratio >= (orig_max / orig_min):
        return False
    if new_ratio > 5.0:
        return False
    # Corrected values should be in a plausible range ($20k–$2M)
    if new_min < 20_000 or new_max > 2_000_000:
        return False
    return True


def fmt_salary(v: int) -> str:
    return f"${v:,}"


def main():
    parser = argparse.ArgumentParser(description="Salary QA — detect and correct wide salary ranges")
    parser.add_argument("--ratio-threshold", type=float, default=DEFAULT_RATIO,
                        help=f"Flag jobs where max/min >= this (default {DEFAULT_RATIO})")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Ollama model (default {DEFAULT_MODEL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print proposed changes without writing")
    parser.add_argument("--force", action="store_true",
                        help="Re-review jobs already marked salary_qa_reviewed")
    args = parser.parse_args()

    data = load_jobs()
    jobs = data["jobs"]

    flagged = []
    for j in jobs:
        if j.get("status") != "active":
            continue
        lo = j.get("min", 0)
        hi = j.get("max", 0)
        if lo <= 0 or hi <= 0:
            continue
        ratio = hi / lo
        if ratio < args.ratio_threshold:
            continue
        if not args.force and j.get("salary_qa_reviewed"):
            continue
        flagged.append(j)

    print(f"[salary_qa] Flagged {len(flagged)} jobs with ratio >= {args.ratio_threshold}x")

    if not flagged:
        print("[salary_qa] Nothing to review — done")
        return

    print(f"[salary_qa] Warming up Ollama ({args.model})...")
    warmup_ollama(args.model)

    corrections = 0
    reviewed = 0

    for j in flagged:
        job_id = str(j["id"])
        lo = j["min"]
        hi = j["max"]
        ratio = hi / lo
        role = j.get("role", "")[:50]
        company = j.get("company", "")[:25]

        print(f"\n  [{job_id}] {role} @ {company}")
        print(f"         stored: {fmt_salary(lo)}–{fmt_salary(hi)} ({ratio:.1f}x)")

        text = get_latest_clean_text(job_id)
        if text is None:
            print(f"         no archive — skipping (flagged in log only)")
            continue

        # Build prompt
        min_str = f"{lo:,}"
        max_str = f"{hi:,}"
        # Focus the text: extract salary-relevant section if possible
        focused = _extract_salary_section(text)
        prompt = SALARY_PROMPT.format(text=focused)

        try:
            raw = call_ollama(prompt, args.model)
        except Exception as e:
            print(f"         Ollama error: {e}")
            continue

        parsed = parse_llm_response(raw)
        if not parsed:
            print(f"         could not parse LLM response: {raw[:120]}")
            continue

        new_min = parsed.get("min")
        new_max = parsed.get("max")
        confidence = parsed.get("confidence", "?")
        note = parsed.get("note", "")

        print(f"         LLM says: {fmt_salary(new_min) if new_min else 'null'}–"
              f"{fmt_salary(new_max) if new_max else 'null'} "
              f"({confidence}) — {note}")

        # Mark as reviewed regardless
        reviewed += 1

        if (new_min and new_max and
                (new_min != lo or new_max != hi) and
                is_reasonable_correction(lo, hi, new_min, new_max)):

            print(f"         ✓ CORRECTION: {fmt_salary(lo)}–{fmt_salary(hi)} → "
                  f"{fmt_salary(new_min)}–{fmt_salary(new_max)}")
            corrections += 1

            if not args.dry_run:
                j["min"] = new_min
                j["max"] = new_max
                j["salary_qa_corrected"] = True
                j["salary_qa_original"] = {"min": lo, "max": hi}
                j["salary_qa_note"] = note
                j["salary_qa_reviewed"] = utc_now()
        else:
            # Confirmed as-is (could be genuine wide band)
            if not args.dry_run:
                j["salary_qa_reviewed"] = utc_now()
                if new_min and new_max:
                    j["salary_qa_note"] = note

    print(f"\n[salary_qa] Summary: {len(flagged)} flagged, "
          f"{reviewed} reviewed, {corrections} corrected")

    if args.dry_run:
        print("[salary_qa] Dry run — no changes written")
        return

    if reviewed > 0:
        with JOBS_FILE.open("w") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            f.write("\n")
        print(f"[salary_qa] jobs.json updated")


if __name__ == "__main__":
    main()
