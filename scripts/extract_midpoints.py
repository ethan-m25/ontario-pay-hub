#!/usr/bin/env python3
"""Extract midpoints from job-archive source.json files and compute histogram bins."""
import json
import os
import sys

ARCHIVE_DIR = "/Users/clawii/ontario-pay-hub/data/job-archive/jobs"
SLIDER_MIN = 30000
SLIDER_MAX = 220000
BINS = 20
BIN_W = (SLIDER_MAX - SLIDER_MIN) / BINS

def extract_midpoints():
    midpoints = []
    errors = 0
    for entry in os.scandir(ARCHIVE_DIR):
        if not entry.is_dir():
            continue
        src = os.path.join(entry.path, "source.json")
        if not os.path.exists(src):
            continue
        try:
            with open(src) as f:
                data = json.load(f)
            mn = data.get("min")
            mx = data.get("max")
            if mn is None or mx is None:
                continue
            try:
                mn = float(mn)
                mx = float(mx)
            except (TypeError, ValueError):
                continue
            mp = (mn + mx) / 2
            if mp <= 0:
                continue
            midpoints.append(mp)
        except Exception:
            errors += 1

    return midpoints, errors

def compute_bins(midpoints):
    bins = [0] * BINS
    for mp in midpoints:
        if mp < SLIDER_MIN:
            idx = 0
        elif mp >= SLIDER_MAX:
            idx = BINS - 1
        else:
            idx = min(int((mp - SLIDER_MIN) / BIN_W), BINS - 1)
        bins[idx] += 1
    return bins

def main():
    midpoints, errors = extract_midpoints()
    bins = compute_bins(midpoints)
    print(f"# Total midpoints extracted: {len(midpoints)}", file=sys.stderr)
    print(f"# Errors: {errors}", file=sys.stderr)
    print(f"# Bin counts: {bins}", file=sys.stderr)
    # Output JS array
    print(json.dumps(bins))

if __name__ == "__main__":
    main()
