#!/usr/bin/env python3
import json
from collections import Counter, defaultdict
from pathlib import Path

from category_classifier import classify_category


REPO = Path(__file__).resolve().parents[1]
JOBS_PATH = REPO / "data" / "jobs.json"
GOLD_PATH = REPO / "data" / "manual-category-overrides.json"


def main():
    jobs = {str(j["id"]): j for j in json.loads(JOBS_PATH.read_text(encoding="utf-8"))["jobs"]}
    gold = json.loads(GOLD_PATH.read_text(encoding="utf-8"))["jobs"]

    total = 0
    correct = 0
    mismatches = []
    confusion = Counter()
    by_cat = defaultdict(lambda: [0, 0])

    for row in gold:
        job = jobs.get(str(row["id"]))
        if not job:
            continue
        result = classify_category(job)
        pred = result["predicted_category"]
        truth = row["category"]
        total += 1
        by_cat[truth][0] += 1
        if pred == truth:
            correct += 1
            by_cat[truth][1] += 1
        else:
            confusion[(truth, pred)] += 1
            mismatches.append({
                "id": job["id"],
                "role": job["role"],
                "company": job["company"],
                "truth": truth,
                "pred": pred,
                "confidence": result["confidence_level"],
                "signals": result["matched_signals"],
                "alt": result["alternative_category_candidate"],
            })

    print(f"accuracy: {correct}/{total} = {correct/total:.2%}" if total else "accuracy: n/a")
    print("\nby category:")
    for cat, (n, ok) in sorted(by_cat.items()):
        print(f"  {cat}: {ok}/{n} = {ok/n:.2%}")

    print("\nmost common mismatches:")
    for (truth, pred), n in confusion.most_common(15):
        print(f"  {truth} -> {pred}: {n}")

    print("\nexample mismatches:")
    for row in mismatches[:25]:
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
