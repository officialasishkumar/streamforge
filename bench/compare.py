#!/usr/bin/env python3
import json
import sys
from pathlib import Path

TOLERANCE = 0.10


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def as_markdown(rows):
    header = "| Metric | Baseline | Current | Delta | Status |"
    sep = "|---|---:|---:|---:|---|"
    body = []
    for r in rows:
        body.append(f"| {r['metric']} | {r['baseline']:.4f} | {r['current']:.4f} | {r['delta']:.2%} | {r['status']} |")
    return "\n".join([header, sep] + body)


def main():
    if len(sys.argv) != 3:
        print("usage: bench/compare.py <baseline.json> <current.json>")
        return 2

    baseline = load_json(Path(sys.argv[1]))["metrics"]
    current = load_json(Path(sys.argv[2]))["metrics"]

    rows = []
    failed = False
    for metric, baseline_value in baseline.items():
        cur = float(current.get(metric, baseline_value))
        delta = 0.0 if baseline_value == 0 else (cur - float(baseline_value)) / float(baseline_value)
        regression = delta > TOLERANCE
        status = "REGRESSION" if regression else "OK"
        rows.append(
            {"metric": metric, "baseline": float(baseline_value), "current": cur, "delta": delta, "status": status}
        )
        if regression:
            failed = True

    print(as_markdown(rows))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
