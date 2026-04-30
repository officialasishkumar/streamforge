# Benchmark Baseline Updates

1. Run all scripts under `bench/k6` against a stable environment.
2. Export aggregate metrics JSON and normalize field names used by `bench/compare.py`.
3. Replace `baseline.json` only when at least three consecutive runs stay within expected variance.
4. Commit baseline updates separately from feature code so perf deltas are auditable.
