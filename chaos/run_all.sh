#!/usr/bin/env bash
set -euo pipefail

scripts=(
  "01_kill_worker.sh"
  "02_postgres_outage.sh"
  "03_malformed_event.sh"
  "04_kafka_latency.sh"
  "05_redis_outage.sh"
  "06_consumer_rebalance.sh"
  "07_clock_skew.sh"
)

passed=0
failed=0

for script in "${scripts[@]}"; do
  echo "running=${script}"
  output="$(bash "$(dirname "$0")/${script}" 2>&1 || true)"
  echo "${output}"
  last_line="$(printf "%s" "${output}" | awk 'NF {line=$0} END {print line}')"
  if [[ "${last_line}" == "PASS" ]]; then
    passed=$((passed + 1))
  else
    failed=$((failed + 1))
  fi
done

echo "summary=passed:${passed},failed:${failed}"
if [[ "${failed}" -eq 0 ]]; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
