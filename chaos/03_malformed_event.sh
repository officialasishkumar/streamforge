#!/usr/bin/env bash
set -euo pipefail

status="$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:8080/v1/events" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"tenant-a","events":[{"event_type":"bad","body":{},"client_timestamp":"not-a-time"}]}')"

if [[ "${status}" == "400" ]]; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
