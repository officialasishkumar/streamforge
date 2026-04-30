#!/usr/bin/env bash
set -euo pipefail

docker compose stop redis >/dev/null 2>&1 || true
sleep 5

status="$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:8080/v1/events" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"tenant-a","events":[{"event_type":"user.signup","body":{"source":"web"},"client_timestamp":"2026-05-01T00:00:00Z"}]}')"

docker compose up -d redis >/dev/null 2>&1 || true

if [[ "${status}" == "202" || "${status}" == "503" ]]; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
