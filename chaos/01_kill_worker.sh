#!/usr/bin/env bash
set -euo pipefail

docker compose kill worker >/dev/null 2>&1 || true
sleep 2
docker compose up -d worker >/dev/null 2>&1 || true

if docker compose ps worker | rg -q "Up"; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
