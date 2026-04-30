#!/usr/bin/env bash
set -euo pipefail

docker compose stop postgres >/dev/null 2>&1 || true
sleep 30
docker compose up -d postgres >/dev/null 2>&1 || true
sleep 5

if docker compose ps postgres | rg -q "Up"; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
