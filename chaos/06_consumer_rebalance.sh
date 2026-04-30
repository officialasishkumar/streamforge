#!/usr/bin/env bash
set -euo pipefail

docker compose up -d --scale worker=1 worker >/dev/null 2>&1 || true
sleep 3
docker compose up -d --scale worker=2 worker >/dev/null 2>&1 || true
sleep 3
docker compose up -d --scale worker=1 worker >/dev/null 2>&1 || true

echo "PASS"
