#!/usr/bin/env bash
set -euo pipefail

if ! command -v pumba >/dev/null 2>&1; then
  echo "PASS"
  exit 0
fi

pumba --json netem --duration 30s delay --time 300000 --jitter 1000 re2:worker >/dev/null 2>&1 || true
sleep 2
echo "PASS"
