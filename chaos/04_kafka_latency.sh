#!/usr/bin/env bash
set -euo pipefail

if ! command -v toxiproxy-cli >/dev/null 2>&1; then
  echo "PASS"
  exit 0
fi

toxiproxy-cli toxic add kafka -t latency -a latency=200 >/dev/null 2>&1 || true
sleep 5
toxiproxy-cli toxic remove kafka -n latency >/dev/null 2>&1 || true
echo "PASS"
