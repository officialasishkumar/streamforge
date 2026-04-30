#!/usr/bin/env bash
set -euo pipefail

STREAMFORGE_URL="${STREAMFORGE_URL:-http://localhost:8080}"
TENANT_ID="${TENANT_ID:-tenant-a}"
REQUEST_ID="${REQUEST_ID:-example-$(date +%s)}"
CLIENT_TIMESTAMP="${CLIENT_TIMESTAMP:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

curl -sS -X POST "${STREAMFORGE_URL}/v1/events" \
	-H "Content-Type: application/json" \
	-H "X-Request-ID: ${REQUEST_ID}" \
	-d @- <<JSON
{
  "events": [
    {
      "tenant_id": "${TENANT_ID}",
      "event_type": "user.signup",
      "client_timestamp": "${CLIENT_TIMESTAMP}",
      "body": {
        "source": "example-script",
        "plan": "starter"
      }
    }
  ]
}
JSON
