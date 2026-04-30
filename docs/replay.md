# Replay Operations

The replay tool republish archived event payloads from S3 to Kafka to recover from worker outages, schema regressions, or downstream incidents.

## Command

`go run ./cmd/replay --tenant tenant-a --from 2026-05-01T00:00:00Z --to 2026-05-01T06:00:00Z --rps 200`

## Flags

- `--tenant`: restrict replay to a single tenant.
- `--from`: lower RFC3339 bound on archive object modified time.
- `--to`: upper RFC3339 bound on archive object modified time.
- `--rps`: replay throttle to protect worker and database capacity.

## Operational Guidance

- Start with low RPS and observe consumer lag and Postgres pool saturation.
- Replay smallest affected tenant scope first before widening window.
- Keep replay windows explicit to avoid accidentally replaying unrelated historical data.
