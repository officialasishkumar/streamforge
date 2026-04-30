# Incident: Postgres Pool Exhaustion Under Worker Load

- **Date:** 2026-04-15
- **Severity:** SEV-2
- **Duration:** 47 minutes

## Summary

Worker throughput collapsed during a sustained ingest spike because workers held Postgres connections while awaiting S3 archive writes, exhausting pool capacity and stalling transactional writes.

## Timeline

- 13:02 UTC: Alerts fired for worker lag growth and ingest p95 latency increase.
- 13:07 UTC: On-call confirmed Postgres acquisition waiters increasing without corresponding query throughput.
- 13:14 UTC: Temporary worker replica reduction reduced queue churn but lag continued to rise.
- 13:26 UTC: Trace correlation showed connection lease held across downstream S3 write latency spikes.
- 13:34 UTC: Emergency patch deployed moving S3 write out of transaction boundaries.
- 13:49 UTC: Pool waiters normalized and consumer lag returned to baseline.

## Root Cause

Worker transaction handling coupled external S3 write latency with Postgres connection lifetime. Under load, this consumed all available pool connections and blocked other workers from progressing.

## Fix

- Moved S3 write out of the transaction scope.
- Added a 2-second cap on pool acquisition wait to fail fast and shed load predictably.

## Follow-ups

1. Add dedicated metric and alert on pool acquisition wait p95.
2. Add chaos scenario to inject S3 latency while asserting Postgres pool headroom.
3. Add load-test check that validates transaction duration budget under dependency slowdown.
