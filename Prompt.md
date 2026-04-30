You are building StreamForge, a self-hosted real-time event analytics platform. Production-intent open-source project — NOT a learning exercise, portfolio piece, or demo. Write all code, comments, docs, and commit messages as if strangers will deploy this to production at scale.

Use the strongest model available to you (Claude Opus 4.6+, GPT-5.4, or Gemini 3 Pro) for architecture and code-heavy steps. Switch to a faster model only for trivial file scaffolding if you want to save time. Cost is not a constraint.

Execute the steps below sequentially. After each step, run the git workflow described under "GIT WORKFLOW". PAUSE after each step and wait for me to say "continue" before moving to the next step.

# STACK (non-negotiable)

- Go 1.22+ for all services (cmd/ingest, cmd/worker, cmd/replay)
- Apache Kafka via confluent-kafka-go (manual offset commits, never auto-commit)
- PostgreSQL 16 via pgx/v5 + sqlc (no ORM, raw SQL)
- Redis via go-redis/v9
- AWS S3 + SQS via aws-sdk-go-v2 (configurable endpoint URL for LocalStack)
- Prometheus client_golang for metrics; structured slog logs
- Helm + plain Kubernetes manifests for deployment
- GitHub Actions for CI; ghcr.io for container registry

Do NOT introduce: ClickHouse, Terraform, ArgoCD, KEDA, eksctl, schema registry as a separate service, ORMs (gorm/ent), Sarama, Gin/Echo/Fiber. Use stdlib net/http. If you reach for any forbidden tool, stop and ask first.

# ENGINEERING PATTERNS (bake in from the start)

- Idempotency: every event has idempotency_key (client-provided or sha256 of tenant_id|event_type|body|client_timestamp). Workers check Redis (24h TTL) then Postgres unique index before insert.
- At-least-once delivery: Kafka offsets commit ONLY after successful Postgres write + S3 archive. Crash before commit = redelivery; dedup catches duplicates.
- Outbox pattern: worker writes event row + outbox row in the same Postgres transaction. Separate outbox-publisher goroutine drains outbox to SQS. Guarantees notification fires iff event is stored.
- Consistent hashing: Kafka partitioner uses tenant_id as key (preserves per-tenant ordering). Use serialx/hashring for any client-side sharding decisions.
- Rate limiting: token bucket per tenant via redis-rate, returns 429 with Retry-After. Wrapped in circuit breaker so Redis outage doesn't kill ingest (configurable fail-open).
- Circuit breaker: every external dependency call (Kafka, S3, SQS, Postgres, Redis) through sony/gobreaker. 5 failures in 30s opens, half-open after 60s.
- Backpressure: bounded buffered channels everywhere; on full, return 503 with Retry-After. NEVER unbounded queues.
- Retry with exponential backoff + full jitter via cenkalti/backoff/v4. Per-request retry budget; drop after N retries to avoid amplification.
- Bulkheading: separate goroutine pools (errgroup + semaphore) for ingest, S3 archive, outbox publisher. One pool exhausting cannot starve others.
- Graceful shutdown on SIGTERM: stop accepting requests → drain in-flight (30s deadline) → commit final Kafka offsets → close pools.
- Health checks: /healthz (liveness, 200 if process alive) vs /readyz (readiness, 503 if Kafka/Postgres/Redis unreachable in last 5s).
- Observability: correlation ID per request (X-Request-ID, generated if absent), propagated through Kafka headers and into Postgres rows. Every log line carries correlation_id, tenant_id, event_type.
- Golden signals as Prometheus metrics: latency (p50/p95/p99 histograms), traffic (rate), errors (rate by type), saturation (pool utilization, queue depth, consumer lag).

# CODE STYLE

- Lowercase single-word package names where possible.
- Errors wrapped with %w and contextual prefixes: fmt.Errorf("ingest: validate event: %w", err).
- No panics outside main() and tests.
- context.Context as first parameter on every I/O function.
- No global state except Prometheus collectors registered in init().
- Structured slog (JSON) only; no fmt.Println in non-cmd code.
- Lint: golangci-lint with errcheck, gosec, govet, staticcheck, gofumpt.
- No SELECT *, no N+1, no time.Sleep in production code (use context deadlines).
- No hardcoded secrets or endpoints — config-driven via streamforge.yaml.
- TODO comments require an associated GitHub issue number.

# DOCS TONE

- First sentence of any README describes a problem, not a thing built.
- Forbidden phrases: "I built", "I learned", "showcase", "portfolio", "demo project", "side project", "this project demonstrates".
- No emoji anywhere.
- Architecture diagrams are Mermaid, prose-supported. No tech-stack badge collages.
- Limitations sections list 3-5 honest architectural trade-offs.
- Operations sections cover failure modes specifically.

# GIT WORKFLOW (run after every step)

ON STEP 0 ONLY (bootstrap):
1. Run `gh auth status`. If not logged in as officialasishkumar, run `gh auth login` and pause for me to complete.
2. `git init` if needed.
3. `git config user.name "Asish Kumar"` and `git config user.email "officialasishkumar@gmail.com"`.
4. `gh repo create officialasishkumar/streamforge --public --source=. --remote=origin --description "Self-hosted real-time event analytics platform — Kafka, Postgres, S3 archive, full chaos and perf regression suite" --homepage "https://github.com/officialasishkumar/streamforge"`
5. `git branch -M main`

ON EVERY STEP (including step 0):
1. `git status` to verify changes.
2. `git add -A`
3. Commit with Conventional Commits prefix (feat/fix/chore/docs/test/perf/ci/refactor) + scoped subject + 3-6 bullet body listing concrete changes. NEVER use generic messages like "initial commit" or "update files". If a step produces logically distinct chunks, make SEPARATE commits per chunk.
4. `git push -u origin main` (first push), then `git push origin main` subsequent.
5. Print the commit SHA(s) and URL(s) like https://github.com/officialasishkumar/streamforge/commit/<sha>.

AFTER STEP 0's first successful push, also run:
- `gh repo edit officialasishkumar/streamforge --add-topic kafka --add-topic golang --add-topic kubernetes --add-topic postgresql --add-topic event-driven --add-topic observability --add-topic distributed-systems --add-topic chaos-engineering --add-topic event-streaming --add-topic analytics`
- `gh api -X PUT repos/officialasishkumar/streamforge/branches/main/protection -f required_status_checks='{"strict":true,"contexts":["lint","build","unit-test","integration-test","e2e","helm-lint","security","smoke-bench"]}' -F enforce_admins=false -f required_pull_request_reviews='{"required_approving_review_count":0}' -F restrictions=null` — if it fails because the repo is too new, retry once after 5 seconds; if still failing, log and continue.

# STEPS

## Step 0 — Cursor rules + repo bootstrap

Create `.cursor/rules/000-project.md` (alwaysApply: true), `.cursor/rules/100-go-style.md` (globs: ["**/*.go"]), `.cursor/rules/200-docs.md` (globs: ["**/*.md", "**/README*"]) — encode the STACK, ENGINEERING PATTERNS, CODE STYLE, and DOCS TONE sections above into these three rule files with proper YAML frontmatter.

Create `.gitignore` (Go + macOS + IDE), `LICENSE` (Apache 2.0), `go.mod` (module github.com/officialasishkumar/streamforge, go 1.22), `.golangci.yml`, `Makefile` with targets: build, test, integration-test, lint, e2e, chaos, bench, perf-baseline, perf-regression, docker-build, helm-lint.

Create empty directories with .gitkeep: cmd/ingest, cmd/worker, cmd/replay, internal/, pkg/, deploy/helm/streamforge/, deploy/k8s/, deploy/grafana/dashboards/, db/migrations/, db/queries/, chaos/, bench/k6/, bench/results/, docs/, docs/postmortems/, examples/, scripts/, .github/workflows/.

Run the git workflow including the bootstrap steps. PAUSE.

## Step 1 — Config and core types

Generate `streamforge.yaml` with sections: ingest (port, max_batch_size, request_timeout), workers (pool_size, batch_size, fetch_timeout), kafka (brokers, topics: events/dlq/outbox, partitioner_strategy: tenant_hash), postgres (dsn, pool_min, pool_max, statement_timeout), s3 (bucket, endpoint, region, archive_prefix), sqs (queue_url, endpoint, region), redis (addr, db, ratelimit_key_prefix), observability (metrics_addr, log_level, sample_rate), rate_limits (default_per_tenant_rps, default_burst). Include inline comments explaining non-obvious choices.

Generate `internal/config/config.go` loading via spf13/viper with env-var override (STREAMFORGE_*). Validate fail-fast on startup.

Generate `internal/types/event.go` with Event struct, IdempotencyKey computation, JSON marshaling, and `event_test.go` with serialization round-trip benchmark.

Run git workflow. PAUSE.

## Step 2 — Database layer

Generate `db/migrations/` with goose-compatible files:
- 001_init.sql: tenants, event_schemas, events_partitioned (partitioned by RANGE on event_time, monthly partitions, with idempotency_key UNIQUE per tenant), outbox, dlq_events
- 002_indexes.sql: btree on (tenant_id, event_time DESC), btree on (tenant_id, event_type, event_time DESC), partial index on outbox WHERE sent_at IS NULL

Generate `db/queries/*.sql` for sqlc with type-safe queries for: insert event (with ON CONFLICT DO NOTHING for idempotency), batch insert events, find unsent outbox rows, mark outbox sent, insert DLQ event, fetch active schemas per tenant.

Generate `sqlc.yaml` config. Generate `internal/store/` wrapping sqlc-generated code with circuit breakers, structured logging, and pgx connection pool management.

Generate `scripts/create_partition.sh` (creates next-month partition; meant to run as a Kubernetes CronJob).

Run git workflow (multiple commits: migrations, sqlc setup, store wrapper, partition script). PAUSE.

## Step 3 — Ingest API

Generate `cmd/ingest/main.go` and `internal/ingest/`. Stdlib net/http server with structured slog, Prometheus /metrics, /healthz, /readyz, graceful shutdown.

POST /v1/events endpoint:
- JSON event batch (max 1000 events, configurable)
- Validates against JSON schema fetched from Postgres (in-memory cache, 60s TTL)
- Token-bucket rate limit per tenant via redis-rate
- Computes idempotency_key for events that don't have one
- Routes to Kafka via confluent-kafka-go with key=tenant_id (consistent partitioning)
- Writes raw batch to S3 BEFORE acking client (archive-first semantics)
- Returns 202 success, 429 rate-limited (Retry-After), 503 circuit-open (Retry-After), 400 validation failure with details

Files: server.go, handler.go, ratelimit.go, schema_cache.go, kafka_producer.go, s3_archive.go, middleware.go (correlation ID, logging, metrics).

Include `internal/ingest/integration_test.go` (build tag integration) using testcontainers-go to spin up Kafka + Postgres + Redis + LocalStack and assert end-to-end event flow including rate limit, schema rejection, and circuit breaker behavior.

Run git workflow (separate commits: server skeleton, handler, rate limit, schema cache, Kafka producer, S3 archive, integration tests). PAUSE.

## Step 4 — Worker

Generate `cmd/worker/main.go` and `internal/worker/`. Worker pulls from Kafka (consumer group: streamforge-workers):

1. Idempotency check (Redis lookup, Postgres unique-key fallback)
2. Postgres insert into events_partitioned
3. Outbox row insert in SAME transaction
4. Kafka offset commit ONLY after step 2-3 succeed
5. Separate outbox-publisher goroutine drains outbox to SQS, marks sent

errgroup-based worker pool (configurable size, default 8). Failure modes: Postgres down → pause + exponential backoff + circuit breaker; bad event → DLQ topic with reason, rest of batch succeeds; worker crash → no commit, redelivery, dedup.

Include integration test asserting at-least-once + dedup under simulated worker crash mid-batch.

Run git workflow (separate commits: consumer loop, idempotency, transaction logic, outbox publisher, integration test). PAUSE.

## Step 5 — Replay tool, observability, dashboards

Generate `cmd/replay/main.go` — CLI tool that reads raw events from S3 archive and republishes them to Kafka with optional date range, tenant filter, and rate limit (so replays don't melt the workers).

Generate `internal/observability/` with Prometheus collectors: streamforge_events_received_total{tenant,event_type,result}, streamforge_events_processed_total{tenant,result}, streamforge_kafka_consumer_lag{topic,partition}, streamforge_postgres_pool_in_use/idle/waiting, streamforge_circuit_breaker_state{dependency}, streamforge_request_duration_seconds (histogram).

Generate `deploy/grafana/dashboards/streamforge.json` — full dashboard with RED metrics, worker throughput, consumer lag, pool saturation, circuit breaker states, DLQ rate, S3 archive lag.

Generate `deploy/grafana/provisioning/` (datasources + dashboard provider configs).

Run git workflow (separate commits: replay tool, observability package, Grafana dashboard, provisioning). PAUSE.

## Step 6 — Local stack and Helm/k8s

Generate `docker-compose.yml`: Kafka (KRaft mode), Postgres, Redis, LocalStack (S3 + SQS), Prometheus, Grafana (with provisioned dashboard), the ingest API, the worker, the schema seeder. Working out of the box.

Generate `deploy/helm/streamforge/`:
- Chart.yaml v0.1.0
- values.yaml with operator-grade defaults and inline comments
- templates/: ingest Deployment + Service + Ingress, worker Deployment with HPA on CPU + custom metric (kafka consumer lag via prometheus-adapter), ServiceMonitor, ConfigMap from streamforge.yaml, Secret template, partition-creator CronJob, PodDisruptionBudgets, NetworkPolicies, ServiceAccount with minimal RBAC.

Generate `deploy/k8s/` raw manifests for users not on Helm, with a README documenting apply order.

Run git workflow (separate commits: docker-compose, Helm chart skeleton, Helm templates, raw k8s manifests). PAUSE.

## Step 7 — Chaos suite

Generate `chaos/` bash scripts using pumba and toxiproxy. Each prints PASS or FAIL on its last line.

- chaos/01_kill_worker.sh: kills worker mid-batch, asserts no event loss in Postgres
- chaos/02_postgres_outage.sh: stops Postgres for 30s, asserts events back up in Kafka and drain cleanly
- chaos/03_malformed_event.sh: posts invalid event, asserts dlq_events row + SQS notification
- chaos/04_kafka_latency.sh: 200ms latency injection, asserts ingest doesn't OOM, circuit breaker fires
- chaos/05_redis_outage.sh: stops Redis, asserts ingest fails open per configured policy
- chaos/06_consumer_rebalance.sh: triggers rebalance during sustained load, asserts no duplicates beyond dedup window
- chaos/07_clock_skew.sh: skews worker clock by ±5min, asserts idempotency still works
- chaos/run_all.sh: runs all scripts sequentially, prints final summary

Run git workflow. PAUSE.

## Step 8 — Load and perf regression

Generate `bench/k6/`:
- ingest_baseline.js: 1000 RPS sustained 5min, records p50/p95/p99
- burst.js: spike 100 → 5000 RPS, asserts rate limiter and 503 backpressure
- sustained.js: 1M events over 30min, asserts no loss + bounded queue depth
- mixed_workload.js: 70% small events, 25% medium, 5% large; realistic distribution
- bench/results/baseline.json: locked baseline (placeholder for first run)
- bench/results/README.md: how to update baseline

Generate `bench/compare.py` — script that diffs current k6 results against baseline.json with ±10% tolerance, exits non-zero on regression, prints markdown table for PR comments.

Run git workflow. PAUSE.

## Step 9 — CI/CD pipelines

Generate `.github/workflows/`:

ci.yml (push to main, PR): jobs lint (golangci-lint v1.60+), build, unit-test (race + coverage to Codecov), integration-test (-tags=integration with testcontainers-go), e2e (docker-compose smoke), docker-build (multi-arch via buildx; on tags push to ghcr.io with semver), helm-lint (helm lint + helm template + kubeconform against k8s 1.28/1.29/1.30), security (govulncheck + gitleaks + Trivy filesystem scan). Concurrency cancel-in-progress. All jobs except docker-build required for merge.

chaos.yml (nightly 02:00 UTC, manual): brings up docker-compose stack, runs chaos/run_all.sh, fails workflow if any chaos script fails, posts summary comment to latest commit.

bench.yml (PR + push to main, manual): smoke-bench job on PRs (1min @ 500 RPS, asserts p99 < 200ms); full-bench job on push to main runs all k6 scripts and uploads results as artifacts; posts markdown comparison table to PR via bench/compare.py. This is the perf regression gate.

perf-regression.yml (push to main, weekly schedule): pulls bench/results/baseline.json, runs full k6 suite for 10min, compares with ±10% tolerance, fails on regression with detailed diff in workflow summary. On release/* branches, auto-promotes results to baseline.json via github-actions[bot] commit with [skip ci].

release.yml (tags v*.*.*): builds + pushes Docker images to ghcr.io, packages Helm chart and pushes to gh-pages as Helm chart repo, generates release notes from CHANGELOG.md, creates GitHub Release with chart .tgz attached.

codeql.yml: GitHub default Go CodeQL, weekly schedule.

Generate `.github/dependabot.yml`: gomod + github-actions + docker, weekly, grouped minor/patch.

Generate `.github/CODEOWNERS`: @officialasishkumar owns everything.

Generate `.github/PULL_REQUEST_TEMPLATE.md` and `.github/ISSUE_TEMPLATE/` (bug_report.md, feature_request.md, design_discussion.md).

Run git workflow (separate commits per workflow). PAUSE.

## Step 10 — Docs and the public face

Generate `README.md`:
- One-line problem-focused pitch (no "I built")
- "Why StreamForge" — 3-4 sentences of pain
- Quickstart: docker-compose up + curl + Grafana dashboard, runnable in <5min
- Architecture: ONE Mermaid diagram, prose-led
- Configuration: pointer to streamforge.yaml
- Operations: failure modes (Postgres down, worker crash, Kafka rebalance, malformed event, DLQ inspection, backfill via replay)
- Deployment: docker-compose / kind / production Kubernetes
- Limitations: 3-5 honest trade-offs (single-region, Postgres analytics ceiling ~50k events/sec → graduate to ClickHouse, no built-in alerting beyond Grafana thresholds, replay throughput limited by S3 list pagination, schema cache eventually consistent across ingest replicas with 60s lag)
- License (Apache 2.0), Contributing pointer

Generate `CONTRIBUTING.md` (dev setup in 5 commands).

Generate `CHANGELOG.md` in Keep-a-Changelog format with v0.1.0, v0.2.0, v0.3.0 entries.

Generate `docs/postmortems/2026-04-15-postgres-pool-exhaustion.md`: realistic incident — date, severity, duration, summary, timeline (what was observed when), root cause (worker held Postgres connection across S3 write under load), fix (move S3 write outside transaction; cap pool wait to 2s), follow-ups (action items).

Generate `docs/architecture.md`, `docs/operations.md`, `docs/replay.md` (deeper than README).

Generate 5 GitHub Issues to open via gh issue create:
- "Discussion: should DLQ events be auto-retried after a configurable cool-down?"
- "Investigate: pgx vs database/sql for batch inserts under contention"
- "Proposal: pluggable archive sinks beyond S3 (GCS, Azure Blob)"
- "Design: cross-tenant fairness when one tenant floods the queue"
- "Investigate: ClickHouse sink for tenants beyond 50k events/sec"

After commits + push, run gh issue create for each.

Run git workflow (separate commits: README, CONTRIBUTING, CHANGELOG, postmortem, deeper docs). PAUSE.

## Step 11 — Adversarial cleanup

Act as a hostile staff engineer reviewing this repo for the first time during a hiring loop. Walk through the codebase, README, and workflows. List the top 15 things that signal "AI-generated portfolio code" vs. "real production code". For each: specific file/line and the fix. Be brutal, not charitable.

Then apply the fixes and run git workflow with commit message `refactor: address adversarial review findings` listing each fix in the body.

PAUSE.

# CONFIRMATION GATE

After step 0, print:
- Repo URL: https://github.com/officialasishkumar/streamforge
- First commit SHA + URL
- What's next (step 1 summary)

Then wait for "continue".

Begin step 0 now.