---
description: Go implementation and quality constraints
globs:
  - "**/*.go"
---

# Go Style Rules

- Keep package names lowercase and concise.
- Use `context.Context` as the first parameter for all I/O and external dependency functions.
- Wrap errors with contextual prefixes and `%w`, for example `fmt.Errorf("ingest: validate event: %w", err)`.
- Avoid panics outside `main` packages and tests.
- Avoid global state except Prometheus collectors explicitly registered in `init`.
- Use structured JSON `slog` logging in non-test runtime paths.
- Enforce quality gates with `golangci-lint` including `errcheck`, `gosec`, `govet`, `staticcheck`, and `gofumpt`.
- Avoid `SELECT *`, avoid N+1 access patterns, and avoid `time.Sleep` in production logic; prefer context deadlines.
- Ensure every external dependency path includes circuit-breaker handling and bounded retry behavior.
