# Contributing

## Development Setup (5 commands)

1. `git clone https://github.com/officialasishkumar/streamforge.git`
2. `cd streamforge`
3. `go mod tidy -go=1.22`
4. `docker compose up -d`
5. `go test ./...`

## Workflow Expectations

- Keep commits atomic and use Conventional Commits.
- Run `go test ./...` and relevant integration or bench commands before opening PRs.
- Keep operational docs updated when changing behavior in failure paths.
