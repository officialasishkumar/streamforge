# syntax=docker/dockerfile:1.7

FROM golang:1.25.9-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/streamforge-ingest ./cmd/ingest \
	&& CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/streamforge-worker ./cmd/worker \
	&& CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/streamforge-replay ./cmd/replay

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates tzdata \
	&& rm -rf /var/lib/apt/lists/* \
	&& groupadd --system streamforge \
	&& useradd --system --gid streamforge --home-dir /var/lib/streamforge streamforge

WORKDIR /etc/streamforge

COPY --from=build /out/streamforge-ingest /usr/local/bin/streamforge-ingest
COPY --from=build /out/streamforge-worker /usr/local/bin/streamforge-worker
COPY --from=build /out/streamforge-replay /usr/local/bin/streamforge-replay
COPY streamforge.yaml /etc/streamforge/streamforge.yaml

USER streamforge:streamforge

ENV STREAMFORGE_SERVICE=ingest
EXPOSE 8080 9090

ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/streamforge-${STREAMFORGE_SERVICE} \"$@\"", "--"]
