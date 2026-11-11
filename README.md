# Lakehouse Query Studio

A local-first analytical query workbench over S3-compatible object storage.
Users store or register Parquet datasets in S3/MinIO, discover them in a web
dashboard, and run bounded SQL against them from an embedded query platform.
The initial engine is **single-node DuckDB**: this is a query layer over object
storage, not a distributed database.

> **Single-node limit.** The first release runs exactly one DuckDB node and does
> not shard, replicate, or scale out. It must not be described as a distributed
> OLAP database. The query engine sits behind `internal/query.Engine` (added in
> a later phase) so a measured need could justify a distributed engine later.

## Repository layout

| Path | Purpose |
|---|---|
| `cmd/api` | Go control-plane process (health, readiness, config) |
| `cmd/worker` | Query/ingestion worker placeholder (phase 1) |
| `internal/` | Shared Go packages: config, logging, health, server |
| `web/` | React + TypeScript + Vite dashboard with a typed API client |
| `docker-compose.yml` | Local Postgres, MinIO, API, worker, and web |
| `Makefile` | `up`, `down`, `run`, `web`, `test`, `verify` targets |
| `docs/` | Design contract and architecture (phase 0) |

## Prerequisites

- **Go** 1.25 or newer
- **Node.js** 22 and **npm** 11
- **Docker** with the Compose v2 plugin (`docker compose`)
- **golangci-lint** (optional; `make verify` falls back to `go vet` when absent)

## Clean start

```bash
make up
```

`make up` builds and starts PostgreSQL, MinIO, the API, the worker placeholder,
and the web dashboard, then waits for the dependency services to become healthy.
Once up:

- Dashboard: <http://localhost:5173>
- API: <http://localhost:8080> (see `/healthz` and `/readyz`)
- MinIO console: <http://localhost:9001>

Stop the stack with `make down`. Named volumes (`pgdata`, `miniodata`) persist
data across restarts.

## Configuration

The API and worker read configuration from environment variables. Local
development defaults are listed in [`.env.example`](.env.example); copy it to
`.env` to override values.

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API listen address |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful-shutdown budget |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `POSTGRES_URL` | _(empty)_ | PostgreSQL URL (`postgres://…`). Empty disables the check |
| `MINIO_ENDPOINT` | `localhost:9000` | Object-store endpoint (`host:port`) |
| `MINIO_ACCESS_KEY` | _(empty)_ | Object-store access key (server-side only) |
| `MINIO_SECRET_KEY` | _(empty)_ | Object-store secret key (server-side only) |
| `MINIO_BUCKET` | `lakehouse` | Object-store bucket |
| `MINIO_USE_SSL` | `false` | Use HTTPS for the object store |
| `API_TOKEN` | _(empty)_ | Optional single deployment token |

Secrets are never committed, logged, or shipped to the browser bundle. Logging
is structured (`log/slog`) and redacts sensitive fields.

## Verification

```bash
make verify
```

`make verify` runs, in order: Go formatting (`gofmt`) and web formatting
(Prettier), `go vet`, the Go linter (golangci-lint when available, else `go vet`)
plus web ESLint, Go unit/race tests (`go test -race ./...`) and web tests
(Vitest), web typechecking (`tsc`), and Go build (`go build ./...`) plus the web
production build. **It does not require a running MinIO or PostgreSQL instance.**

Individual targets: `make test` (Go + web tests), `make run` (API on the host),
and `make web` (Vite dev server with hot reload).

## API (phase 1)

- `GET /` — service metadata (`{ "service": "lakehouse-api", "version": … }`)
- `GET /api/v1/info` — service metadata for the web app (same payload as `/`);
  the dashboard calls this route because `/` is served as the frontend entry
  point by both the Vite dev server and the nginx SPA fallback
- `GET /healthz` — liveness (`{ "status": "ok" }`)
- `GET /readyz` — readiness; reports each dependency (`postgres`, `minio`) and
  returns `200` only when all are healthy, otherwise `503`

Later phases add the versioned dataset and query API under `/api/v1`.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the same `make
verify` verification on Go and web code on every push to `main` and every pull
request, plus golangci-lint for Go.
