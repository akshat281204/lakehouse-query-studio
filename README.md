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
| `cmd/worker` | Worker: ingestion jobs (phase 3) and query jobs on DuckDB (phase 4) |
| `internal/` | Shared Go packages: config, logging, health, server |
| `web/` | React + TypeScript + Vite dashboard with a typed API client |
| `docker-compose.yml` | Local Postgres, MinIO, API, worker, and web |
| `Makefile` | `up`, `down`, `run`, `web`, `test`, `verify` targets |
| `docs/` | Design contract (phase 0), catalog (phase 2), ingestion (phase 3), queries (phase 4), dashboard (phase 5) |

## Prerequisites

- **Go** 1.25 or newer, with **cgo** and a C/C++ compiler (the DuckDB engine is linked in)
- **Node.js** 22 and **npm** 11
- **Docker** with the Compose v2 plugin (`docker compose`)
- **golangci-lint** (optional; `make verify` falls back to `go vet` when absent)

## Clean start

```bash
make up
```

`make up` first writes a gitignored `.env` with freshly generated local
credentials (via `scripts/genenv.sh`), then builds and starts PostgreSQL, MinIO,
the API, the worker placeholder, and the web dashboard, and waits for the
dependency services to become healthy. Once up:

- Dashboard: <http://localhost:5173>
- API: <http://localhost:8080> (see `/healthz` and `/readyz`)
- MinIO console: <http://localhost:9001>

No credentials are committed to the repository: the local PostgreSQL and MinIO
secrets are generated on first boot into a gitignored `.env`. All published
ports are bound to `127.0.0.1` only. Do not expose these ports or reuse local
credentials in a shared deployment.

Stop the stack with `make down`. Named volumes (`pgdata`, `miniodata`) persist
data across restarts.

## Configuration

The API and worker read configuration from environment variables. Secrets are
never committed: `make up` generates a gitignored `.env` with random local
credentials via `scripts/genenv.sh`. [`.env.example`](.env.example) is a
reference that lists the variables; its secret fields are intentionally blank,
and the generator fills any blank secret it finds. To pin your own values, copy
`.env.example` to `.env` and fill the blank fields before running `make up`.

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
| `QUERY_WORKERS` | `2` | Concurrent running queries (1–16) |
| `QUERY_TIMEOUT` | `60s` | Query execution budget (max 300s) |
| `QUERY_LEASE_DURATION` | `120s` | Query lease (max 600s); heartbeat is a third of it |
| `MAX_RESULT_ROWS` / `MAX_RESULT_BYTES` / `MAX_ROW_BYTES` | `10000` / `268435456` / `1048576` | Result bounds; byte values are integer bytes (hard max 100000 / 1 GiB / 16 MiB) |
| `MAX_PAGE_BYTES` / `MAX_EXPORT_BYTES` / `MAX_EXPORT_ROWS` | `4194304` / `67108864` / `50000` | Paging and CSV export bounds (integer bytes) |
| `MAX_SCAN_BYTES` | `1073741824` | Dataset bytes a single query may stage (integer bytes) |
| `QUEUE_DEPTH` | `100` | Combined queued query + ingestion jobs |
| `DUCKDB_THREADS` / `DUCKDB_MEMORY` | `min(4, cpus)` / `2GiB` | Per-query DuckDB budgets |
| `QUERY_TMP_DIR` | OS temp dir | Parent of per-query scratch directories |

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

`make web-build` also runs `scripts/check-bundle.sh`, which fails if the bundle
or web source could expose credentials or use browser storage. See
`docs/dashboard.md`.

Individual targets: `make test` (Go + web tests), `make run` (API on the host),
and `make web` (Vite dev server with hot reload).

## API

- `GET /` — service metadata (`{ "service": "lakehouse-api", "version": … }`)
- `GET /api/v1/info` — service metadata for the web app (same payload as `/`);
  the dashboard calls this route because `/` is served as the frontend entry
  point by both the Vite dev server and the nginx SPA fallback
- `GET /healthz` — liveness (`{ "status": "ok" }`)
- `GET /readyz` — readiness; reports each dependency (`postgres`, `minio`) and
  returns `200` only when all are healthy, otherwise `503`
- `POST /api/v1/datasets` — register an existing Parquet prefix
  (`{ "name": "sales", "prefix": "data/sales" }`)
- `GET /api/v1/datasets` — paged, searchable dataset list (`offset`, `limit`,
  `q`)
- `GET /api/v1/datasets/{id}` — dataset detail with latest version and schema
- `GET /api/v1/datasets/{id}/schema` — schema columns
- `GET /api/v1/datasets/{id}/preview` — bounded sample rows
- `POST /api/v1/ingestions/inspect` — infer the schema of a CSV/JSON/JSONL body
  without storing it
- `POST /api/v1/ingestions?name=&filename=&partitionColumns=` — upload a file
  (raw body, up to 64 MiB) and queue a conversion job (`202`)
- `GET /api/v1/ingestions`, `GET /api/v1/ingestions/{id}` — list/poll jobs
- `POST /api/v1/ingestions/{id}/retry` — retry a failed, retryable job

- `POST /api/v1/queries` — queue one read-only SQL statement (`202`)
- `GET /api/v1/queries`, `GET /api/v1/queries/{id}` — history and status
- `POST /api/v1/queries/{id}/cancel` — cancel a queued or running query
- `GET /api/v1/queries/{id}/results?offset=&limit=` — paged, byte-bounded rows
- `GET /api/v1/queries/{id}/export` — bounded CSV download

See `docs/queries.md` for the SQL boundary, limits, job lifecycle, and
deviations from the phase-0 contract.

See `docs/ingestion.md` for limits, partitioning, recovery, and known
deviations from the phase-0 contract.

The dataset endpoints return `503 unavailable` when PostgreSQL or MinIO is not
configured. All non-2xx responses use a single error envelope
(`{ "error": { "code", "message", "details" } }`); see `docs/design.md` §11 and
`docs/catalog.md`.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the same `make
verify` verification on Go and web code on every push to `main` and every pull
request, plus golangci-lint for Go.
