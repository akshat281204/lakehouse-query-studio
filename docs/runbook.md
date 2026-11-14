# Runbook

How to run, inspect, troubleshoot and clean up the stack, and exactly what has
and has not been validated.

## Prerequisites

* **Docker** with the Compose v2 plugin (`docker compose`)
* **Go 1.25+ with a C/C++ compiler** (the DuckDB engine is linked in through cgo)
* **make**; **Node 22 / npm** only if you run the dashboard outside Docker or run `make verify`
* Free loopback ports: 8080 (API), 5173 (dashboard), 8333 (object store), 55432 (PostgreSQL).
  Change any of them with `API_HOST_PORT`, `WEB_HOST_PORT`, `S3_HOST_PORT`,
  `POSTGRES_HOST_PORT` in `.env`; the API's Host allowlist follows them.

## Run the demo

```bash
make demo
```

Steps, each bounded: generate `.env` and `.s3-config.json` (gitignored, local
random credentials) → `docker compose up -d --build --wait` (300 s) →
`go run ./cmd/demo` (300 s budget): wait for `/readyz`, load `demo-sales`
(2,000 deterministic rows) if it is not already loaded, run
`SELECT region, count(*), sum(quantity * unit_price) … GROUP BY region`, verify
the numbers against values computed in Go, print the URLs.

Safe to rerun. It only reads or creates the dataset named `demo-sales`, never
deletes anything, and reuses an existing load. If a dataset of that name already
holds *other* data, the demo prints a note instead of failing.

Then open the dashboard (default `http://localhost:5173/`): **Overview** for
health and activity, **SQL** to run queries (click the dataset name in the
schema browser to insert it), **History** to rerun, **Upload** to add data.

## What runs where

| Service | Image / process | Host port | Notes |
|---|---|---|---|
| `postgres` | `postgres:16-alpine` | 55432 | Catalog and job state |
| `s3` | `chrislusf/seaweedfs:4.05` | 8333 | S3-compatible object store; identity generated into `.s3-config.json` |
| `api` | built from `Dockerfile` | 8080 | `/healthz`, `/readyz`, `/metrics`, `/api/v1/*` |
| `worker` | built from `Dockerfile` | none (`:9100` metrics, internal) | Ingestion pool, query pool, retention sweeper |
| `web` | built from `web/Dockerfile` (nginx) | 5173 | Static dashboard; proxies `/api` to the API |
| `prometheus`, `grafana` | opt-in `--profile ops` | 9090, 3000 | Operations dashboard; Grafana admin password is in `.env` |

All published ports bind to `127.0.0.1` only. **Do not expose them or reuse the
generated credentials in a shared deployment.**

The community MinIO container images are no longer published (the
`minio/minio` repositories are gone), which is why the default store is
SeaweedFS. The applications speak plain S3 and keep the historical `MINIO_*`
variable names, so any S3-compatible store works: point `MINIO_ENDPOINT`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` at it.

## Develop without building images

```bash
docker compose up -d --wait postgres s3        # dependencies only
set -a; . ./.env; set +a
export POSTGRES_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:55432/$POSTGRES_DB?sslmode=disable"
export MINIO_ENDPOINT=127.0.0.1:8333 MINIO_ACCESS_KEY=$MINIO_ROOT_USER MINIO_SECRET_KEY=$MINIO_ROOT_PASSWORD MINIO_BUCKET=lakehouse
go run ./cmd/api &            # :8080
go run ./cmd/worker &         # metrics on 127.0.0.1:9100
(cd web && npm run dev)       # http://127.0.0.1:5173, proxies /api to :8080
go run ./cmd/demo
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `bind: address already in use` on `docker compose up` | Another process owns a published port. Set the matching `*_HOST_PORT` in `.env` (PostgreSQL defaults to 55432 to avoid a local 5432) |
| Dashboard shows errors; API returns `403 forbidden` ("request host is not allowed") | The browser's `Host` is not on the allowlist. Use `localhost`/`127.0.0.1` with the configured ports, or set `ALLOWED_HOSTS` / `ALLOWED_ORIGINS` |
| `403` with "missing X-Lakehouse-CSRF header" | A mutation without the header. The dashboard and `make demo` send it; add `-H 'X-Lakehouse-CSRF: 1'` to your own `curl -X POST` |
| `401 unauthorized` | `API_TOKEN` is set; send `Authorization: Bearer <token>` (the browser dashboard cannot hold the token, so token mode is for CLI use) |
| Overview or sidebar shows **a dependency is down** | Open Overview > System health for the failing dependency and message. Object store: check `docker compose logs s3` and that `.s3-config.json` is a file, not a directory (delete it and run `make up` again) |
| `demo` waits then fails with "objectstore is not ready (…)" | The S3 service is up but rejecting the signed probe: credentials in `.env` and `.s3-config.json` differ. Run `make up` to regenerate both consistently |
| Queries stay QUEUED | The worker is not running or cannot reach PostgreSQL: `docker compose logs worker`, and check the "In flight" KPI |
| `query_denied` on submit | The statement is not a single read-only `SELECT` over a READY dataset, or it names an unknown dataset. Quote names containing `-` (`"demo-sales"`) |
| `result_expired` (410) | The result spool is older than `EXPORT_TTL` (24 h) and was swept; rerun the query |
| `docker compose up --build` hangs on "load metadata" / pulling base images | Docker Hub throttling or network. Retry later, or `docker pull golang:1.25-bookworm debian:bookworm-slim node:22-alpine nginx:1.27-alpine` first. Use "Develop without building images" meanwhile |
| Port 5173 shows an old dashboard | A stale Vite dev server is running (`lsof -nP -iTCP:5173`) |

## Cleanup

| Command | Effect |
|---|---|
| `make down` | Stops and removes the containers. **Keeps** all data (named volumes `pgdata`, `s3data`) |
| `docker compose down -v` | Also **deletes the volumes**: all datasets, query history and results. Run only when you want a blank slate |
| delete `.env`, `.s3-config.json` | Forgets the generated credentials. Do this only together with `down -v`, or the stack will not be able to read its old data |

The demo itself never runs any of these.

## Inspecting a running stack

```bash
curl -s http://127.0.0.1:8080/readyz            # per-dependency readiness
curl -s http://127.0.0.1:8080/metrics | head    # Prometheus metrics (API)
curl -s "http://127.0.0.1:8080/api/v1/stats?window=1h"
docker compose logs -f worker                   # structured logs; never contain SQL text or credentials
docker compose --profile ops up -d              # Prometheus :9090, Grafana :3000
```

## Tests

```bash
make verify                                  # everything that needs no external service
make test-integration                        # + PostgreSQL (TEST_POSTGRES_URL, a throwaway DB: tests delete rows)
make bench                                   # benchmarks (docs/evaluation.md)
```

## Validation status

What was exercised when this phase was validated, and what was not:

| Item | Status |
|---|---|
| `make verify` (format, vet, lint, Go + web tests, typecheck, builds, bundle-secret check) | Passes |
| Postgres-backed integration tests | Pass against PostgreSQL 16 |
| Real S3 client against a real S3-compatible server | Passes against SeaweedFS (`internal/objectstore/s3_integration_test.go`); **not** against MinIO, whose images are unavailable |
| Compose `postgres` and `s3` services (images, healthchecks, generated S3 identity) | Started healthy |
| API and worker binaries against those real services + `go run ./cmd/demo` | **Run: completes in about 3.4 s, answer verified, rerun reuses the dataset (1 ingestion job, 1 dataset row)** |
| Dashboard in a real browser (headless Chrome, light and dark) against the live API | Loaded; screenshots in `docs/images/` |
| `docker compose up --build` of the `api`, `worker`, `web` images and the single `make demo` command end to end | **Not completed here.** The `api`/`worker` image built earlier (Debian + cgo, no missing shared libraries); the `web` image was never built here. In the final validation environment Docker Hub base-image pulls hung, so the full one-command run could not be demonstrated. The demo driver and the stack it talks to were validated separately as above |
| Prometheus / Grafana containers | Not started in this validation; the configuration validates with `docker compose config` and the metric names they use are checked by a test |
| Object-store least-privilege policies | Not applied to any server (`docs/security.md`) |
