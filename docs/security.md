# Security and observability (phase 6)

The deployment is a single-operator, trusted-local system (design §15.1): one
namespace, no per-user authorization. This phase makes the boundary around that
operator enforceable and the service observable without leaking query text or
credentials.

## Browser-request guard (`internal/guard`, design §15.2)
Wraps every API route, including `/healthz`, `/readyz` and `/metrics`:

1. **Host allowlist** (`ALLOWED_HOSTS`, default loopback + dashboard + `api:8080`).
   A DNS-rebinding page presents its own domain and gets `403 forbidden`.
2. **Origin**, when present, must be an exact allowlisted origin
   (`ALLOWED_ORIGINS`). Absence is not an error.
3. **`Sec-Fetch-Site: cross-site`** is refused; `same-origin`, `same-site`,
   `none` and absent are accepted.
4. **CSRF proof**: every `POST/PUT/PATCH/DELETE` needs `X-Lakehouse-CSRF: 1`,
   whether or not an Origin is present. The web client adds it to every
   mutation. A foreign page cannot add a custom header without a preflight, and
   the preflight is refused for foreign origins.
5. **CORS** only echoes an exact allowed origin (never `*`).
6. **`API_TOKEN`** (≥ 16 chars, optional, for CLI/scripts): bearer, constant-time
   compare, `401` otherwise. `/healthz` is exempt from the token (not from the
   Host check) so container healthchecks work. The dashboard never holds the
   token, so token mode needs a same-origin proxy that injects it.
Rejections use the §11 envelope and never echo the offending header values.
nginx forwards `Host $http_host` (with the port) so the allowlist matches.

## SQL and resource boundary
Already enforced in phase 4 (`docs/queries.md`). Phase 6 adds adversarial
coverage: staging never crosses prefix boundaries (stale attempts, other
versions, sibling datasets sharing a name prefix, `attempt=10` vs `attempt=1`,
`uploads/`, other queries' `exports/`, `staging/`); the sealed engine reports
the configured thread/memory/external-access settings and refuses to change
them; panics and runaway aggregates are contained. See the Known limits in
`docs/queries.md` (notably that `memory_limit` does not bound one huge string,
so the worker container memory limit is the real backstop).

## Credentials and least privilege
Credentials exist only in service environment variables. They are not in the
bundle (`scripts/check-bundle.sh`), API responses, metrics, or logs: the
logger redacts credential-like keys, statement text keys (`statement`, `sql`,
…) and `scheme://user:password@` inside any string or error.

`ops/minio-policies/` holds three object-store policies, pinned by a test
(`internal/config/policies_test.go`) that checks which actions each may take on
which prefix:

| Principal | Read | Write / delete |
|---|---|---|
| API | `lake/`, `exports/`, `data/`, `uploads/` | `uploads/` only |
| Ingestion worker | `uploads/`, `data/`, `lake/`, `staging/` | `lake/`, `staging/`; delete `uploads/`, `exports/` (sweeper) |
| Query worker (`QUERY_MINIO_ACCESS_KEY/SECRET_KEY`) | `lake/`, `data/` | `exports/` only |

The worker uses the query credential for query reads and result spools when
`QUERY_MINIO_*` is set, so a bug in the query path cannot write datasets.

**Not verified / not done:** the policy files are validated structurally and by
the test above, but were **not applied to a real MinIO** (the image cannot be
pulled in the build environment). The default Compose file still gives every
service the MinIO root credential; applying the policies means creating three
service accounts and setting each service's `MINIO_ACCESS_KEY/SECRET_KEY`.
Per-query STS credentials (design §7.4/§8) are not implemented; DuckDB itself
never holds a credential (it reads staged local files).

## Metrics (`internal/telemetry`, `/metrics`)
Hand-written Prometheus text exposition. **Every label value comes from a
closed set declared with the metric; anything else is recorded as `other`.**
Tests push 5,000 hostile label values and check the series count stays bounded,
check each emitted label value against the declared sets, and check that
statement text, object keys, ids and raw paths never appear.

| Metric | Labels |
|---|---|
| `lqs_http_requests_total`, `lqs_http_request_duration_seconds` | `route` (matched mux pattern), `class` |
| `lqs_query_jobs_total` | `outcome` |
| `lqs_query_failures_total` | `reason` |
| `lqs_query_duration_seconds`, `lqs_query_cancel_requests_total`, `lqs_query_rows_returned_total` | — |
| `lqs_ingest_jobs_total` / `_duration_seconds` | `outcome` |
| `lqs_queue_depth` | `kind`, `state` |
| `lqs_dependency_up` | `dependency` (`postgres`, `minio`) |
| `lqs_worker_up`, `lqs_worker_inflight_queries`, `lqs_worker_query_slots` | — |
| `lqs_sweeper_deleted_total` | `kind` |
| `lqs_build_info` | `version` |

The API serves `/metrics` (behind the guard); the worker serves its own on
`METRICS_ADDR` (default `127.0.0.1:9100`, `:9100` in Compose, **not published**).
Queue depth and dependency health are computed at scrape time from the catalog
and the same probes `/readyz` uses.

## Logs and traces
Structured JSON logs. Each request logs `request_id`, `method`, the matched
**route pattern**, `status`, `duration_ms` — never the raw path or query string.
An inbound `X-Request-Id` is only honoured if it is a short safe token (no log
injection); the ID is echoed in the response. Query/ingestion jobs log ids,
outcomes, durations, row counts and `statement_hash`, never statement text.

**Tracing is not implemented.** There is no OpenTelemetry/OTLP export in this
phase; request IDs and job IDs in logs are the correlation mechanism. This is a
gap against the plan.

## Retention and cleanup (`internal/sweeper`, worker)
| Setting | Default | Effect |
|---|---|---|
| `EXPORT_TTL` | 24h | Result spools older than this are removed; the job keeps its status but loses `result_ref`, so the API answers `410 result_expired` |
| `HISTORY_RETENTION` | 720h | Finished query rows, **statement text included**, are deleted (never QUEUED/RUNNING rows). Must be ≥ `EXPORT_TTL` |
| `STAGING_TTL` | 24h | Orphaned `staging/` objects removed. Must be ≥ 2h so it outlives an ingestion lease |
| `SWEEP_INTERVAL` | 10m | |

The sweeper deletes **only** under `exports/` and `staging/`, refuses any key
containing `..`, and keeps objects whose age is unknown. A corrupted
`result_ref` pointing at `lake/` is ignored (tested). Each pass is capped at
10,000 deletions. This closes the phase-3 and phase-4 "no sweeper yet" gaps for
exports and staging; abandoned `lake/` attempt prefixes from superseded
ingestion attempts are still not reclaimed.

## Operations dashboard (opt-in, separate from the SQL dashboard)
`docker compose --profile ops up -d` starts Prometheus (`127.0.0.1:9090`) and
Grafana (`127.0.0.1:3000`, admin password generated into `.env` by
`make up`, anonymous access off) with the provisioned **Lakehouse service
operations** dashboard: dependency/worker status, queue depth, slot use, query
outcomes/failures/latency, cancels, API rate/latency, ingestion outcomes and
sweeps. `ops/prometheus/alerts.yml` has starter alert rules. A test requires
that every metric the dashboard and alerts name is actually exported and that
the dashboard never groups by id/statement/key-like labels.

**Not verified:** the Prometheus and Grafana containers were not started (their
images could not be pulled in the build environment); the Compose profile
validates with `docker compose config`, and the expressions are checked against
the real metric names, but they have not been run against live data.

## Tests added
Guard (host/origin/fetch-site/CSRF/CORS/token matrix, no echo of caller input,
every real route protected); metrics (closed label sets, bounded series, route
labels); sentinels (SQL literals, identifiers, paths, object keys, bearer and
password values never reach logs or metrics across success/failure/denial/
panic/timeout/cancel); sweeper (TTL, retention, never touches `lake/`/`uploads/`/
unknown age/active queries, idempotent); policies; config bounds; and Postgres
integration for expiry, purge and queue stats.
