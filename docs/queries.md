# Query engine and durable query jobs (phase 4)

Users submit **one read-only SQL statement** over registered or ingested
datasets. A durable job is queued in PostgreSQL, a worker runs it on
**single-node DuckDB**, the bounded result is spooled to object storage as
Parquet, and the API pages or exports it. This is a single-node query layer; it
does not shard, replicate, or scale out. The engine sits behind
`internal/query.Engine`.

Normative contracts (SQL boundary §7, paging/export §9, limits §12, job
lifecycle §13) stay in `docs/design.md`. This page records what phase 4
implements and where it deliberately differs.

## API

| Route | Purpose |
|---|---|
| `POST /api/v1/queries` `{"statement": "..."}` | Validate, resolve relations, queue. `202` with the job; `400 query_denied`; `429 resource_exhausted` when the queue is full |
| `GET /api/v1/queries?offset=&limit=` | History (newest first, limit ≤ 100). Includes the statement |
| `GET /api/v1/queries/{id}` | Status: state, failure reason, rows, truncation, duration, bounded plan summary, `bytesStaged` |
| `POST /api/v1/queries/{id}/cancel` | `202`. A QUEUED query is cancelled at once; a RUNNING one is flagged and stopped by its worker. `409 invalid_state` when already terminal |
| `GET /api/v1/queries/{id}/results?offset=&limit=` | Page of rows as arrays aligned with `columns`. `limit` ≤ 10,000; also bounded by `MAX_PAGE_BYTES`; `nextOffset` is `null` at the end |
| `GET /api/v1/queries/{id}/export` | CSV attachment. Bounded by `MAX_EXPORT_ROWS` and `MAX_EXPORT_BYTES`; `X-Export-Rows` / `X-Export-Truncated` headers |

Referencing data: `FROM sales` (latest READY version at submit time) or
`FROM catalog.sales_v2` (an exact version). The versions are resolved at submit
and stored with the job, so a re-run after restart reads the same data.

Errors use the §11 envelope. `query_denied` is `400`, `invalid_state` `409`,
`result_expired` `410`, `resource_exhausted` `429`. Messages never echo the
statement.

## Job state machine

`QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED | TIMED_OUT`, backed by the
`query_jobs` table (migration `0007`).

* Claim: `FOR UPDATE SKIP LOCKED`, lease (`QUERY_LEASE_DURATION`, default 120s),
  `attempt + 1`, fresh fencing token.
* Heartbeat every `QUERY_HEARTBEAT_INTERVAL` renews the lease **and** carries
  the cancel flag back to the worker. A heartbeat or any transition with a stale
  token matches zero rows (`ErrFenced`); the stale worker discards its work.
* `SUCCEEDED` is recorded only after the spool object exists, in one
  token-conditional update. A fenced worker deletes its own attempt-scoped
  spool.
* Worker crash: the lease expires and the job is **requeued** (never silently
  succeeded) and re-executed from the persisted statement. After
  `MaxQueryAttempts` (3) claims it is `FAILED` with reason `worker_interrupted`
  so a statement that kills its worker cannot loop forever. (The attempt cap is
  an addition to the design.)
* Graceful shutdown: an interrupted query is deliberately left `RUNNING`; its
  lease expires and it is requeued. Shutdown is not recorded as a cancellation.
* Timeout is `TIMED_OUT`, distinct from `FAILED`. The deadline is a Go context
  that interrupts DuckDB.
* Concurrency: `QUERY_WORKERS` (1–16, default 2) bounds RUNNING queries in a
  pool separate from ingestion. `QUEUE_DEPTH` (default 100) bounds the combined
  count of QUEUED query and ingestion jobs (advisory-locked so racing submits
  cannot overshoot).

## SQL safety boundary

Layers, outermost first. No keyword filtering is involved.

1. **Real parser, closed allowlists** (`internal/query/validate.go`). The
   statement is parsed by DuckDB's own parser via `json_serialize_sql`, which
   returns the AST without binding or executing. Exactly one statement; it must
   be a `SELECT`, a `WITH … SELECT`, or a set operation of selects (DDL, DML,
   `COPY`, `ATTACH`, `PRAGMA`, `SET`, `CALL`, `EXPLAIN`, top-level `VALUES`,
   etc. all fail to serialize and are denied). The AST is walked deny-by-default:
   expression classes, table-reference kinds, functions, and operators must be
   on compile-time lists (`allowlist.go`), not derived from DuckDB's catalog.
   Only `unnest` is an allowed table function. Lambdas, parameters, `PIVOT`,
   `AT` time travel, recursive CTEs, and catalog/schema-qualified relations or
   functions are denied. Table references must be CTE names or datasets that
   resolve to a READY version. The statement is re-validated by the worker on
   claim, and relations are re-bound from the persisted version ids.
2. **Server-side binding.** Relations are views the worker creates over files it
   staged itself (below). The user can never name a path or URL.
3. **No credentials in the engine.** See deviations: DuckDB receives local files
   only, so there is no object-store credential to misuse.
4. **Sealed engine configuration.** The views are built in a private database
   file, which is then reopened `access_mode=read_only` and sealed:
   `enable_external_access=false`, `allowed_directories` limited to the query's
   own scratch directories, extension autoload/install and community and
   unsigned extensions off, `memory_limit`, `threads`, `max_temp_directory_size`,
   `lock_configuration=true`. `TestEngineDeniesWhatTheValidatorWould` sends
   `read_csv`, `read_parquet` (local, other dataset, `s3://`, `http://`),
   `glob`, `read_text`, `COPY`, `ATTACH`, `INSTALL`/`LOAD`, `SET`/`PRAGMA`
   re-enabling access, and `CREATE TABLE` straight to the sealed engine,
   bypassing the validator, and requires each to fail.

Known limits of this boundary:

* **`memory_limit` does not bound a single huge string.** A 4 GB `repeat()` was
  measured materializing under a 64 MiB limit, and the Go driver then panics on
  values over 2 GiB. So `repeat`, `lpad`, and `rpad` only accept a constant
  length of at most 1 MiB, driver panics are recovered per query and per job,
  and the compose worker has a container memory limit (`WORKER_MEM_LIMIT`,
  default 6g) as the real backstop. Other string-amplifying functions
  (`replace`, `concat`, `string_agg`) are bounded only by data size × statement
  size, then by the limit above.
* DuckDB lets a query call some zero-argument functions without parentheses
  (`current_user`, …), which the parser reports as column references. A fixed
  denylist (`bareFunctions`) covers the known ones; the engine seal and the
  absence of secrets limit the impact of any that are missed.
* The caller is a trusted operator (design §15). A dataset registered at an
  arbitrary prefix is readable by queries; there is no per-user authorization.

## Execution and results

* The worker lists the version's `object_prefix`, downloads each `.parquet`
  object (bounded per query by `MAX_SCAN_BYTES`, default 1 GiB, and 10,000
  files) into a private `0700` scratch directory preserving relative keys, so
  hive partition directories survive. Registered partitioned data is read with
  `hive_partitioning=true`; ingested data keeps partition columns in the files
  and is not decoded twice. The scratch directory is deleted after every query;
  stale ones from crashed workers are removed at worker start.
* Rows are materialized to at most `MAX_RESULT_ROWS + 1`; the extra row is a
  probe that is discarded, so `truncated` is exact. A row over `MAX_ROW_BYTES`
  fails the query with `resource_exceeded` and no spool. The spool is encoded,
  measured on the **finalized** file, and shrunk to the largest whole-row prefix
  that fits `MAX_RESULT_BYTES` (`truncated=true`).
* Spool: `exports/default/<query-id>/attempt=<n>/result.parquet`, written with
  the in-repo Parquet writer (PLAIN, uncompressed). Booleans, integers up to
  32-bit and `BIGINT`, and floating point keep native types; everything else
  (decimals, dates/timestamps, `UBIGINT`, `HUGEINT`, nested values, blobs) is
  spooled as text.
* Paging reads the spool with `parquet.ReadRows` and emits whole rows until
  `limit` or `MAX_PAGE_BYTES`. A first row larger than the budget is
  `resource_exhausted`, never an empty page. Export measures its own CSV
  encoding, so the JSON and CSV bounds are independent (a control character is
  6 bytes in JSON, 1 in CSV; `TestQueryEncodingsAreBoundedIndependently`). CSV
  cells that begin with `= + - @` tab or CR are prefixed with `'` against
  spreadsheet formula injection.
* Recorded per job: duration, rows, truncation, SHA-256 statement hash, a plan
  summary (DuckDB `EXPLAIN`, 4 KiB, local paths replaced), result bytes, and
  bytes staged. The statement text and credentials are never logged. Failure
  summaries for engine errors may carry the first line of a DuckDB binder,
  catalog, conversion or parser message, which can mention a column name or a
  data value from the caller's own query; other errors are generic.

## Deviations from the phase-0 design

| Design | Implemented | Why |
|---|---|---|
| DuckDB reads S3 through `httpfs` with a per-query STS credential and a scoped secret (§7.4 layers 0, 3) | The worker downloads the files with its S3 client; DuckDB reads local files and holds **no** credentials or object URLs. `httpfs` is not loaded | The bundled DuckDB build ships no `httpfs`; runtime extension downloads were rejected. This removes the credential-fallback risk entirely but costs a copy per query |
| Per-query / per-export scoped STS read credential (§8, §9) | The API and worker use the deployment object-store credential | No STS in this phase. Tracked for phase 6 |
| `bytes_scanned` from the engine | `bytesStaged` (bytes downloaded) | DuckDB's Go driver does not expose scan bytes |
| Export TTL via lifecycle rule and sweeper (§9, §10) | None yet. A missing spool returns `410 result_expired` | Sweeper is phase 6 |
| `attempt` incremented at requeue | Incremented at each claim; requeue only resets state | Equivalent monotonic attempt count; one increment per claim |
| `exports/<namespace>/…` namespace | Always `default` | Single namespace in v1 |
| Result types | Primitive + text spool | Writer limits (see above) |
| Execution timeout covers the whole job | Covers DuckDB execution only; staging is bounded by size, not time | Heartbeats keep the lease alive while staging |

## Engine pin

DuckDB **v1.4.1** through `github.com/marcboeker/go-duckdb/v2 v2.4.3`
(`duckdb-go-bindings v0.1.21`), pinned in `go.mod`. It requires cgo, so the API
and worker are built with a C/C++ toolchain on glibc (the Dockerfile is Debian
based). The bundled extensions are `core_functions`, `icu`, `json`, `parquet`,
and `autocomplete`; no extension is loaded or installable at query time.

## Testing

* Unit: validator allow/deny tables, engine limits and sealed-engine denials,
  runner (success, denial, cancel, timeout, fencing, shutdown, requeue,
  concurrency, limits, panic), API (submit/poll/page/export, byte bounds, errors).
* Integration (`make test-integration`, needs `TEST_POSTGRES_URL`): claim
  exclusivity under 12 racing workers, lease expiry → requeue → fencing of the
  stale worker, repeated-death failure, cancel, combined queue depth under
  racing submits, relation resolution, and a real DuckDB query joining a
  hive-partitioned registered dataset with a flat one.
* **Not covered here:** a MinIO-backed run (the MinIO image could not be pulled
  in the build environment), so object storage is the in-memory store in these
  tests and the S3 client keeps its own HTTP-level tests; a full
  `docker compose up` run; the dashboard (phase 5).
