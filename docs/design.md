# Lakehouse Query Studio — Design Contract

> Phase 0 deliverable. Normative contract for data layout, catalog/version
> semantics, ingestion, query job lifecycle, the SQL safety boundary, result
> retention, and the single-node limit. Later phases implement against this
> document and must not silently contradict it.

## 1. Scope and status

This document resolves the design ambiguities listed in
`plan/00-design-contract.md`. It is descriptive (a design and contract), not
implementation code. It applies to the first release only: a single-node
DuckDB query layer over S3-compatible object storage.

Explicitly out of scope for this release (recorded here so nothing is promised
later):

- Distributed/multi-node query execution; any future engine (Trino, etc.) is
  deferred until a measured need justifies it.
- Multi-tenant identity, billing, governance, or a semantic BI layer.
- Arbitrary user-installed DuckDB extensions or unrestricted file/URL access.
- Iceberg/Delta/Hudi catalog semantics.

## 2. Terminology

| Term | Meaning |
|---|---|
| namespace | Logical grouping/tenant boundary. Exactly one default namespace (`default`) in v1. |
| dataset | A named, namespace-scoped logical collection of Parquet data with one or more versions. |
| dataset version | An immutable, versioned snapshot of a dataset pointing at a concrete object prefix. |
| ingestion job | Durable, asynchronous unit of work that registers or converts data into a dataset version. |
| query job | Durable, asynchronous unit of work that executes one validated read-only SQL statement. |
| object store | MinIO (local) or S3-compatible remote. Source of Parquet data. |
| catalog | PostgreSQL: durable metadata and job state. Never stores result payloads or credentials. |

## 3. Durable entities

Every entity that survives a process restart is listed here with its lifecycle
and state machine. "Durable" means stored in PostgreSQL (catalog) or object
storage (immutable data); in-memory state is always reconstructible.

### 3.1 Entity inventory

| Entity | Storage | Key fields | Terminal states |
|---|---|---|---|
| namespace | PostgreSQL | id, slug | ACTIVE, ARCHIVED |
| dataset | PostgreSQL | id (UUID), namespace_id, name, source_prefix, format, status, created_at, updated_at | ACTIVE, ARCHIVED |
| dataset_version | PostgreSQL | id, dataset_id, version (monotonic int), object_prefix, format, partition_columns, status, schema_snapshot_id, error_summary, created_at | READY, FAILED |
| schema_column | PostgreSQL | id, version_id, position, name, type, nullable, is_partition | — (rows) |
| ingestion_job | PostgreSQL | id (UUID), dataset_id, target_version, kind (register\|convert), status, input_ref, error_summary, retry_of, timestamps, lease_owner, lease_expires_at | SUCCEEDED, FAILED, CANCELLED |
| query_job | PostgreSQL | id (UUID), statement_hash, dataset_version_ids, status, failure_reason, rows_returned, bytes_scanned, result_ref, plan_summary, timestamps, lease_owner, lease_expires_at | SUCCEEDED, FAILED, CANCELLED, TIMED_OUT |
| object (Parquet) | object store | key under `lake/…` | — (immutable) |
| spooled result | object store | key under `exports/…` | — (TTL-expired) |

### 3.2 Dataset lifecycle

Dataset status: `ACTIVE` ↔ `ARCHIVED`.

- A dataset is created (ACTIVE) when its first version is requested.
- `ARCHIVED` is a soft delete: the dataset stops appearing in discovery and
  query views, but its versions and objects are retained per the retention
  policy (§10). Archive is reversible; hard delete is out of scope in v1.
- A dataset may have zero `READY` versions while its first version is being
  built or has failed; it is still an ACTIVE dataset record.

### 3.3 Dataset version lifecycle

Version status: `PENDING → BUILDING → READY | FAILED`.

| From | Event | To | Notes |
|---|---|---|---|
| (new) | ingestion/registration requested | PENDING | version row created, schema not yet exposed |
| PENDING | worker picks up job | BUILDING | schema inspection (register) or conversion (convert) in progress |
| BUILDING | all Parquet written + schema captured + catalog committed in one transaction | READY | becomes queryable atomically (§6) |
| BUILDING | unrecoverable error or cancellation | FAILED | error_summary set; staging objects cleaned; never READY |

- `READY` is the only state in which a version is exposed to the query engine
  or the dataset browser's schema/preview.
- `FAILED` is terminal for that version; a retry creates a new ingestion job on
  a fresh version number (an explicit `retry_of` link records the lineage).
- Version numbers are monotonically increasing integers per dataset and are
  never reused.

## 4. Object key layout

All keys are relative to the configured bucket. Layout is ASCII, URL-safe, and
deterministic; dataset and namespace are slug-normalized before use in keys.

```
# Converted / registered Parquet (immutable, the query source)
lake/<namespace>/<dataset>/version=<n>/[col=value/...]/part-<uuid>.parquet

# Original uploads (retained only when retention is enabled)
uploads/<namespace>/<dataset>/<job-id>/<sanitized-original-name>

# Ingestion staging (temporary; cleaned on success or failure)
staging/<namespace>/<dataset>/<job-id>/part-<uuid>.parquet

# Spooled query results and exports (temporary; TTL)
exports/<namespace>/<query-id>/result.parquet
exports/<namespace>/<query-id>/export.csv
```

Rules:

1. `lake/`, `uploads/`, `staging/`, and `exports/` are disjoint top-level
   prefixes; query access is scoped to `lake/` only.
2. Hive-style partition directories are `col=value/` segments in the
   dataset-declared partition order; unpartitioned datasets write files directly
   under `version=<n>/`.
3. Part files are written once and never mutated; a version commit is a
   catalog pointer update, not an object rewrite.
4. Original uploads are retained only when the dataset/namespace policy sets
   `retain_original=true`; otherwise upload objects are removed after the
   version reaches `READY`.
5. No object key appears in client-visible data (§8).

## 5. Catalog and version semantics

- A version's object prefix is `lake/<namespace>/<dataset>/version=<n>/`.
- The schema snapshot (columns, types, nullability, partition columns) is
  captured and stored with the version and becomes immutable once the version
  is `READY`.
- The catalog never stores credentials, secret keys, or raw result payloads.
- Timestamps are stored as UTC; `created_at`/`updated_at` on all mutable rows.
- Uniqueness: `(namespace, dataset name)` is unique among active datasets;
  version number is unique per dataset; job ids are UUIDs.

## 6. Ingestion behavior

Two kinds of ingestion job:

| Kind | Input | Action | Output |
|---|---|---|---|
| `register` | an existing Parquet prefix in the object store | validate prefix, inspect schema, capture partition metadata | version `READY` pointing at the existing prefix (objects are copied into `lake/…` when not already there, so a stable `lake/` view exists) |
| `convert` | uploaded CSV/JSON/JSONL | validate, infer schema, convert to Parquet under staging, then commit | version `READY` with new `lake/…` objects |

Common rules:

- Ingestion is asynchronous and persisted as an `ingestion_job`.
- Conversion writes to `staging/…` first and only "commits" by (a) placing
  complete Parquet parts into `lake/…/version=<n>/` and (b) updating the
  catalog to `READY` in the same logical commit. A partial version is never
  exposed as `READY`.
- Retry-safe: a retried job writes to a fresh staging directory; a successful
  retry does not create a second visible version.
- Failure leaves no dangling staging objects and no `READY` version.
- Partition columns are explicit and user-selected with a safe default (no
  partitioning, or only low-cardinality columns); high-cardinality partitioning
  is not applied by default.

## 7. SQL safety boundary (normative)

### 7.1 Allowed statements

Exactly one of the following, and nothing else:

- a single `SELECT` statement; or
- a single `SELECT` statement preceded by a `WITH` common-table-expression
  clause.

Rejected in all cases: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, any DDL
(`CREATE`, `DROP`, `ALTER`), `COPY`, `EXPORT DATABASE`, `ATTACH`, `DETACH`,
`INSTALL`, `LOAD`, `FORCE INSTALL`, `PRAGMA`, `SET`, `RESET`, `CALL`,
transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`), `USE`, multiple
statements (including semicolon-separated), user-supplied `EXPLAIN`/
`EXPLAIN ANALYZE`, and any `VALUES`-top-level statement.

### 7.2 Allowed functions

Functions are allow-listed by category. The lists below are representative
(the implementation enforces the allow-list from the parser/engine, and the
adversarial tests in §7.5 pin the boundary), and they are all read-only and
non-I/O.

Allowed categories:

- Comparison/logical/arithmetic operators; `CAST`, `::`, `TRY_CAST`, `typeof`.
- String: `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `length`, `substr`,
  `substring`, `concat`, `concat_ws`, `replace`, `reverse`, `split`,
  `string_split`, `position`, `instr`, `left`, `right`, `lpad`, `rpad`,
  `repeat`, `starts_with`, `ends_with`, `contains`, `like`/`ilike`,
  `regexp_matches`, `regexp_replace`, `regexp_extract`.
- Numeric/math: `abs`, `round`, `floor`, `ceil`/`ceiling`, `sqrt`,
  `pow`/`power`, `exp`, `ln`, `log`, `log10`, `sign`, `mod`, `greatest`,
  `least`.
- Date/time: `current_date`, `current_time`, `current_timestamp`, `now`,
  `date_trunc`, `date_part`, `date_diff`, `date_add`, `date_sub`, `extract`,
  `strftime`, `make_date`, `make_time`, `make_timestamp`, `epoch`,
  `to_timestamp`, `year`, `month`, `day`, `hour`, `minute`, `second`.
- Null/conditional: `coalesce`, `nullif`, `ifnull`, `if`, `case`.
- Aggregates: `count`, `sum`, `avg`, `min`, `max`, `count_distinct`,
  `approx_count_distinct`, `stddev`, `stddev_pop`, `stddev_samp`, `variance`,
  `var_pop`, `var_samp`, `median`, `quantile_cont`, `quantile_disc`, `arg_min`,
  `arg_max`, `string_agg`, `list`, `array_agg`, `first`, `last`, `bool_and`,
  `bool_or`, `bit_and`, `bit_or`, `bit_xor`.
- Window functions (used with `OVER`): `row_number`, `rank`, `dense_rank`,
  `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first_value`,
  `last_value`, `nth_value`.
- Struct/list read-only access: column de-referencing, `unnest` over list/struct
  columns, and list/struct accessor functions that do not open files or URLs.

Explicitly denied (the I/O and control surface, regardless of the allow-list):

- File/table functions: `read_csv`, `read_csv_auto`, `read_json`,
  `read_json_auto`, `read_ndjson`, `read_parquet`, `parquet_scan`, `read_text`,
  `read_blob`, `glob`, `sniff_csv`, `sniff_parquet`.
- Network/URL: `http_get`, `http_post`, any `httpfs`/`s3` URL-taking function,
  and any function accepting a `url`, `path`, or `filename` argument.
- Session/system/config: `current_setting`, `set_config`, `current_schema`,
  secret-related functions, and `duckdb_*` introspection that exposes the
  filesystem or settings.
- Extension/module management (already statement-denied, re-listed here for
  function completeness): `INSTALL`, `LOAD`.

### 7.3 Relation scope (what a query may reference)

- The only relations a query may reference are catalog views generated
  server-side for `READY` dataset versions (one view per version, named
  `catalog.<dataset>_v<n>` or an equivalent internal name).
- The parser resolves every table reference and rejects any reference to a raw
  `s3://` URL, `http(s)://` URL, `file://` path, local filesystem path, or any
  table not present in the generated catalog for that query.

### 7.4 Engine-level enforcement (defense in depth)

The boundary is enforced by layers, never by a keyword blacklist alone:

1. **Parser**: SQL is parsed with a real parser to an AST; statement count,
   statement type, table references, and function calls are validated as above.
2. **Engine mode**: DuckDB is opened with `access_mode=read_only`,
   `enable_external_access=false`, unsigned extensions disabled, and
   `INSTALL`/`LOAD` unavailable.
3. **Generated-view execution**: for each validated query, the worker creates
   the referenced catalog views in a per-query, read-only database connection
   and executes the user statement against only those views. Arbitrary object
   access is therefore impossible even if one layer is bypassed.
4. **Secret scoping**: object-store credentials are injected server-side with
   least privilege and are never present in the SQL text or the client.

### 7.5 Testability

The boundary is tested by adversarial cases that must fail (recorded in
`plan/06-security-and-observability.md`): DDL/DML, `COPY`, `ATTACH`,
`INSTALL`/`LOAD`, `EXPORT`, multiple statements, local paths
(`read_csv('/etc/…')`), arbitrary S3 URLs, `http_get`, extension loading, reads
of an unregistered prefix, and oversized result requests. Each must be denied
and must not read data outside registered prefixes.

## 8. Object access and credentials

- Credentials live only in the API/worker configuration (environment or secret
  store), never in catalog rows, logs, metrics, traces, browser bundles, or API
  responses.
- Two least-privilege scopes:
  - **ingest**: read `uploads/` and registered source prefixes; write
    `staging/` and `lake/`.
  - **query**: read `lake/` (registered prefixes only); read/write `exports/`
    (spooling results and exports).
- The dashboard/API never returns bucket credentials, secret keys, or raw
  object keys. Preview/schema results are bounded (§9).

## 9. Result paging, export, and retention

- Query result payloads are never stored as rows in PostgreSQL.
- On completion, results are spooled to `exports/<namespace>/<query-id>/`
  (Parquet) and summarized in the `query_job` row (row count, byte count,
  result reference). Small and large results use the same path for consistency.
- Paging: `GET …/queries/{id}/results?offset=&limit=` returns a bounded page
  (default limit 1,000; hard max 10,000). Offset paging reads from the spool.
- Row bound: a query returns at most `MAX_RESULT_ROWS` (default 10,000, hard
  max 100,000). Rows beyond the bound are dropped and `truncated:true` is
  recorded; the engine stops early once the bound is reached.
- CSV export: `GET …/queries/{id}/export` streams a bounded CSV (default max
  50,000 rows, capped by the result bound) from the spool, written to
  `exports/<namespace>/<query-id>/export.csv`.
- TTL: `exports/` objects expire after `EXPORT_TTL` (default 24h) via an
  object-store lifecycle rule plus a background sweeper; a query whose spool
  has expired returns a clear `result_expired` error rather than stale rows.

## 10. Data retention policy

| Data | Retention |
|---|---|
| `lake/` Parquet objects | Until the dataset/version is archived; no automatic deletion in v1 |
| Original uploads | Deleted after `READY` unless `retain_original=true` |
| `staging/` objects | Deleted immediately on success or failure |
| `exports/` (results/exports) | TTL default 24h, swept |
| query_job history rows | Bounded retention (default 30 days, configurable); SQL text retention configurable (hash always kept) |
| Failed version error summaries | Retained with the version until archive |

## 11. API error contract

All non-2xx responses use one JSON envelope:

```json
{
  "error": {
    "code": "invalid_argument",
    "message": "human-readable, safe, bounded",
    "details": { }
  }
}
```

Codes (enum) and status mapping:

| code | HTTP |
|---|---|
| invalid_argument | 400 |
| not_found | 404 |
| already_exists | 409 |
| conflict | 409 |
| invalid_state | 409 |
| query_denied | 400 |
| query_cancelled | 409 |
| query_timeout | 504 |
| result_expired | 410 |
| resource_exhausted | 429 |
| rate_limited | 429 |
| unauthorized | 401 |
| forbidden | 403 |
| unavailable | 503 |
| internal | 500 |

Safety rules for errors: never echo SQL text, object keys, credentials, or
filesystem paths; messages are length-bounded; field-level `details` is typed
and sanitized.

## 12. Timeouts, limits, and concurrency

Defaults are configuration values (overridable); tests assert the bounds, and
only pin a default where a test explicitly chooses to.

| Control | Default | Bound |
|---|---|---|
| Query execution timeout | 60s | max 300s |
| Worker concurrency (queries) | 2 | configurable, ≥1 |
| DuckDB threads | min(4, cpus) | configurable |
| DuckDB memory | 4 GiB | configurable |
| SQL text length | 64 KiB | hard cap |
| Statements per query | 1 | exactly 1 |
| Result rows | 10,000 | hard max 100,000 |
| Page size | 1,000 | hard max 10,000 |
| Export rows | 50,000 | capped by result bound |
| Queue depth | 100 | bounded, rejects when full |

## 13. Query job lifecycle and crash model

Query status: `QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED | TIMED_OUT`.

| From | Event | To | Notes |
|---|---|---|---|
| (new) | submit validated | QUEUED | durable row, statement hash recorded |
| QUEUED | worker acquires lease | RUNNING | lease_owner/lease_expires_at set |
| RUNNING | results committed to object store + row marked | SUCCEEDED | success recorded only after results + state are committed |
| RUNNING | cancel accepted | CANCELLED | observable; cancellation is persisted |
| RUNNING | timeout exceeded | TIMED_OUT | distinct from FAILED |
| RUNNING | engine/object-store error | FAILED | failure_reason set (engine_error, object_store, resource_exceeded, …) |
| RUNNING | lease expires (worker died) | FAILED (retryable) or requeue | never reported SUCCEEDED without committed results |

- Queued jobs survive worker restart (state is in PostgreSQL; the queue is
  reconstructed by scanning for `QUEUED` rows).
- Interrupted running jobs are detected by lease expiry and moved to an
  explicit retryable/FAILED state — never silently SUCCEEDED.
- Cancellation and timeout are observable in status and persisted.

## 14. Single-node limit

- DuckDB is single-node for this release. There is no sharding, no distributed
  execution, and no claim of horizontal scale-out.
- The query engine sits behind `internal/query.Engine` (defined in phase 4) so
  a measured need can justify Trino or another distributed engine later; no
  such work is scheduled in this plan.
- Documentation and the README must state this limit explicitly and must not
  describe the product as a distributed OLAP database.
