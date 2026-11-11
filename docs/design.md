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
- Multi-tenant identity, per-user authentication/authorization, RBAC, billing,
  governance, or a semantic BI layer. v1 is a single-operator, single-namespace
  deployment; the caller trust model and the enforceable exposure boundary are
  defined in §15.
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
- `ARCHIVED` is a reversible soft delete implemented at the catalog layer only.
  Archiving a dataset sets its status to `ARCHIVED`: it stops appearing in
  discovery, schema/preview, and query views, and no new ingestion or query job
  may target it. Archive **does not delete any version rows or any objects**;
  it only flips the status. Because the underlying versions and objects are
  untouched, unarchive (returning the dataset to `ACTIVE`) is always reversible
  and requires no data restoration. Hard delete and object GC are out of scope
  in v1 (§10).
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

### 7.2 Allowed functions (exhaustive, deny-by-default)

The function surface is a **closed, compile-time allowlist**. The worker
resolves every function reference — after Unicode case-folding and alias
normalization — against this fixed list and rejects any name that is not
present. The list is a constant compiled into the worker; it is **not** derived
by enumerating DuckDB's runtime function catalog, so a later DuckDB release
that adds functions does not silently widen the surface. Everything not
enumerated below — unlisted aliases, user-defined functions, table macros, and
extension-provided functions — is denied with `query_denied`.

**Operators.** Comparison (`=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`); logical
(`AND`, `OR`, `NOT`); arithmetic (`+`, `-`, `*`, `/`, integer division `//`,
`%`, `^`, unary `+`/`-`); string concatenation (`||`); `IS [NOT] NULL`;
`IS [NOT] DISTINCT FROM`; `[NOT] BETWEEN`; `[NOT] IN`; `[NOT] LIKE`;
`[NOT] ILIKE`; `[NOT] SIMILAR TO`; regex match `~`, `!~`, `~*`, `!~*`; and the
subscript `[]` operator for 1-based list/array indexing and map key lookup.
`CASE`/`WHEN`/`THEN`/`ELSE` is a language construct and is allowed.

**Casts and type predicates.** `CAST(expr AS type)`, the `::` operator,
`TRY_CAST`, and `typeof`.

**String.** `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `length` (alias `len`),
`substring` (alias `substr`), `concat`, `concat_ws`, `replace`, `reverse`,
`split` (aliases `string_split`, `str_split`), `string_to_array`, `position`
(alias `strpos`), `left`, `right`, `lpad`, `rpad`, `repeat`, `starts_with`,
`ends_with`, `contains`, `prefix`, `suffix`, `regexp_matches`,
`regexp_replace`, `regexp_extract`, `regexp_full_match`.

**Numeric / math.** `abs`, `sign`, `round`, `floor`, `ceil` (alias `ceiling`),
`sqrt`, `pow` (alias `power`), `exp`, `ln`, `log`, `log2`, `log10`, `mod`,
`greatest`, `least`.

**Date / time.** `current_date`, `current_time`, `current_timestamp` (alias
`now`), `date_trunc` (alias `datetrunc`), `date_part` (alias `datepart`),
`date_diff` (alias `datediff`), `date_add` (alias `dateadd`), `date_sub` (alias
`datesub`), `extract`, `strftime`, `make_date`, `make_time`, `make_timestamp`,
`epoch`, `to_timestamp`, `year`, `month`, `day`, `hour`, `minute`, `second`,
`dayofmonth`, `dayofweek`, `dayofyear`, `week`, `quarter`. `extract` and
`date_part` accept only the fields `year, month, day, hour, minute, second,
millisecond, microsecond, dow, isodow, doy, week, quarter, epoch, decade,
century, millennium`.

**Null / conditional.** `coalesce`, `nullif`, `ifnull`, `if`, and `CASE`
(construct).

**Aggregates** (usable with `DISTINCT`, `ORDER BY`, and `FILTER (WHERE …)`
where DuckDB permits them). `count` (including `count(*)`), `sum`, `avg` (alias
`mean`), `min`, `max`, `approx_count_distinct`, `stddev` (alias `stddev_samp`),
`stddev_pop`, `variance` (alias `var_samp`), `var_pop`, `median`, `mode`,
`quantile_cont` (alias `quantile`), `quantile_disc`, `arg_min` (alias
`min_by`), `arg_max` (alias `max_by`), `string_agg` (alias `group_concat`),
`list` (alias `array_agg`), `first`, `last`, `any_value`, `bool_and`,
`bool_or`, `bit_and`, `bit_or`, `bit_xor`.

**Window functions** (only inside an `OVER (...)` clause). `row_number`,
`rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`,
`first_value`, `last_value`, `nth_value`.

**Nested types (struct / list / map), read-only accessors only.**

- Struct: field dereference `s.field` (and chained `s.a.b`), and
  `struct_extract(s, 'field')`.
- List/array: subscript `l[1]` (1-based), `list_extract(l, idx)`, and
  `array_extract(l, idx)`; `length`/`len` for list size.
- Map: subscript `m['key']`, `map_extract(m, 'key')`, and
  `map_extract_value(m, 'key')`.
- Expansion: `unnest` (see table functions below).

No other `struct_*`, `list_*`, `array_*`, or `map_*` function is allowed;
`struct_pack` and other constructors are denied.

**Table functions.** Exactly one is allowed: `unnest`. All other table
functions are denied, including `range`, `generate_series`, and every reader
and meta function listed in the denial set below.

**Explicitly denied (also covered by deny-by-default, listed for clarity).**

- File/table readers and metadata: `read_csv`, `read_csv_auto`, `read_json`,
  `read_json_auto`, `read_ndjson`, `read_ndjson_auto`, `read_parquet`,
  `parquet_scan`, `parquet_metadata`, `parquet_schema`, `read_text`,
  `read_blob`, `glob`, `list_files`, `sniff_csv`, `sniff_parquet`, and any
  other `read_*` function.
- Network / URL: `http_get`, `http_post`, any `httpfs` function, and any
  function accepting a `url`, `path`, `filename`, or `secret` argument.
- Session / system / config / secret: `current_setting`, `set_config`,
  `current_schema`, `current_catalog`, `current_user`, `session_user`,
  `version`, `gen_random_uuid`, `uuid`, `random`, `random_normal`, and every
  `duckdb_*` introspection function.
- Extension / module management (also statement-denied): `INSTALL`, `LOAD`.

**Deny-by-default rule.** After alias normalization, any function name not
present in the lists above is rejected. This includes future DuckDB functions,
unlisted aliases, collations, and anything provided by a loaded extension
(extensions are not loaded in v1). The allowlist is enforced at validation time
and again by the engine-level controls in §7.4.

### 7.3 Relation scope (what a query may reference)

- The only relations a query may reference are catalog views generated
  server-side for `READY` dataset versions (one view per version, named
  `catalog.<dataset>_v<n>` or an equivalent internal name).
- The parser resolves every table reference and rejects any reference to a raw
  `s3://` URL, `http(s)://` URL, `file://` path, local filesystem path, or any
  table not present in the generated catalog for that query.

### 7.4 Engine-level enforcement (defense in depth)

The boundary is enforced by layers, never by a keyword blacklist alone. Reads of
registered Parquet happen through DuckDB's S3/HTTPFS filesystem; the
`enable_external_access=false` gate does **not** remove that capability — it
removes the *user's* ability to perform external I/O, and the worker re-enables
its own registered-prefix reads through an explicit path allowlist.

1. **Parser/AST validation**: SQL is parsed with a real parser to an AST;
   statement count, statement type, table references, and function calls are
   validated against §7.1–§7.3 before any engine work begins. No user statement
   ever contains a table function, a file/URL scalar function, or a path/URL
   literal, so there is no user-supplied "arbitrary path" to evaluate.

2. **Trusted bootstrap (worker code, not user SQL)**: the worker opens the
   per-query DuckDB connection with server-side S3 credentials and creates one
   generated view per referenced `READY` version,
   `CREATE VIEW catalog.<dataset>_v<n> AS SELECT * FROM parquet_scan('s3://…/lake/<namespace>/<dataset>/version=<n>/**')`.
   The prefix is taken from the catalog, never from user input. The worker then
   sets `allowed_directories`/`allowed_paths` to exactly the registered prefixes
   the query may touch (and the `exports/` spool prefix), so the generated views
   can read under an otherwise-locked engine.

3. **Locked execution (user SQL)**: before the user statement runs, the worker
   applies `access_mode=read_only`, `enable_external_access=false`,
   `lock_configuration=true` (with `allowed_configs` limited to the query-budget
   settings), `allow_unsigned_extensions=false`, extension auto-install/autoload
   disabled, and `disabled_filesystems` set to disable `LocalFileSystem` (and
   any other filesystem not required for the S3-compatible backend). Under
   `enable_external_access=false`, any user attempt to invoke a file/URL reader,
   `COPY`, `INSTALL`/`LOAD`, or a local or arbitrary S3 path is rejected at bind
   time; the only reads that still succeed are those under
   `allowed_directories`/`allowed_paths`, which the worker populated with the
   registered prefixes.

4. **Secret scoping**: object-store credentials are injected server-side with
   least privilege (query scope: read `lake/` and read/write `exports/` only)
   and are never present in the SQL text or the client. Even if every DuckDB
   layer above were bypassed, the credential's IAM policy denies reads outside
   the registered prefixes.

### 7.5 Testability

The boundary is tested by both positive and adversarial cases (recorded in
`plan/06-security-and-observability.md`):

- **Registered queries succeed**: a validated `SELECT` over
  `catalog.<dataset>_v<n>` returns rows for every `READY` version; joins and
  aggregates across two registered versions of the same or different datasets
  succeed; `unnest` over a list/struct column succeeds.
- **Out-of-prefix denial must fail**: DDL/DML, `COPY`, `ATTACH`, `INSTALL`/
  `LOAD`, `EXPORT`, `PRAGMA`, `SET`, `CALL`, multiple statements, local paths
  (`read_csv('/etc/…')`), arbitrary S3 URLs
  (`read_parquet('s3://bucket/uploads/…')`, an unregistered prefix, a different
  dataset's prefix), `http_get`, extension loading, unlisted functions (e.g.,
  `read_text`, `current_setting`, `gen_random_uuid`), and oversized result
  requests. Each must be denied with `query_denied` (or the mapped engine/limit
  error) and must read no data outside the registered prefixes.

Both behaviors are pinned in phase 6 with adversarial tests, including a
sentinel object placed outside every registered prefix that a passing test must
never observe.

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
| `lake/` Parquet objects | Retained for the life of the deployment. Archive does **not** delete them (unarchive is reversible); no automatic deletion in v1. Hard delete/GC is out of scope. |
| Original uploads | Deleted after `READY` unless `retain_original=true` |
| `staging/` objects | Deleted immediately on success or failure |
| `exports/` (results/exports) | TTL default 24h, swept |
| query_job history rows | Bounded retention (default 30 days, configurable); SQL text retention configurable (hash always kept) |
| Failed version error summaries | Retained with the version for its lifetime (archive does not delete version rows) |

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

`unauthorized` (401) and `forbidden` (403) have precise meanings under the
trust model in §15: `unauthorized` means a missing or invalid deployment token
when token authentication is enabled; `forbidden` means the authenticated
caller's operation is refused by a deployment policy gate (there is no per-user
authorization in v1).

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

## 15. Caller trust, authentication, and authorization

### 15.1 Trust model

v1 is a **single-operator, trusted-local deployment**. There is exactly one
namespace (`default`); there is no multi-tenant isolation. Every party that can
reach the API — the operator, the bundled dashboard, and any operator tooling —
is in the operator's trust domain and holds full capability to enumerate
datasets, submit queries, and consume results. This is a deliberate, documented
limit; multi-tenant identity and per-user authorization are out of scope (§1).

### 15.2 Enforceable exposure boundary

- **Network boundary (primary)**: by default the API and the dashboard bind to
  `127.0.0.1` (or the private Docker Compose network) and are **not** published
  to public or host-facing interfaces. Exposure beyond the local host is out of
  scope; a remote or shared deployment requires adding authentication before it
  is permitted (a precondition, not an option).
- **Deployment token (optional hardening)**: the API MAY be configured with a
  single static deployment token (`API_TOKEN`, supplied via environment/secret).
  When set, every request must present it as a bearer credential; a missing or
  invalid token returns `401 unauthorized`. This is one shared operator
  credential, not per-user identity.
- The token is never embedded in the static dashboard bundle. In the default
  single-host Compose deployment the dashboard is served same-origin and relies
  on the network boundary; if token authentication is enabled, the token is
  provided to the operator's browser session at runtime (for example, via a
  same-origin login/session route that keeps the credential server-side), never
  baked into client-visible data.

### 15.3 Authorization

- There is **no per-user authorization** in v1: an authenticated caller is the
  operator and may perform any operation.
- `forbidden` (403) is reserved and is produced only by deployment policy gates
  that are independent of caller identity — for example, refusing to archive a
  dataset with in-flight jobs, or refusing a mutation on an immutable version.
- Both `unauthorized` and `forbidden` are defined now (rather than added later)
  so the error contract (§11) and its meaning are stable when per-user authn/z
  is introduced in a future release.

### 15.4 Testability

- With token authentication disabled, the API answers only on the configured
  bind address; a request from outside that boundary cannot reach it.
- With token authentication enabled, requests without a token return `401`, and
  requests with a wrong token return `401`.
- A policy-gated operation (e.g., archive with in-flight jobs) returns `403`
  with a stable code, proving the authorization path exists independently of
  caller identity.
