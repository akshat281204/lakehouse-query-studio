# Lakehouse Query Studio — Architecture

> Phase 0 deliverable. Describes component responsibilities, data flow,
> failure boundaries, security boundaries, and deployment topology for the
> first (single-node) release. The normative data/query/security contracts are
> in `docs/design.md`; this document describes how the pieces fit together.

## 1. Overview

The system is a single-node analytical query layer over S3-compatible object
storage. A Go API/control plane and a Go query worker coordinate durable
metadata (PostgreSQL) and immutable Parquet data (MinIO/S3); DuckDB executes
bounded, read-only SQL against registered dataset versions; and a React +
TypeScript dashboard provides discovery, SQL execution, results, charts, and
history. The first release runs entirely under Docker Compose.

## 2. Components and responsibilities

| Component | Responsibility |
|---|---|
| Go API / control plane | Dataset registration, ingestion coordination, query submission/status/cancellation, result paging/export, health/readiness/metrics, configuration validation. Stateless; all durable state lives in PostgreSQL and the object store. Is the sole reader of result spools: it pages and exports results by reading `exports/<namespace>/<query-id>/…` under a per-query, prefix-scoped read credential (§9 of `docs/design.md`). |
| Go query worker | Bounded asynchronous job execution. Runs DuckDB through a maintained Go binding behind `internal/query.Engine`; isolates engine-specific setup and S3 configuration behind that interface. Claims jobs under a per-claim `fencing_token`/`attempt` and writes result spools to an attempt-scoped export prefix so a stale worker cannot publish results or overwrite terminal state (§6.1, §13 of `docs/design.md`). |
| DuckDB | Local, single-node analytical execution over Parquet via its S3/HTTPFS support. The worker parses/validates user SQL, re-binds the allowed catalog references to their registered prefixes, and executes the rewritten statement on a read-only connection whose S3 credential is scoped to exactly those prefixes (see `docs/design.md` §7.4). Never reachable from browser code. |
| PostgreSQL | Durable catalog metadata, dataset versions, ingestion jobs, query jobs, and bounded query history. Never stores result payloads or credentials. |
| MinIO (local) / S3-compatible (remote) | Immutable source and converted Parquet objects. Uses distinct prefixes for datasets, uploads, staging, and temporary query exports. |
| React + TypeScript dashboard | Application UI: dataset catalog, SQL editor, result grid, charting, history, and operational summary. No object-store credentials or keys ever reach the browser. |
| Docker Compose | Local Postgres, MinIO, API, worker, and dashboard; named volumes and health/readiness checks. |

## 3. Data flow

### 3.1 Registration and ingestion

1. The dashboard calls `POST /api/v1/datasets` to register an existing Parquet
   prefix or create an upload target.
2. The API writes a durable `dataset` and `dataset_version` (PENDING) plus an
   `ingestion_job` (QUEUED) in PostgreSQL.
3. The worker claims the job lease under a fresh `fencing_token`/`attempt`
   (conditional update keyed on `status='QUEUED'`) and either inspects the
   existing prefix (`register`) or converts uploaded CSV/JSON into Parquet under
   `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/` (`convert`).
4. On success, the worker places complete Parquet parts into the attempt-scoped
   prefix `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/`, captures the
   schema, and in one fenced transaction (conditional on the current
   `fencing_token`) sets the version `READY` with `object_prefix` pointing at
   that attempt prefix. A fenced worker's commit matches zero rows and it aborts
   without making the attempt visible; the stale-attempt sweeper removes only
   conclusively abandoned (terminal) attempt prefixes whose attempt-scoped write
   credential has expired plus a grace margin, serialized against the publish
   commit (`docs/design.md` §4 rules 6–8). On failure the version is marked
   `FAILED` with an error summary; staging and attempt-prefix objects are
   reclaimed by the sweeper only after write-quiescence (the attempt's write
   credential has expired or been revoked), never by a still-credentialed
   delayed worker's race (`docs/design.md` §6.1).

### 3.2 Query execution

1. The dashboard submits SQL via `POST /api/v1/queries`; the API validates the
   statement against the safety boundary (§7 of `docs/design.md`) and writes a
   durable `query_job` (QUEUED) that persists the full SQL statement plus its
   hash — the worker executes the persisted statement, so a job that is requeued
   after a worker restart still has the exact text to run.
2. The worker claims the job lease under a fresh `fencing_token`/`attempt`
   (conditional update keyed on `status='QUEUED'`), reads the persisted
   `statement` from the job row, then runs the query on a
   single connection: it parses/validates the statement, re-binds each
   `catalog.<dataset>_v<n>` reference to that version's registered `lake/`
   prefix (taken from the catalog, never the SQL text), obtains a per-query
   prefix-scoped S3 credential and registers it as a connection-scoped secret,
   then executes the rewritten statement on a read-only connection with the local
   filesystem disabled (see `docs/design.md` §7.4).
3. The worker spools results to
   `exports/<namespace>/<query-id>/attempt=<n>/result.parquet` on a separate
   write-scoped connection, records `rows_returned`, `truncated`, and byte
   counts (and bytes scanned / plan
   summary when the engine exposes them), and marks the job `SUCCEEDED` only
   after results and state are committed — the `SUCCEEDED` transition is a
   conditional update on the current `fencing_token`, so a fenced worker's commit
   matches zero rows and it aborts without publishing.
4. The dashboard polls `GET /api/v1/queries/{id}`. The API pages results via
   `GET /api/v1/queries/{id}/results` and exports bounded CSV via
   `GET /api/v1/queries/{id}/export`, reading the spool itself under a
   per-query, prefix-scoped read credential limited to that query's
   `exports/<namespace>/<query-id>/` prefix (`docs/design.md` §8, §9).

## 4. Failure boundaries

- The API is stateless; restarting it never loses jobs or catalog state.
- PostgreSQL is the source of truth for metadata and job state; the object
  store is the source of truth for data and result payloads.
- The worker persists state before claiming work (lease) and only reports
  `SUCCEEDED` after both results and job state are committed. Every job claim
  carries a fresh `fencing_token`/`attempt`; all terminal transitions and final
  commits are conditional on the current token, so a delayed/partitioned worker
  that outlives its lease is fenced and cannot publish results or overwrite
  terminal state. A worker crash leaves running jobs detectable by lease expiry;
  interrupted jobs move to an explicit retryable/FAILED state (ingestion) or are
  requeued (query), never silently succeeded (`docs/design.md` §6.1, §13).
- Result spools are attempt-isolated (`exports/…/attempt=<n>/`); a stale
  attempt's objects are never referenced and are reclaimed by TTL.
- Object-store or engine failures surface as `unavailable` (503) or `internal`
  (500) errors under the `docs/design.md` §11 error contract, and ingestion
  failures are retry-safe (fresh staging, no duplicate visible version).
- The query engine is isolated behind `internal/query.Engine`; DuckDB is
  single-node, and a future distributed engine would replace only that
  boundary.

## 5. Security boundaries

- **Caller trust**: single-operator, trusted-local-only deployment. The API and
  dashboard bind to `127.0.0.1`/the private Compose network by default, but
  loopback binding is **not** a caller boundary: a strict Host/Origin/fetch-site
  allowlist plus CSRF protection for state-changing requests is mandatory in the
  default deployment, and an optional single deployment token (`API_TOKEN`)
  additionally gates all requests. No per-user authentication or authorization
  exists in v1 (see `docs/design.md` §15).
- **SQL boundary**: one read-only `SELECT` (optionally with `WITH` CTE) over the
  registered datasets; enforced by parser/AST validation (closed function
  allowlist, deny-by-default, table-reference whitelist), server-side re-binding
  of allowed references to their registered prefixes, a per-query prefix-scoped
  S3 credential, and a read-only execution connection with the local filesystem
  disabled and configuration locked (see `docs/design.md` §7).
- **Object access**: ingest uses a least-privilege worker credential to **read**
  uploads/source, and an **attempt-scoped, short-lived** write credential for
  `staging/`/`lake/` whose expiry is recorded as `write_credential_expires_at` so
  the stale-attempt sweeper can guarantee no writes after delete (`docs/design.md`
  §4 rule 8, §8); each query uses a short-lived credential
  scoped to exactly the committed attempt prefixes
  `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/` it references (read),
  registered as a prefix-scoped DuckDB secret with ambient
  credential fallback disabled; the worker holds a write-scoped credential for
  only that query's `exports/<namespace>/<query-id>/attempt=<n>/` prefix; and the
  API holds a short-lived, prefix-scoped read credential for only the one
  query's `exports/<namespace>/<query-id>/` prefix to page and export results.
  The browser never receives credentials or object keys.
- **Secrets**: credentials are server-side only and never appear in logs,
  metrics, traces, catalog rows, or API responses. SQL text and user-controlled
  values are excluded from logs and metric labels.
- **Limits**: query duration, worker concurrency, DuckDB threads/memory, page
  size, and result size are bounded server-side (see `docs/design.md` §12).

## 6. Deployment topology

```
                         ┌──────────────────────────────┐
                         │         Docker Compose       │
                         │                              │
  ┌──────────┐   HTTP    │  ┌───────────┐               │
  │ Browser  │───────────┼─▶│ Dashboard │               │
  │ (React)  │           │  │  (web)    │               │
  └──────────┘           │  └─────┬─────┘               │
                         │        │ REST (versioned API)│
                         │  ┌─────▼─────┐   ┌─────────┐ │
                         │  │ API/control│──▶│PostgreSQL│ │
                         │  │  plane    │   │(catalog) │ │
                         │  └─────┬─────┘   └─────────┘ │
                         │        │ job rows            │
                         │  ┌─────▼─────┐               │
                         │  │ Query     │──▶ DuckDB    │
                         │  │ worker    │   (read-only)│
                         │  └─────┬─────┘               │
                         │        │                     │
                         │  ┌─────▼─────┐               │
                         │  │  MinIO    │  (S3-compatible)│
                         │  │  (object) │                │
                         │  └───────────┘               │
                         └──────────────────────────────┘
```

- Single host, single instance of each service. MinIO may be swapped for a
  remote S3-compatible bucket without code changes (adapter boundary).
- The API reads result spools directly from MinIO for paging/export; the worker
  writes them (attempt-scoped) and both use prefix-scoped credentials (§3.2,
  §5, and `docs/design.md` §8, §9).
- By default the API and dashboard bind to `127.0.0.1`/the private Compose
  network and are not published to public interfaces; this network boundary is
  reinforced by a mandatory Host/Origin/fetch-site allowlist and CSRF protection
  for state-changing requests, since loopback binding alone does not establish
  the caller's identity (see `docs/design.md` §15).
- Named Compose volumes persist PostgreSQL data and MinIO objects across
  restarts.
- No horizontal scaling, no multi-region replication, no Kubernetes, and no
  hosted dependency is required for the demo.

## 7. Single-node constraint and evolution

- DuckDB is single-node for this release; there is no distributed execution and
  no claim of horizontal scale-out. This limit is documented in the README and
  must not be understated.
- The query engine boundary (`internal/query.Engine`) is the only place a
  measured need could later justify Trino or another distributed engine; no such
  work is scheduled in this plan.
