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
| Go API / control plane | Dataset registration, ingestion coordination, query submission/status/cancellation, result paging/export, health/readiness/metrics, configuration validation. Stateless; all durable state lives in PostgreSQL and the object store. |
| Go query worker | Bounded asynchronous job execution. Runs DuckDB through a maintained Go binding behind `internal/query.Engine`; isolates engine-specific setup and S3 configuration behind that interface. |
| DuckDB | Local, single-node analytical execution over Parquet via its S3/HTTPFS support. The worker registers dataset views in a trusted bootstrap phase, then runs user SQL on a read-only connection with external access disabled and only registered prefixes whitelisted (see `docs/design.md` §7.4). Never reachable from browser code. |
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
3. The worker acquires the job lease and either inspects the existing prefix
   (`register`) or converts uploaded CSV/JSON into Parquet under `staging/`
   (`convert`).
4. On success, the worker places complete Parquet parts into
   `lake/<namespace>/<dataset>/version=<n>/`, captures the schema, and marks the
   version `READY` in the same logical commit. On failure, staging objects are
   removed and the version is marked `FAILED` with an error summary.

### 3.2 Query execution

1. The dashboard submits SQL via `POST /api/v1/queries`; the API validates the
   statement against the safety boundary (§7 of `docs/design.md`) and writes a
   durable `query_job` (QUEUED) with a statement hash.
2. The worker acquires the job lease and opens DuckDB in two phases on one
   connection. First, a **trusted bootstrap** phase registers a generated view
   per referenced `READY` dataset version (`catalog.<dataset>_v<n>`, reading
   only that version's registered `lake/` prefix via the S3/HTTPFS filesystem
   with server-side credentials) and whitelists exactly those prefixes. Then
   the user's single validated statement executes against only those views with
   external access disabled (see `docs/design.md` §7.4).
3. The worker spools results to `exports/<namespace>/<query-id>/result.parquet`,
   records row/byte counts (and bytes scanned / plan summary when the engine
   exposes them), and marks the job `SUCCEEDED` only after results and state are
   committed.
4. The dashboard polls `GET /api/v1/queries/{id}`, pages results via
   `GET /api/v1/queries/{id}/results`, and exports bounded CSV via
   `GET /api/v1/queries/{id}/export`.

## 4. Failure boundaries

- The API is stateless; restarting it never loses jobs or catalog state.
- PostgreSQL is the source of truth for metadata and job state; the object
  store is the source of truth for data and result payloads.
- The worker persists state before claiming work (lease) and only reports
  `SUCCEEDED` after both results and job state are committed. A worker crash
  leaves running jobs detectable by lease expiry; interrupted jobs move to an
  explicit retryable/FAILED state (never silently succeeded).
- Object-store or engine failures surface as `unavailable`/`bad_gateway`-style
  errors mapped to the error contract, and ingestion failures are retry-safe
  (fresh staging, no duplicate visible version).
- The query engine is isolated behind `internal/query.Engine`; DuckDB is
  single-node, and a future distributed engine would replace only that
  boundary.

## 5. Security boundaries

- **Caller trust**: single-operator, trusted-local-only deployment. The API and
  dashboard bind to `127.0.0.1`/the private Compose network by default; an
  optional single deployment token (`API_TOKEN`) gates all requests. No per-user
  authentication or authorization exists in v1 (see `docs/design.md` §15).
- **SQL boundary**: one read-only `SELECT` (optionally with `WITH` CTE) over
  generated catalog views; enforced by parser/AST validation (closed function
  allowlist, deny-by-default), a trusted-bootstrap + locked-execution DuckDB
  mode (`access_mode=read_only`, `enable_external_access=false`,
  `allowed_directories`/`allowed_paths` scoped to registered prefixes,
  `lock_configuration`, disabled local filesystem), and server-side secret
  scoping (see `docs/design.md` §7).
- **Object access**: two least-privilege credential scopes — `ingest` (read
  uploads/source, write staging/lake) and `query` (read lake, read/write
  exports). The browser never receives credentials or object keys.
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
- By default the API and dashboard bind to `127.0.0.1`/the private Compose
  network and are not published to public interfaces; this is the primary
  exposure boundary for the trusted-local-only deployment (see
  `docs/design.md` §15).
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
