# Lakehouse Query Studio — Project Plan

## Product goal

Make data in Amazon S3 or MinIO usable for analytical SQL without first loading
it into a conventional warehouse. The product is a web-based **lakehouse query
workbench**: object storage holds Parquet files; a metadata catalog describes
datasets; a bounded DuckDB query service reads registered datasets; and the
dashboard provides discovery, SQL execution, results, charts, exports, and query
history.

The first release is intentionally **single-node**. It demonstrates object-store
querying, columnar data layout, query planning, and a usable analytics UI. It is
not a distributed OLAP database and must not claim to be one. Keep the query
engine behind an interface so a measured need can justify Trino or another
distributed engine later.

## Target user experience

1. Start the stack locally with Docker Compose.
2. Upload CSV/JSON files or register an existing Parquet prefix in MinIO/S3.
3. Review inferred schema and partition settings; create a dataset version.
4. Open the dashboard's dataset browser and inspect schema/sample rows.
5. Write SQL in the dashboard, autocomplete dataset/column names, run/cancel the
   query, inspect a paginated result grid, and visualize selected columns.
6. Reopen query history, inspect duration/bytes scanned/plan where available, and
   export bounded results to CSV.

## Architecture contract

### Components

- **Go API/control plane:** dataset registration, ingestion coordination, query
  submission/status/cancellation, result paging/export, health, and configuration.
- **Go query worker:** bounded asynchronous job execution. Use DuckDB through a
  maintained Go binding behind `internal/query.Engine`; isolate engine-specific
  setup and S3 configuration behind this interface.
- **DuckDB:** local/single-node analytical execution over Parquet via its S3/HTTPFS
  support. Never let browser code connect to DuckDB or receive object-store keys.
- **PostgreSQL:** durable catalog metadata, dataset versions, ingestion jobs,
  query jobs, and bounded query history.
- **MinIO locally / S3-compatible remotely:** immutable source and converted
  Parquet objects. Use distinct prefixes for datasets and temporary query exports.
- **React + TypeScript dashboard:** application UI with dataset catalog, SQL editor,
  result grid, charting, history, and operational summary.
- **Docker Compose:** local Postgres, MinIO, API, worker, and dashboard. Avoid
  requiring hosted services for the demo.

### Storage layout and metadata

- Use a documented object layout such as
  `lake/<namespace>/<dataset>/version=<n>/.../*.parquet`.
- Prefer Parquet for analytical reads. Preserve the original upload only if the
  design specifies retention; do not silently create unbounded duplicate copies.
- Support explicit partition columns and Hive-style directory partitions. Do not
  partition by high-cardinality values by default.
- Catalog at least: dataset ID/name, object-store prefix, format, schema, partition
  columns, version, status, created/updated timestamps, and ingest error summary.
- Query job metadata includes query ID, dataset references, submitted/started/
  finished timestamps, status, bounded error, rows returned, bytes scanned when
  available, and duration. Never persist credentials in catalog rows.
- Keep large result payloads out of PostgreSQL. Return bounded pages; use a
  temporary object prefix with TTL for large CSV exports.

### API surface (versioned)

Exact route names may follow repository conventions, but implement equivalent
operations:

- `POST /api/v1/datasets` — register a Parquet prefix or create an upload target.
- `POST /api/v1/datasets/{id}/ingest` — submit ingestion/conversion job.
- `GET /api/v1/datasets`, `GET /api/v1/datasets/{id}`, and bounded preview/schema.
- `POST /api/v1/queries` — submit a SQL query and return a durable query ID.
- `GET /api/v1/queries/{id}` — status and execution metadata.
- `GET /api/v1/queries/{id}/results` — bounded, paginated rows.
- `POST /api/v1/queries/{id}/cancel` and `GET /api/v1/queries` for history.
- `GET /healthz`, `/readyz`, and `/metrics`.

## Dashboard requirements

The custom web dashboard—not just Grafana—must include:

- **Overview:** object-store/API/query-worker health, dataset count, active/failed
  queries, and recent activity. Avoid fabricated usage metrics.
- **Datasets:** searchable list, source prefix, format, schema, partition metadata,
  ingest status, preview, and version/error details.
- **SQL workbench:** editor with syntax highlighting, schema tree, autocomplete if
  practical, run/cancel, elapsed-time/status indicator, and readable errors.
- **Results:** paginated table, row/column counts, basic chart builder (select x/y
  and chart type), CSV export, and query-plan/execution detail when available.
- **History:** recent jobs with status, duration, dataset references, and rerun
  action. Clearly distinguish queued/running/cancelled/failed/completed.
- Responsive enough for laptop widths, with loading, empty, error, and long-query
  states. No secret or bucket credential may appear in client-visible data.

## Query safety and reliability

- Only allow a documented single-statement, read-only SQL subset for the first
  release. Reject DDL/DML, `COPY`, `ATTACH`, `INSTALL`/`LOAD`, arbitrary local
  filesystem paths, and arbitrary S3 URLs.
- Do not use a regex/keyword blacklist as the sole SQL security boundary. Use
  DuckDB read-only/extension controls and a parser or a generated-view execution
  model; add adversarial tests proving access cannot escape registered datasets.
- Object-store credentials are server-side, least-privilege, and never returned
  to the browser. Separate ingestion write access from query read access where
  practical.
- Bound query duration, worker concurrency, threads, memory, page size, and
  result size. Make cancellation observable and persisted.
- On worker restart, queued jobs remain runnable; interrupted running jobs move
  to an explicit retryable/failed state. Never report success before results and
  job state are committed.
- Keep SQL text and error handling deliberate: no SQL or secret values in logs;
  avoid returning raw DuckDB/MinIO credentials or filesystem paths in errors.

## Numbered implementation phases

Phases are numbered 0–8 for Stackflow. Each phase is independently verifiable and
must leave `make verify` green. See `plan/` for builder-ready phase contracts.

| Phase | Focus | Demonstrable result |
|---:|---|---|
| 0 | Design contract and API/data/security decisions | No ambiguous storage/query contract before code |
| 1 | Go/React monorepo foundation and local Compose stack | Clean local startup, CI, health/readiness |
| 2 | PostgreSQL catalog and dataset registration | Browse and preview registered Parquet datasets |
| 3 | Upload, schema inference, and Parquet ingestion | CSV/JSON becomes versioned, queryable Parquet |
| 4 | DuckDB query worker and durable query jobs | Submit, poll, cancel, page, and export SQL results |
| 5 | User-facing dashboard and embedded SQL workbench | End-to-end workflow from browser |
| 6 | Query safety, limits, observability, and resilience | Adversarial/security and restart behavior tested |
| 7 | Benchmark, fault tests, and operational dashboard polish | Reproducible measurements and service visibility |
| 8 | Demo, runbook, README, and release validation | One-command demonstration and honest evaluation |

## Verification and benchmark plan

### Required automated verification

- `make verify`: Go formatting, vet, lint, unit/race tests, Go build, frontend
  typecheck/lint/tests/build, migration checks, and any deterministic integration
  tests that do not require external services.
- Integration tests are explicitly tagged and run only after Compose dependencies
  are healthy. Never make the default unit-test target depend on a running MinIO or
  PostgreSQL service.
- Test registration of missing/corrupt objects, schema inference, ingestion
  retries, SQL validation, result paging, query timeout/cancellation, worker crash,
  MinIO outage, and dashboard API error states.
- Test unsafe queries: DDL/DML, `COPY`, `ATTACH`, extension commands, local paths,
  arbitrary URLs, multiple statements, and oversized result requests.

### Performance measurements

- Use a reproducible TPC-H dataset (scale factor 1 as baseline; scale factor 10
  optional if local hardware allows) plus a documented partitioned dataset.
- Run selected queries at concurrency 1, 5, and 10; report p50/p95/p99 latency,
  throughput, rows returned, bytes scanned where available, and warm/cold-cache
  conditions.
- Measure ingestion throughput and conversion time separately from query time.
- Record CPU, RAM, Go/DuckDB versions, MinIO/Postgres settings, and hardware.
- Do not promise a speedup in advance. The benchmark report is the source of truth;
  targets are goals, not achieved results.

## Non-goals for the first release

- Distributed/multi-node query execution or a claim of replacing Snowflake/Redshift.
- Full multi-tenant identity, billing, governance, or a semantic BI layer.
- Arbitrary user-installed DuckDB extensions or unrestricted SQL access to files.
- Hosted/cloud deployment, Kubernetes, multi-region replication, or Iceberg unless
  a measured requirement justifies the extra catalog complexity.

## Definition of done

The dashboard can register/ingest a dataset, browse its schema, execute safe SQL,
show/paginate results, chart and export them; jobs survive API/worker restart with
truthful states; Compose setup is repeatable; automated and integration tests pass;
benchmarks and limitations are documented; and the README reports only measured
numbers. The initial engine's single-node limit is explicit.
