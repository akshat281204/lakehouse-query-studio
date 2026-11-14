# Upload, conversion, and Parquet ingestion (phase 3)

Users upload a CSV, JSON (array of objects), or JSONL file. The API stores the
upload, queues a durable `convert` job, and the worker converts it to
partitioned Parquet and publishes a new dataset version. Normative contracts
(key layout, partition encoding, job state machine) remain in `docs/design.md`.

## Flow

1. `POST /api/v1/ingestions/inspect` (optional) parses the file without storing
   it and returns the inferred schema, row count, and 5 sample rows.
2. `POST /api/v1/ingestions?name=&filename=&format=&partitionColumns=a,b&retainOriginal=`
   with the raw file as the request body validates the file, writes it to
   `uploads/<ns>/<dataset>/<job-id>/<sanitized-name>`, and creates the dataset
   (or the next version of an existing one), a `PENDING` version, and a
   `QUEUED` job in one transaction. It answers `202` with the job. If the job
   cannot be created the orphaned upload is deleted.
3. The worker claims the job (`QUEUED → RUNNING`, version `BUILDING`), reads the
   upload, infers types, converts, and writes Parquet:
   `staging/…/attempt=<n>/` first, then
   `lake/<ns>/<dataset>/version=<v>/attempt=<n>/[col=value/…]/part-<uuid>.parquet`.
4. It verifies the object count under the attempt prefix, then publishes in one
   fenced transaction: version `READY` with its `object_prefix`, row count,
   partition columns, and schema columns; job `SUCCEEDED`. Only then does the
   version become visible to schema/preview.
5. `GET /api/v1/ingestions/{id}` is polled by the dashboard;
   `POST /api/v1/ingestions/{id}/retry` retries a failed, retryable job;
   `GET /api/v1/ingestions` lists recent jobs.

## Input rules and limits

| Rule | Value |
|---|---|
| Upload size | 64 MiB (HTTP `413 payload_too_large`) |
| Rows / columns | 1,000,000 / 256 |
| Encoding | UTF-8 (BOM stripped); NUL characters rejected |
| Column names | 1–128 bytes, no control characters, no leading/trailing space, unique case-insensitively |
| Types | `boolean`, `int64`, `double`, `string`. A column is the narrowest type fitting every value; int+float widens to `double`; any other mix becomes `string` |
| Nulls | Empty CSV cells and JSON `null`/missing keys; the column is nullable |
| JSON | Flat objects only; nested objects/arrays are rejected |
| Part size | at most 100,000 rows per Parquet file |

Input errors return `422 invalid_input` from the API and fail a job with a
non-retryable `invalid_input` reason. Messages name the line or column rule but
never echo cell values.

## Partitioning

Partitioning is **off by default**. `partitionColumns` selects up to 4 integer,
boolean, or string columns (not `double`); at least one column must remain.
Directories use the percent-encoding of design §4.1 (`region=us%2Feast/`, null
as `%00`). At most 256 distinct directories are allowed so a high-cardinality
column fails fast instead of producing an explosion of tiny files. Within a
directory, files are split at 100,000 rows. Partition columns are also kept in
the file data, so every part carries the complete schema.

## Durability and recovery

- Jobs are rows in `ingestion_jobs`; nothing is held only in memory. A job
  survives API and worker restarts.
- A claim sets `attempt`, a fresh `fencing_token`, and a lease (15 minutes).
  A `RUNNING` job whose lease expired is reclaimed as the next attempt, writing
  to its own `attempt=<n>` prefix. After 3 attempts the job fails as
  `worker_interrupted` and is retryable.
- `Complete`/`Fail` only succeed for the current token (`ErrFenced` otherwise),
  so a stale worker can never publish or overwrite state.
- Retry creates a **new** job and a **new** version number linked by
  `retry_of`; the failed version stays `FAILED`. A job can be retried once while
  a retry is live or succeeded, so retries never create duplicate visible
  versions.
- Claiming uses `FOR UPDATE SKIP LOCKED`, so multiple workers never run the same
  job.
- Migrations run under a PostgreSQL advisory lock because the API and worker
  both migrate at startup.

## Cleanup

On failure the worker deletes the staging and attempt-prefix objects it wrote
before recording the failure; on success it deletes its staging copy and, unless
`retainOriginal` was set, the original upload. A failed job keeps its upload so
a retryable failure can be retried.

## Deviations from the phase-0 contract (documented limitations)

- **No per-attempt scoped credentials.** The worker uses the deployment object
  store key (server-side only). Design §4 rules 6/8 defer cleanup until a
  short-lived credential expires; with one credential and a fencing token the
  worker instead removes only its own attempt-scoped keys after it has stopped
  writing, which cannot race a live write of the same attempt. A crashed
  worker's objects are not reclaimed until the stale-attempt sweeper (phase 6).
- **No lease renewal.** Conversion is bounded by the 64 MiB limit and runs under
  a timeout shorter than the lease, so heartbeats are not needed yet.
- **Registered Parquet is still referenced in place** (`object_prefix` equals
  the source prefix); copying into `lake/…` is not done in this phase.
- Uploads are buffered in memory (bounded by the size limit); there is no
  multipart or streaming upload.

## Running the integration tests

`make verify` stays hermetic. Tests that need a live PostgreSQL are tagged
`integration`:

```bash
TEST_POSTGRES_URL=postgres://user:pass@127.0.0.1:5432/db?sslmode=disable make test-integration
```

They cover the catalog, job claiming and fencing, crash recovery, retries, and
concurrent claim/create against real SQL. The object store is exercised through
an in-memory fake plus HTTP-level tests of the S3 client; a MinIO-backed run is
not part of this phase's automated tests.
