---
phase: 4
slug: query-engine-and-jobs
title: Execute bounded SQL over registered Parquet datasets
touches: internal/query/, internal/jobs/, internal/api/, migrations/, scripts/
verify: make verify
---

## Goal

SQL queries run asynchronously against registered datasets with durable job state,
bounded resources, result paging, cancellation, and safe export.

## Changes

- Define `internal/query.Engine`; implement DuckDB behind it, configuring S3/MinIO
  access server-side and creating stable views for registered dataset versions.
- Add durable query-job states, queueing, worker limits, heartbeat/lease or another
  explicit crash model, timeout, cancellation, and safe error classification.
- Permit one read-only statement over registered datasets. Reject writes, DDL,
  attach/install/load, multiple statements, arbitrary paths, and object URLs not
  belonging to the catalog. Use parser/engine controls, not keyword filtering
  alone.
- Add submit/status/history/results/cancel/export APIs. Bound rows/bytes/page size;
  place large temporary exports in a separate output prefix with TTL.
- Capture duration, row count, query hash, bounded plan summary, and bytes scanned
  when the engine exposes them. Never log query text or storage credentials.

## Acceptance

- A query can be submitted, polled, cancelled, paged, and exported.
- Queued/running jobs have truthful restart behavior; terminal success is recorded
  only after results are available.
- Unsafe SQL, arbitrary local files, and arbitrary S3 paths are denied by tests.
- Resource limits and maximum result sizes are enforced server-side.
- `make verify` passes; integration tests execute real Parquet queries via MinIO.

## Out of scope

- Dashboard SQL editor; phase 5 integrates this API into the user experience.
