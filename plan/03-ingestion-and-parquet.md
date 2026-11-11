---
phase: 3
slug: ingestion-and-parquet
title: Ingest supported files into versioned Parquet datasets
touches: internal/ingest/, internal/catalog/, internal/api/, web/src/, migrations/
verify: make verify
---

## Goal

Users can upload supported files and create a queryable Parquet dataset with
durable, inspectable ingestion status.

## Changes

- Support CSV and JSON/JSONL input initially; validate size, encoding, schema,
  column names, and supported types before conversion.
- Run ingestion asynchronously with durable job states, bounded concurrency,
  retry-safe writes, and explicit failure summaries.
- Convert to Parquet under a dataset-version prefix. Support user-selected
  partition columns with a safe default and documented small-file behavior.
- Update catalog schema/partitions only after the complete version is written;
  never expose a partial dataset as ready.
- Add upload/ingestion UI with progress, schema preview, and retry affordance.

## Acceptance

- CSV and JSON fixtures become valid, registered Parquet and survive worker/API
  restart.
- Invalid files fail with safe, actionable errors and leave no ready catalog
  version or dangling temporary objects.
- Retry after interrupted ingestion does not create duplicate visible versions.
- Integration tests cover MinIO upload, conversion, schema and partial failure.
- `make verify` passes.

## Out of scope

- Streaming CDC and automatic schema evolution beyond an explicit new version.
