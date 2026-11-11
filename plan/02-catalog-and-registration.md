---
phase: 2
slug: catalog-and-registration
title: Add dataset catalog and Parquet registration
touches: internal/catalog/, internal/api/, migrations/, web/src/, docs/
verify: make verify
---

## Goal

Users can register existing Parquet data in MinIO/S3 and discover it through a
durable metadata catalog.

## Changes

- Add migrations for namespaces, datasets, dataset versions, schema columns, and
  ingestion job metadata; include uniqueness and foreign-key constraints.
- Implement an object-store abstraction and MinIO/S3 adapter with server-side
  credentials and bounded prefix listing.
- Implement dataset registration: validate bucket/prefix, inspect Parquet schema,
  capture format/partition metadata, and persist a versioned catalog record.
- Add list/detail/schema/preview APIs with paging and consistent error shapes.
- Add dashboard dataset list/detail/schema/preview views with empty/loading/error
  states.

## Acceptance

- Registration survives API restart and rejects missing/unreadable prefixes.
- Schema and preview do not load an unbounded object or result set.
- Tests cover duplicate registration, malformed schema, object-store errors, and
  deterministic identifier handling.
- `make verify` passes.

## Out of scope

- CSV/JSON conversion and SQL execution.
