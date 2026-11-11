---
phase: 0
slug: design-contract
title: Define the object, catalog, SQL, and security contracts
touches: PROJECT_PLAN.md, docs/design.md, docs/architecture.md
verify: git diff --check
---

## Goal

Resolve design ambiguities before implementation: object layout, catalog/version
semantics, ingestion behavior, query job lifecycle, SQL safety boundary, result
retention, and single-node DuckDB limits.

## Changes

- Create `docs/design.md` with dataset, ingestion-job, and query-job state models;
  object key layout; API error contract; result paging/retention; and transitions.
- Create `docs/architecture.md` covering Go API/worker, PostgreSQL catalog,
  MinIO/S3, DuckDB, dashboard, failure boundaries, and deployment topology.
- Record exactly which SQL statements/functions are allowed and how the backend
  prevents reads outside registered dataset prefixes.
- Record that large results use bounded paging or temporary exports, not database
  row storage.
- Do not add implementation code in this phase.

## Acceptance

- Architecture and design identify every durable entity and lifecycle.
- SQL boundary, object access, credentials, timeouts, concurrency, and data
  retention are explicit and testable.
- DuckDB is explicitly single-node; any future distributed engine is deferred.
- `git diff --check` passes.

## Out of scope

- Runtime code, schema migrations, UI, and performance claims.
