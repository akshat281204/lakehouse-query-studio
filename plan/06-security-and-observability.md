---
phase: 6
slug: security-and-observability
title: Harden SQL access and expose query-service health
touches: internal/query/, internal/api/, internal/telemetry/, web/src/, docker-compose.yml
verify: make verify
---

## Goal

Keep untrusted query input inside its intended data boundary and make service/job
health visible without leaking secrets or high-cardinality identifiers.

## Changes

- Review and test read-only SQL controls, per-query time/memory/thread/result
  limits, cancellation, worker concurrency, and cleanup of temporary exports.
- Scope object-store permissions and prefixes; separate query read permissions from
  ingestion writes. Keep credentials out of frontend and logs.
- Add structured logs, traces, and bounded-cardinality Prometheus metrics for
  ingestion/query outcomes, duration, queue depth, cancellations, and worker
  availability.
- Add a service-operations dashboard or Grafana panels for API/worker health,
  query latency/failures, queue depth, and object-store availability. This is
  separate from the user-facing SQL dashboard.
- Add audit-safe query history behavior and retention configuration.

## Acceptance

- Security tests exercise escape attempts and query/resource limits.
- Sentinel credentials and SQL literals do not appear in logs/metrics/traces.
- Labels exclude query text, object keys, user-controlled values, and dataset IDs.
- Services run with least-privilege object access documented.
- `make verify` passes.

## Out of scope

- Hosted identity provider, billing, multi-region, or customer-facing RBAC.
