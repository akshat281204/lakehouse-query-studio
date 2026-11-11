---
phase: 7
slug: benchmarks-and-fault-tests
title: Measure query behavior and test failure recovery
touches: test/, scripts/, docs/evaluation.md, Makefile, internal/
verify: make verify
---

## Goal

Make performance and reliability claims reproducible and honest.

## Changes

- Add a deterministic sample workload (TPC-H SF1 baseline; SF10 optional) and
  query set covering selective filters, partition pruning, aggregates, and joins.
- Add a benchmark command reporting p50/p95/p99 latency, throughput, concurrency,
  warm/cold cache, rows returned, bytes scanned where available, and ingestion
  throughput. Include machine, versions, memory, and config in output.
- Test failures: MinIO unavailable, corrupt/partial input, worker restart during
  ingestion/query, query timeout/cancellation, API restart, oversized result, and
  temporary-export expiry.
- Compare partitioned vs unpartitioned datasets and record bytes scanned/time; do
  not claim improvement unless the measured result supports it.
- Write methodology, raw results, limitations, and reproduction commands to
  `docs/evaluation.md`.

## Acceptance

- Benchmarks run repeatably from documented commands and report all required
  environment context.
- Failure tests assert persisted job/catalog state and object-store outcomes, not
  merely HTTP status codes.
- Performance results are measured; no assumed target is reported as achieved.
- `make verify` passes; integration tests are explicitly opt-in when requiring
  MinIO or large generated fixtures.

## Out of scope

- Promising a universal latency or throughput number across hardware.
