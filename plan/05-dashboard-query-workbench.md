---
phase: 5
slug: dashboard-query-workbench
title: Build the dataset dashboard and embedded SQL workbench
touches: web/src/, internal/api/, docs/
verify: make verify
---

## Goal

Users can discover datasets and run, inspect, visualize, and export queries without
leaving the dashboard.

## Changes

- Build an overview with dependency status, dataset count, recent query activity,
  and active/failed job counts sourced from real API data.
- Complete dataset search/list/detail/schema/preview and ingestion status screens.
- Add a SQL editor with syntax highlighting, registered-schema browser, run/cancel,
  elapsed/status indicators, and safe error display.
- Add paginated results, row/column counts, CSV export, basic chart selection, and
  an explain/execution detail panel when available.
- Add query history with status/duration and a safe rerun action. Keep API results
  typed and handle loading, empty, and error states.
- Ensure no MinIO/S3 secret is present in page source, browser storage, or network
  responses.

## Acceptance

- Browser end-to-end test registers or selects a dataset, runs SQL, sees results,
  creates a chart, and exports data.
- Query cancellation and long-running states are visible and work.
- UI tests cover empty/error/loading states and API failures.
- `make verify` passes.

## Out of scope

- Multi-user authorization and a full BI semantic/modeling layer.
