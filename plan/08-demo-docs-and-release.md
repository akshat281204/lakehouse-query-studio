---
phase: 8
slug: demo-docs-and-release
title: Ship a repeatable demo, runbook, and project documentation
touches: README.md, docs/, scripts/, Makefile, docker-compose.yml
verify: make verify
---

## Goal

A reviewer can launch the stack, load sample data, execute a query in the
dashboard, see results, and reproduce the documented evaluation.

## Changes

- Add `make demo` that starts dependencies, waits for health, loads a small
  deterministic sample dataset, registers it, runs a representative SQL query,
  and prints the dashboard URL and result summary.
- Make the demo safe to rerun, bounded in time, and explicit about cleanup. It
  must not delete user data or broad Docker volumes.
- Document system architecture, data layout, API, query-safety model, local setup,
  troubleshooting, configuration, benchmark method/results, and single-node
  limits.
- Update README with features actually implemented, screenshots if available,
  setup/demo commands, measured results, and honest non-goals.
- Perform final clean-checkout validation and resolve all CI/review findings.

## Acceptance

- `make demo` completes unattended and displays a working dashboard query result.
- README's checked features are backed by working commands/tests.
- `make verify` passes from a clean checkout.
- Final benchmark numbers and limitations match `docs/evaluation.md`.
- No unmeasured performance claims are introduced.

## Out of scope

- Hosted production deployment, distributed query engine, or hosted Grafana.
