---
phase: 1
slug: local-foundation
title: Create the API, web, CI, and local service foundation
touches: go.mod, cmd/, internal/, web/, docker-compose.yml, Makefile, .github/workflows/ci.yml, README.md
verify: make verify
---

## Goal

A clean checkout can start a healthy local stack and verify both Go and frontend
code without requiring pre-existing data.

## Changes

- Create a Go module, minimal API process, structured/redacted logging, config
  validation, `/healthz`, `/readyz`, and graceful shutdown.
- Scaffold a React + TypeScript + Vite web application with a typed API client and
  an initial empty-state page.
- Add Compose services for PostgreSQL, MinIO, API, query-worker placeholder, and
  web app; add readiness/health checks and named local volumes.
- Add Make targets (`up`, `down`, `run`, `web`, `test`, `verify`) with deterministic
  formatting, lint, test, typecheck, and build behavior.
- Add CI that runs the same verification on Go and web code.
- Document local prerequisites and the clean-start command.

## Acceptance

- Compose starts and dependency health is accurately surfaced by the API.
- API and web app build and tests pass from a clean checkout.
- Default `make verify` does not require a running MinIO/Postgres instance.
- No credentials are committed or sent to frontend bundles.

## Out of scope

- Dataset or query functionality; later phases add those contracts.
