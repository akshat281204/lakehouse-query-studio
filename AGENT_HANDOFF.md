# Agent handoff — Lakehouse Query Studio

This directory is the project brief for a new, separate repository. The user will
hand this file and `PROJECT_PLAN.md` to an implementation agent and ask it to
initiate the project. **Do not put this project inside the existing
`provisioning-orchestrator` repository.** Use this directory as the project/repo
root, unless the user gives a different location.

## Mission

Build a local-first OLAP query workbench over S3-compatible object storage. Users
store or register Parquet datasets in S3/MinIO, discover them in a web dashboard,
and run SQL against them from an embedded query platform. The initial engine is
single-node DuckDB; this is a query layer over object storage, not a claim that
S3 itself has become a distributed database.

The dashboard is a first-class deliverable: dataset browser, SQL editor, query
execution/status, results, basic charts, exports, and query history. A separate
Grafana dashboard may be added for service-health metrics, but it does not replace
the user-facing query dashboard.

## Read before implementation

1. Read `PROJECT_PLAN.md` and every `plan/NN-*.md` phase file. Treat them as the
   product contract. Do not broaden the scope or silently change the architecture.
2. Invoke the installed **Stackflow** skill before setting up the PR loop. Use its
   current instructions and reference templates; do not vendor/copy the skill's
   driver implementation into this repository.
3. Preserve the initial plan and handoff documents in the new repository.
4. Implement one numbered phase at a time. Every phase must leave the repository
   buildable/testable on its own. Do not start phase N+1 until phase N has passed
   verification and reviewer approval; the Stackflow driver handles this order.

## Initiation checklist

Run these checks from this directory before creating or modifying a GitHub repo:

```bash
pwd
gh auth status
opencode models
stack --help
```

- Confirm the authenticated GitHub account is `akshat281204` (or ask the user if
  it is not).
- Check whether `akshat281204/lakehouse-query-studio` already exists. If it does,
  inspect it and preserve its work; do not overwrite or force-push it.
- If it does not exist, initialize this directory on `main`, commit the brief and
  plan as the bootstrap commit, and create a **public** repo named
  `akshat281204/lakehouse-query-studio` with this directory as its source.
- The user's goal is autonomous implementation. Once the preflight succeeds,
  proceed without asking for routine approval. Ask only for a genuine product
  decision, an access/authorization blocker, or a destructive action that would
  overwrite existing user data.

## Stackflow setup and operation

1. Invoke the installed `stackflow` skill and follow its **Setup**, **Writing the
   plan**, **Running**, **Merging**, and **When the stack drifts** instructions.
2. From the project root run `stack init` **without** `--example`. The `plan/`
   directory in this handoff is already populated; the scaffold copies files only
   when absent. Do not replace it with sample phases.
3. Configure `stack.config.json` for the plan directory, `make verify`, the
   `builder` and `reviewer` agents, and a review cap of 3. Select currently
   available models from different model families (`opencode models`); smoke-test
   that both resolve before running. The models previously used in the workspace
   are examples only, not requirements.
4. Create `.opencode/agents/builder.md` and `reviewer.md` using the Stackflow
   skill's reference templates. Keep the builder unable to change plan files,
   Git state, and the reviewer's verdict. Keep the reviewer read-only on source,
   able to read the exact diff and write only to `.review/`.
5. Run `stack validate` and `stack doctor`. Resolve every reported problem before
   spending model calls. Configure the repository for **merge commits only**;
   disable squash and rebase merging. Do not force-push. Use Stackflow's
   `stack-order` workflow and the exact required status checks it reports.
6. Run `stack run`. It should open each phase PR against `main` and continue to the
   next phase without waiting for manual merges. The user will merge PRs in order.
7. Monitor with `stack watch` / `stack status`. If a phase fails, preserve its
   branch and work, inspect `.review/phase-N.md` and `.stack/stdout.log`, fix the
   concrete issue, then resume using `stack run --from N`. Never reset a branch
   that contains phase work. Use `--review-only` when the committed tree only
   needs a fresh review.
8. A red CI check is not approval. Fix the branch, re-review when needed, and
   keep later phases based on the approved parent. Do not lower test coverage or
   weaken acceptance criteria just to make a phase green.

## Build and quality rules

- Go API / catalog / job coordinator; React + TypeScript dashboard; DuckDB behind
  a small query-engine interface; PostgreSQL for durable metadata and job state;
  MinIO for local S3-compatible object storage.
- Pin tool/dependency versions. Keep the local experience reproducible through
  Docker Compose and `make` targets.
- Never expose MinIO/S3 credentials to browser code. Never log credentials,
  query parameters, or raw object-store secrets.
- SQL execution must be bounded and constrained to registered datasets. Reject
  mutating or dangerous statements, local-file access, extension installation,
  and arbitrary object paths. Do not rely on keyword filtering alone; use a
  parser/engine controls and add adversarial tests.
- Query work is asynchronous and persisted. Cancellation, timeout, worker restart,
  and object-store failure must leave a truthful durable job state.
- Report measured performance only. Do not turn targets in the plan into resume
  claims unless the benchmark actually ran and recorded the environment/results.
- Do not claim distributed execution: the first version uses single-node DuckDB.

## Definition of complete

All phases 0–8 are implemented, reviewed, verified, and represented by ordered PRs;
`make verify` passes; the local Compose demo can ingest/register data and run a
query from the dashboard; safety and recovery tests pass; and benchmark results
and limitations are written down. The README must state actual measured results,
not aspirational ones.
