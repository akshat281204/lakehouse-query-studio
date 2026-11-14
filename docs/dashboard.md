# Dashboard and SQL workbench (phase 5)

Routes (hash-based): `#/` Overview, `#/datasets` and `#/datasets/{id}`,
`#/query` SQL workbench, `#/history`, `#/upload`. The app is dependency-free
beyond React: the editor highlighting and charts are small in-repo components.

## Screens
* **Overview** – dependency status from `/readyz` (including the per-dependency
  detail the API sends with a 503), dataset total, and active/failed counts for
  queries and ingestions. Counts are computed from the most recent jobs the API
  returns (up to 100 queries), and say so. A section whose request fails shows
  its error and `—`; no number is ever invented.
* **SQL workbench** – textarea with a highlighted mirror (cosmetic only; the
  server parses and validates every statement), schema browser listing READY
  datasets with lazy column schemas and click-to-insert, Run (Ctrl/⌘+Enter),
  Cancel, status badge and elapsed time. Polls the job until terminal.
  Queued, running, cancelling, cancelled, failed, timed-out and denied states
  each have their own message. Failure text is the server's bounded summary.
* **Results** – paged table (100 rows per page, from `/results`), row × column
  counts, truncation notice, CSV export link (the browser downloads
  `/export` directly), a bar/line chart of any numeric column against any label
  column (uses the loaded page only and says how many rows it skipped), and a
  Details tab with duration, rows, bytes staged, result size, attempt and the
  bounded execution plan.
* **History** – status, duration and rows per query. **Rerun** submits the same
  text as a new job through the normal validated path (it never reuses a stored
  result or bypasses validation); the statement is passed in memory only.

## Safety
* Cell values and errors are rendered as React text; there is no
  `dangerouslySetInnerHTML`. A test renders `<img onerror>` as literal text.
* The app uses no `localStorage`, `sessionStorage` or IndexedDB.
* `scripts/check-bundle.sh` (run by `make web-build`, hence `make verify`)
  fails if the built bundle contains credential variable names or any real
  secret value from the local `.env`, or if the source uses browser storage.
  The API never returns credentials, and the browser only ever talks to the API.

## Tests
Unit and component tests cover loading, empty, error and failure states for
every screen, cancel and long-running states, paging, chart behavior, schema
browser insertion, and the highlighter (round-trips any input).
`App.flow.test.tsx` drives the real App, client and pages against a fake API at
the `fetch` boundary through: pick a dataset → run SQL → results → chart →
export link → history → rerun, plus denial and overview.

**Not covered:** a real-browser end-to-end run against the full stack. It needs
the compose stack including MinIO, whose image could not be pulled in the build
environment, so the flow test above runs in jsdom with a fake API instead.
