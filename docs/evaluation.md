# Evaluation: performance and failure recovery (phase 7)

Everything here was **measured**; no target is reported as achieved. Numbers apply to
the machine below and say nothing about other hardware. Raw results are in
[`docs/results/`](results/) (JSON, plus the Markdown tables the command generates).

## How to reproduce

```bash
make verify                    # includes the fault tests (no object store, no large fixtures)
make bench                     # ~600k lineitem rows, about 10 s; writes docs/results/bench-sf0.1.*
make bench-sf1                 # opt-in: ~6M lineitem rows, several minutes, ~2 GiB RAM
make test-integration          # opt-in: adds Postgres-backed tests (needs TEST_POSTGRES_URL)
go run ./cmd/bench -h          # scale, iterations, concurrency levels, DuckDB threads/memory
```

Each run writes its own environment block (machine, versions, git commit and
dirty flag, limits). The workload is seeded, so two runs generate identical bytes
(`TestWorkloadIsDeterministic`).

## Environment of the recorded results

| | |
|---|---|
| CPU | Apple M4 (10 logical) |
| Memory | 16 GiB |
| OS / arch | darwin / arm64 |
| Go / DuckDB | go1.26.3 / v1.4.1 |
| Commit | `188671f55a5f` plus the uncommitted phase-7 working tree (`dirty: true` in the JSON) |
| Object store | in-memory (no network); the machine was **not isolated** (other processes were running) |
| DuckDB per query | 4 threads, 2 GiB memory, default result limits |

## Methodology

* **Workload: TPC-H-*like*, not TPC-H.** `customer`, `orders`, `lineitem` with TPC-H
  table names, column shapes and cardinality ratios (SF 1 = 150k / 1.5M / ~6M rows), filled
  from a seeded generator. It was not produced by `dbgen`; do not quote these as TPC-H
  results. Dates are ISO strings and money is `double` because the in-repo Parquet
  writer supports only boolean/int64/double/string. "SF1" here means SF1-like cardinalities.
* **Path measured:** the real validator, job store, runner, staging, DuckDB engine and
  Parquet spooling in one process. Latency is end to end (submit to result spooled) and
  is split into **stage** (download dataset files to local scratch), **execute** (DuckDB)
  and **spool**.
* **Correctness first:** every query's first result is checked against ground truth
  computed by the generator (`TestEveryQueryIsValidAndCorrectOnBothLayouts`,
  `TestWrongAnswersAreRejected`). A wrong answer aborts the run instead of producing a number.
* **Query set:** selective filter + aggregate, group-by on one year, full-scan aggregate,
  two-table join, three-way join, point lookup (a scan: there are no indexes), and a large
  unfiltered projection truncated at the 10,000-row limit. All pass the production SQL validator.
* **Samples:** 1 "first" run (first after generation or the previous query; the OS page cache
  is **not** dropped, so this is not a guaranteed cold read) and 15 warm runs. "Warm" means a warm
  OS cache and warmed code paths. These sections use the **baseline runner** (no file pruning, no stage
  cache), so every query re-stages its datasets; pruning and the cache are measured separately below. With 15 samples p95 and p99 sit at or next to the maximum.
* **Throughput:** 24 (SF0.1) or 16 (SF1) queries from the mixed set are submitted at once and
  drained with 1, 2 and 4 concurrent query slots; latency includes queue wait.
* **Partitioned vs unpartitioned:** the same rows are written flat (30 files at SF1) and
  hive-partitioned by `l_year` (210 files). The two layouts' runs are **interleaved**, and the
  full-scan pair is a **control**: pruning cannot help it, so a gap the control also shows is not
  attributed to pruning. A difference is reported only when it exceeds both 10% and 5 ms, and a
  pair with p95 > 2x p50 is marked inconclusive. (An earlier, non-interleaved run claimed
  "partitioned 32% faster" for the filter query *and* 33% for the control; that was run-order
  noise, which is why the method changed.)
* **Bytes scanned:** the engine does not expose it. `bytes staged` is what was downloaded and is an
  upper bound on what DuckDB read.

## Results

### SF0.1-like (600,277 lineitem rows, 50.4 MiB flat), p50 / p95 ms, n = 15

| Query | p50 / p95 | execute p50 |
|---|---:|---:|
| q1-selective-filter | 24 / 52 | 12 |
| q2-year-flat | 23 / 54 | 11 |
| q2p-year-partitioned | 23 / 54 | 13 |
| q3-full-aggregate | 31 / 39 | 16 |
| q3p-full-partitioned | 31 / 80 | 17 |
| q4-join | 16 / 19 | 13 |
| q5-three-way-join | 32 / 41 | 18 |
| q6-point-lookup | 12 / 36 | 9 |
| q7-large-result | 27 / 32 | 15 |

### SF1-like (6,000,346 lineitem rows, 503.6 MiB flat), p50 / p95 ms, n = 15

| Query | p50 / p95 | stage p50 | execute p50 |
|---|---:|---:|---:|
| q1-selective-filter | 714 / 1394 | 627 | 80 |
| q2-year-flat | 758 / 3554 | 681 | 60 |
| q2p-year-partitioned | 707 / 2981 | 583 | 73 |
| q3-full-aggregate | 720 / 4713 | 517 | 158 |
| q3p-full-partitioned | 1001 / 3352 | 522 | 338 |
| q4-join | 44 / 359 | 18 | 23 |
| q5-three-way-join | 358 / 4918 | 203 | 121 |
| q6-point-lookup | 28 / 67 | 15 | 11 |
| q7-large-result | 706 / 4576 | 576 | 108 |

### Throughput under concurrency (queries/s; latency includes queue wait)

SF0.1-like:

| Workers | Queries | Queries/s | p50 ms | p95 ms |
|---:|---:|---:|---:|---:|
| 1 | 24 | 39.16 | 301 | 601 |
| 2 | 24 | 70.51 | 188 | 322 |
| 4 | 24 | 66.92 | 159 | 356 |

SF1-like:

| Workers | Queries | Queries/s | p50 ms | p95 ms |
|---:|---:|---:|---:|---:|
| 1 | 16 | 1.26 | 4379 | 12720 |
| 2 | 16 | 1.23 | 5192 | 12998 |
| 4 | 16 | 1.24 | 10164 | 12880 |

### Partitioned vs unpartitioned (SF1-like)

| Comparison | flat p50 | partitioned p50 | flat execute p50 | partitioned execute p50 | bytes staged flat / partitioned |
|---|---:|---:|---:|---:|---|
| filter on partition column (l_year = 1995) | 758 | 707 | 60 | 73 | 528,061,888 / 480,222,140 |
| full scan, no pruning possible (control) | 720 | 1001 | 158 | 338 | 528,061,888 / 480,222,140 |

The harness's own verdicts: end to end, **no measurable difference** for the filter pair
(inconclusive: high variance); the control pair differed, so layout or noise affects the
comparison. Engine-only, the partitioned layout was **slower** (about 20% on the filter query,
roughly 2x on the full scan); the filter pair is flagged high-variance.

### Ingestion throughput (CSV to Parquet, 300,000 lineitem rows, in-memory store)

SF0.1 run:

| Rows | Input MiB | Output MiB | Seconds | Rows/s | Partition |
|---:|---:|---:|---:|---:|---|
| 300,000 | 16.6 | 25.2 | 0.34 | 875,331 | none |
| 300,000 | 16.6 | 25.2 | 0.39 | 762,269 | l_returnflag |

SF1 run (independent repeat of the same upload):

| Rows | Input MiB | Output MiB | Seconds | Rows/s | Partition |
|---:|---:|---:|---:|---:|---|
| 300,000 | 16.6 | 25.2 | 0.36 | 834,648 | none |
| 300,000 | 16.6 | 25.2 | 0.37 | 806,660 | l_returnflag |

## What the measurements say (and do not)

1. **Staging dominates at scale.** At SF1-like, staging is most of a query's latency (e.g. q1:
   about 630 ms staging vs about 80 ms execution) because every query downloads every file of every
   dataset it names. DuckDB execution itself is tens to low hundreds of milliseconds. The
   in-memory store excludes network cost, so against a real S3-compatible store staging would be slower, not
   faster. This is the largest performance cost of the current design (DuckDB reads local files,
   not S3; see `docs/queries.md`).
2. **No partition-pruning benefit was measured.** The whole dataset is staged either way (the
   partitioned layout stages about 9% fewer bytes only because the partition column is not stored
   in the files), and DuckDB execute time was equal or worse with 210 small files than with 30.
   Pruning that helps would require staging only matching partitions, which is not implemented.
   Do not claim a partitioning speedup from this system.
3. **Concurrency scales at small data and not at SF1-like.** SF0.1: roughly 39 to 70 queries/s
   from 1 to 2 workers, then flat at 4. SF1: about 1.2-1.3 queries/s at every level; the workload
   is bound by copying about 0.5 GiB per query to local disk, which more workers do not speed up.
4. **Latency is noisy at SF1-like** (p95 is often 3-10x the median; similar bytes staged took
   anywhere from about 200 ms to about 700 ms). Medians are indicative; tails are not stable.
5. **Ingestion** converts roughly 40-50 MiB/s of CSV (about 760k-875k rows/s here) end to end with
   an in-memory store. It is single-threaded per job and buffers the upload in memory (64 MiB /
   1M-row limit).
6. Nothing here establishes a latency target or SLO, or compares against another engine.

## Failure and recovery tests

`test/faults` chains the real API handlers, catalog stores, ingestion and query runners, the DuckDB
engine and the sweeper over an object store that can be made to fail (`objectstore.Faulty`). Every
test asserts **persisted job/catalog state and object-store contents**, not only status codes. They
run in `make verify`; the Postgres variant is opt-in (`make test-integration`).

| Scenario | Asserted state |
|---|---|
| Object store down while querying | job `FAILED` / `object_store`; no result fields; no `exports/`; no scratch dir; next query succeeds after recovery |
| Object store fails while spooling | `FAILED`; no `result_ref`; results endpoint 409; no partial spool |
| Object store fails mid-ingestion, then retry | job `FAILED` retryable, version `FAILED`, no `lake/` or `staging/` objects, query on it denied; retry gives version 2 `READY` and an exact `count(*)` |
| Corrupt Parquet (garbage / truncated / bad magic) | that query `FAILED` / `engine_error`, no object key leaked, healthy dataset unaffected, only its spool exists |
| Corrupt or partial upload (ragged, header-only, binary, unterminated quote, empty) | `FAILED` non-retryable `invalid_input`, version `FAILED`, no data objects written, no input bytes echoed |
| Query worker killed mid-query | job stays `RUNNING` (not terminal); after lease expiry a new worker finishes it as attempt 2; only the `attempt=2` spool exists |
| Ingestion worker killed mid-write (partial `lake/` write) | job `RUNNING`, then recovered as attempt 2; published prefix is `attempt=2`; `count(*)` equals the input exactly (no double counting from the dead attempt) |
| API restart | queued and finished jobs, history and results readable from a brand-new handler; Postgres variant: new pool/store/handler sees the queued job, a worker runs it, the row is `SUCCEEDED` |
| Real query timeout (DuckDB) | `TIMED_OUT` (not `FAILED`), no spool, scratch removed |
| Real query cancel (DuckDB) | `CANCELLED` within seconds of the API call, no spool, scratch removed |
| Oversized result | `SUCCEEDED`, `truncated`, exactly the row limit; results and CSV export agree. A single oversized row gives `FAILED` / `resource_exceeded` and no spool |
| Temporary-export expiry | sweeper clears `result_ref` and deletes the object; results and export answer `410 result_expired`; the row is kept; dataset objects untouched |

Findings from these tests that changed the product:

* **Integer aggregates came back as text.** `SELECT sum(id)` is a DuckDB `HUGEINT`, which the spool
  wrote as a string, so sums did not chart and compared as text. Fixed: wide integers become `int64`
  when every value fits and stay exact text otherwise; `DECIMAL` columns become `double` when no
  value exceeds 15 significant digits (`TestWideIntegersAndDecimalsKeepUsableTypes`).
* A data race in the benchmark harness was caught by `-race` and fixed.

### Known limits these tests document, not fix

* An ingestion attempt that dies leaves its `lake/.../attempt=N/` objects behind. They are
  unreferenced and never queried (asserted above), but they are not reclaimed; only `exports/` and
  `staging/` are swept.
* The "object store down" cases use the injected-fault in-memory store, not a stopped object-store
  container. The S3 client has HTTP-level tests and an opt-in integration test against a real
  S3-compatible server (SeaweedFS); `cmd/bench -s3` exists but has not been run against one.

## Limitations of this evaluation

* One machine, one recorded run per scale (earlier repeats agreed on every conclusion above), not isolated.
* TPC-H-like synthetic data, uniform distributions, no skew; strings are uncompressed (the writer
  has no compression), so file sizes and staging cost are larger than with typical Parquet.
* In-memory object store; no network or S3 behavior is represented.
* Latencies are small-sample statistics; percentiles above p95 are effectively the maximum.
* No comparison against other engines or other DuckDB configurations (threads and memory were fixed).

## Phase 9: file pruning and the stage cache

Phase 7 found that staging (copying every file of every referenced dataset) dominates latency and that
partitioning gave no measurable benefit, because the same bytes were staged either way. Phase 9 changes
what is staged. What was added is described in [`queries.md`](queries.md); this section only reports what was
**measured**.

**Method.** `make bench` now ends its query measurements with an experiment on one dataset and five
statements, each run under three runner configurations **interleaved** (baseline, then file pruning, then
file pruning + stage cache, repeated 15 times) so machine drift hits all three equally. The warm-up run of each
configuration also verifies its answer is **identical** to the baseline's; a different answer aborts the run.
Same reporting rule as before: a difference is stated only if it exceeds 10% **and** 5 ms, and a variant with
p95 > 2x p50 is marked inconclusive. Files: `docs/results/bench-phase9-sf0.1.*` (in-memory store),
`bench-phase9-sf0.1-s3.*` (a real S3-compatible server, SeaweedFS over loopback HTTP, objects deleted afterwards)
and `bench-phase9-sf1.*` (in-memory, about 6M lineitem rows). The unsuffixed `bench-sf0.1.*` / `bench-sf1.*` are the
unchanged phase-7 baseline runs the earlier sections of this document describe. Same machine and caveats as above (not isolated).

The five statements (the third and fifth are controls that **cannot** be pruned):

| ID | Statement shape | Can it prune? |
|---|---|---|
| o1 | `WHERE l_year = 1995` on the hive-partitioned layout | yes: directory names |
| o2 | `WHERE l_orderkey BETWEEN 1 AND 20000` on flat files clustered by order key | yes: footer min/max |
| o3 | `WHERE l_year = 1995` on the flat layout (every file holds every year) | no |
| o4 | `WHERE o_orderkey = 17` on `orders` | only when `orders` has several files (sf1) |
| o5 | unfiltered aggregate | no filter |

**Results, real S3 requests (sf0.1, p50 end to end ms, baseline / pruning / pruning + cache):**

| | baseline | file pruning | + stage cache | files staged / listed | bytes staged |
|---|---:|---:|---:|---:|---|
| o1 partition column | 266 | 47 | 16 | 3 / 21 | 45.8 -> 6.5 MiB |
| o2 clustered range | 247 | 91 | 13 | 1 / 3 | 50.4 -> 16.8 MiB |
| o3 spread column | 232 | 241 | 15 | 3 / 3 | 50.4 MiB (unchanged) |
| o4 point lookup (1 file) | 45 | 46 | 13 | 1 / 1 | 7.2 MiB (unchanged) |
| o5 full scan | 265 | 258 | 20 | 3 / 3 | 50.4 MiB (unchanged) |

**Results at about 6M rows (sf1, in-memory store):**

| | baseline p50 | file pruning p50 | files staged / listed | bytes staged |
|---|---:|---:|---|---|
| o1 partition column | 180 | 43 | 30 / 210 | 458.0 -> 65.4 MiB |
| o2 clustered range | 759 | 17 | 1 / 30 | 503.6 -> 16.8 MiB |
| o4 point lookup | 92 | 14 | 1 / 8 | 72.1 -> 9.6 MiB |

What the numbers support:

* **Pruning removes the bytes it can prove unnecessary, and the saving follows the bytes.** Staged bytes fell by
  86% (o1) and 67% (o2) at sf0.1 and by 86%, 97% and 87% (o1, o2, o4) at sf1. End-to-end p50 fell from 266 to 47 ms
  (o1, -82%) and 247 to 91 ms (o2, -63%) over real HTTP requests, where the run-to-run spread was small (p95 within
  about 20% of p50). The sf1 and in-memory runs show the same direction with much larger spread (p95 up to 5x p50
  on this non-isolated machine), so their millisecond figures are indicative only.
* **This reverses the Phase 7 finding about partitioning.** `l_year = 1995` on the partitioned layout now stages
  3 of 21 files (30 of 210 at sf1) instead of all of them; Phase 7 measured identical bytes.
* **Pruning only helps where the data is laid out to allow it.** o3 filters on a column present in every file, so
  no file can be ruled out: 3 of 3 files were staged and the p50 difference (232 vs 241 ms) is under the reporting
  threshold. Flat files help when they are **clustered** by the filtered column (o2), not when the column is
  spread across them (o3).
* **The stage cache removes the copy.** With every file already cached (repeated identical access: the best
  case) staging dropped to about 1 ms and end-to-end p50 to 13-20 ms in every row, including the unprunable
  ones (o3, o5: 232 -> 15 ms and 265 -> 20 ms over real HTTP). It does not change what DuckDB does (execute
  times are unchanged) and it only helps repeats; a first read pays the full cost, and the cache run's first
  execution is not faster than the baseline's.
* **Pruning overhead is small but not zero.** With statistics already memoised, evaluating 200 files costs
  about 0.11 ms (`BenchmarkPruneObjectsMemoised`); reading and parsing 200 footers from an in-memory store costs
  about 0.33 ms (`BenchmarkPruneObjectsColdFooters`). **Not measured:** the cost of the first query on a large
  dataset against a remote store, where 200 files means 400 range requests (run 8 at a time). In the loopback
  run, the first execution of o3 (three footers read, nothing pruned) took 303 ms against 230 ms for the
  baseline's first run; with a single sample that cannot be separated from noise.

What is **not** supported by these runs, and is not claimed: any gain from pruning for statements outside the
simple single-table `AND` shape; any gain on a remote store with real latency (only loopback was measured);
anything about hardware other than this machine; and the figures in the sf1 and in-memory tables beyond their
direction, because of their variance. The unprunable controls show the in-memory runs' noise floor: o4 and o5 at
sf0.1 execute identical work in both variants yet their medians differ by up to 19% (90 vs 107 ms for o5).
