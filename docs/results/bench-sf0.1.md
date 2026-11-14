### Environment

| | |
|---|---|
| Generated | 2026-11-14T17:09:43Z |
| OS / arch | darwin / arm64 |
| CPU | Apple M4 (10 logical) |
| Memory | 16.0 GiB |
| Go | go1.26.3 |
| DuckDB | v1.4.1 |
| Commit | `188671f55a5f7c9cb833f4984cf7bd92fe2ceb86` (dirty: true) |
| Object store | in-memory object store (no network latency; staging is a memory copy) |
| OS cache | not dropped (needs root); 'first' runs are first-after-generation, not guaranteed cold |

### Configuration

Scale 0.1 (TPC-H-like, seed 20240601), 15 warm iterations per query. Query limits: timeout 5m0s, 4 threads, memory 2GiB, max rows 10000, max result bytes 268435456, max scan bytes 4294967296. max rows/result bytes are the product defaults.

### Dataset

| Table | Rows | Files | Size |
|---|---:|---:|---:|
| customer | 15000 | 1 | 0.5 MiB |
| orders | 150000 | 1 | 7.2 MiB |
| lineitem | 600277 | 3 | 50.4 MiB |
| lineitem_part | 600277 | 21 | 45.8 MiB |

### Query latency (single client, end to end, ms)

n = 15 warm runs per query; with so few samples p95/p99 are at or near the maximum. Every result was checked against ground truth computed by the generator.

| Query | first | p50 | p95 | p99 | max | stage p50 | execute p50 | spool p50 | rows | staged MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q1-selective-filter | 32 | 24 | 52 | 52 | 52 | 11 | 12 | 0 | 1 | 50.4 |
| q2-year-flat | 24 | 23 | 54 | 54 | 54 | 11 | 11 | 0 | 3 | 50.4 |
| q2p-year-partitioned | 26 | 23 | 54 | 54 | 54 | 8 | 13 | 0 | 3 | 45.8 |
| q3-full-aggregate | 28 | 31 | 39 | 39 | 39 | 12 | 16 | 0 | 6 | 50.4 |
| q3p-full-partitioned | 28 | 31 | 80 | 80 | 80 | 12 | 17 | 0 | 6 | 45.8 |
| q4-join | 17 | 16 | 19 | 19 | 19 | 2 | 13 | 0 | 5 | 7.7 |
| q5-three-way-join | 59 | 32 | 41 | 41 | 41 | 13 | 18 | 0 | 5 | 58.1 |
| q6-point-lookup | 13 | 12 | 36 | 36 | 36 | 2 | 9 | 0 | 1 | 7.2 |
| q7-large-result | 26 | 27 | 32 | 32 | 32 | 11 | 15 | 0 | 10000 (trunc.) | 50.4 |

### Throughput under concurrency

| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 24 | 0.61 | 39.16 | 301 | 601 | 612 | 0 |
| 2 | 24 | 0.34 | 70.51 | 188 | 322 | 339 | 0 |
| 4 | 24 | 0.36 | 66.92 | 159 | 356 | 358 | 0 |

### Partitioned vs unpartitioned

| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |
|---|---:|---:|---:|---:|---|---|---|
| filter on partition column (l_year = 1995) | 23 | 23 | 11 | 13 | 52827520 / 48041606 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | no measurable execute-time difference |
| full scan, no pruning possible (control) | 31 | 31 | 16 | 17 | 52827520 / 48041606 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | no measurable execute-time difference |

### Ingestion throughput (CSV -> Parquet)

| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |
|---:|---:|---:|---:|---:|---:|---:|---|
| 300000 | 16.6 | 25.2 | 3 | 0.34 | 875331 | 48.5 | none |
| 300000 | 16.6 | 25.2 | 4 | 0.39 | 762269 | 42.2 | l_returnflag |

### Limitations of this run

- Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.
- Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.
- The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.
- Every query stages (downloads) all files of every dataset it references before DuckDB runs; there is no cache, so 'warm' means a warm OS page cache and warmed code paths, not a result or data cache.
- The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.
- 'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.
- Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.
- Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.
