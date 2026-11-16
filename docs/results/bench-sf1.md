### Environment

| | |
|---|---|
| Generated | 2026-11-14T17:09:55Z |
| OS / arch | darwin / arm64 |
| CPU | Apple M4 (10 logical) |
| Memory | 16.0 GiB |
| Go | go1.26.3 |
| DuckDB | v1.4.1 |
| Commit | `188671f55a5f7c9cb833f4984cf7bd92fe2ceb86` (dirty: true) |
| Object store | in-memory object store (no network latency; staging is a memory copy) |
| OS cache | not dropped (needs root); 'first' runs are first-after-generation, not guaranteed cold |

### Configuration

Scale 1 (TPC-H-like, seed 20240601), 15 warm iterations per query. Query limits: timeout 5m0s, 4 threads, memory 2GiB, max rows 10000, max result bytes 268435456, max scan bytes 4294967296. max rows/result bytes are the product defaults.

### Dataset

| Table | Rows | Files | Size |
|---|---:|---:|---:|
| customer | 150000 | 1 | 5.3 MiB |
| orders | 1500000 | 8 | 72.1 MiB |
| lineitem | 6000346 | 30 | 503.6 MiB |
| lineitem_part | 6000346 | 210 | 458.0 MiB |

### Query latency (single client, end to end, ms)

n = 15 warm runs per query; with so few samples p95/p99 are at or near the maximum. Every result was checked against ground truth computed by the generator.

| Query | first | p50 | p95 | p99 | max | stage p50 | execute p50 | spool p50 | rows | staged MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q1-selective-filter | 350 | 714 | 1394 | 1394 | 1394 | 627 | 80 | 0 | 1 | 503.6 |
| q2-year-flat | 2147 | 758 | 3554 | 3554 | 3554 | 681 | 60 | 0 | 3 | 503.6 |
| q2p-year-partitioned | 2223 | 707 | 2981 | 2981 | 2981 | 583 | 73 | 0 | 3 | 458.0 |
| q3-full-aggregate | 779 | 720 | 4713 | 4713 | 4713 | 517 | 158 | 0 | 6 | 503.6 |
| q3p-full-partitioned | 1024 | 1001 | 3352 | 3352 | 3352 | 522 | 338 | 0 | 6 | 458.0 |
| q4-join | 61 | 44 | 359 | 359 | 359 | 18 | 23 | 0 | 5 | 77.4 |
| q5-three-way-join | 1044 | 358 | 4918 | 4918 | 4918 | 203 | 121 | 0 | 5 | 581.0 |
| q6-point-lookup | 28 | 28 | 67 | 67 | 67 | 15 | 11 | 0 | 1 | 72.1 |
| q7-large-result | 260 | 706 | 4576 | 4576 | 4576 | 576 | 108 | 0 | 10000 (trunc.) | 503.6 |

### Throughput under concurrency

| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16 | 12.72 | 1.26 | 4379 | 12720 | 12720 | 0 |
| 2 | 16 | 13.00 | 1.23 | 5192 | 12998 | 12998 | 0 |
| 4 | 16 | 12.88 | 1.24 | 10164 | 12880 | 12880 | 0 |

### Partitioned vs unpartitioned

| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |
|---|---:|---:|---:|---:|---|---|---|
| filter on partition column (l_year = 1995) | 758 | 707 | 60 | 73 | 528061888 / 480222140 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | partitioned execute 22% slower (p50 73 vs 60 ms); INCONCLUSIVE: high variance |
| full scan, no pruning possible (control) | 720 | 1001 | 158 | 338 | 528061888 / 480222140 | partitioned layout 39% slower end to end (p50); the control differs, so layout or measurement noise affects this comparison; INCONCLUSIVE: high variance (p95 > 2x p50) | partitioned execute 114% slower (p50 338 vs 158 ms) |

### Ingestion throughput (CSV -> Parquet)

| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |
|---:|---:|---:|---:|---:|---:|---:|---|
| 300000 | 16.6 | 25.2 | 3 | 0.36 | 834648 | 46.2 | none |
| 300000 | 16.6 | 25.2 | 4 | 0.37 | 806660 | 44.7 | l_returnflag |

### Limitations of this run

- Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.
- Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.
- The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.
- Every query stages (downloads) all files of every dataset it references before DuckDB runs; there is no cache, so 'warm' means a warm OS page cache and warmed code paths, not a result or data cache.
- The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.
- 'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.
- Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.
- Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.
