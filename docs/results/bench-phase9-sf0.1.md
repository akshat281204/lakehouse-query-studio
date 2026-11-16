### Environment

| | |
|---|---|
| Generated | 2026-11-15T10:47:58Z |
| OS / arch | darwin / arm64 |
| CPU | Apple M4 (10 logical) |
| Memory | 16.0 GiB |
| Go | go1.26.3 |
| DuckDB | v1.4.1 |
| Commit | `1071148a0f2ad9f25a6649a82a32939a7ae8c103` (dirty: true) |
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
| q1-selective-filter | 31 | 29 | 96 | 96 | 96 | 16 | 12 | 0 | 1 | 50.4 |
| q2-year-flat | 46 | 24 | 94 | 94 | 94 | 11 | 11 | 0 | 3 | 50.4 |
| q2p-year-partitioned | 50 | 24 | 35 | 35 | 35 | 8 | 13 | 0 | 3 | 45.8 |
| q3-full-aggregate | 27 | 29 | 59 | 59 | 59 | 11 | 16 | 0 | 6 | 50.4 |
| q3p-full-partitioned | 28 | 29 | 70 | 70 | 70 | 10 | 18 | 0 | 6 | 45.8 |
| q4-join | 15 | 15 | 17 | 17 | 17 | 3 | 12 | 0 | 5 | 7.7 |
| q5-three-way-join | 79 | 32 | 70 | 70 | 70 | 13 | 17 | 0 | 5 | 58.1 |
| q6-point-lookup | 11 | 12 | 39 | 39 | 39 | 2 | 10 | 0 | 1 | 7.2 |
| q7-large-result | 28 | 29 | 47 | 47 | 47 | 11 | 15 | 0 | 10000 (trunc.) | 50.4 |

### Throughput under concurrency

| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 24 | 0.66 | 36.27 | 367 | 638 | 662 | 0 |
| 2 | 24 | 0.53 | 45.55 | 366 | 514 | 527 | 0 |
| 4 | 24 | 0.62 | 38.98 | 521 | 606 | 615 | 0 |

### Partitioned vs unpartitioned

| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |
|---|---:|---:|---:|---:|---|---|---|
| filter on partition column (l_year = 1995) | 24 | 24 | 11 | 13 | 52828240 / 48046142 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | no measurable execute-time difference |
| full scan, no pruning possible (control) | 29 | 29 | 16 | 18 | 52828240 / 48046142 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | no measurable execute-time difference |

### File pruning and stage cache

Same dataset, same queries, three runner configurations measured interleaved. The answer of every variant was checked identical to the baseline's. 'First run' is the first execution of that variant (footers unread, cache empty: the one-time cost); p50/p95 are warm repeats, where 'cache' numbers are the repeated-identical-access best case (every file already cached).

**o1-partition-column** — filter on the hive partition column (l_year = 1995), partitioned layout

`SELECT l_returnflag, count(*) AS n FROM lineitem_part WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 22 | 45 | 118 | 8 | 18 | 21 / 21 | 45.8 MiB | 0 |
| file pruning | 12 | 12 | 24 | 1 | 10 | 3 / 21 | 6.5 MiB | 0 |
| file pruning + stage cache | 13 | 11 | 13 | 1 | 10 | 3 / 21 | 6.5 MiB | 3 |

- file pruning: 73% faster end to end (p50 12 vs 45 ms); staged 3 of 21 files, 39.3 MiB less (86% of the baseline bytes); INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): no measurable end-to-end difference (under 10% or 5 ms; p50 12 vs 11 ms); 3 of 3 staged files served from the cache

**o2-clustered-range** — range on the column the flat files are clustered by (l_orderkey), flat layout

`SELECT count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem WHERE l_orderkey BETWEEN 1 AND 20000`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 60 | 37 | 576 | 21 | 15 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 25 | 19 | 104 | 4 | 12 | 1 / 3 | 16.8 MiB | 0 |
| file pruning + stage cache | 32 | 10 | 16 | 0 | 9 | 1 / 3 | 16.8 MiB | 1 |

- file pruning: 50% faster end to end (p50 19 vs 37 ms); staged 1 of 3 files, 33.6 MiB less (67% of the baseline bytes); INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 45% faster end to end (p50 10 vs 19 ms); 1 of 1 staged files served from the cache

**o3-spread-column** — filter on a column present in every file (l_year), flat layout: cannot prune

`SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 60 | 67 | 579 | 37 | 13 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 69 | 73 | 751 | 55 | 14 | 3 / 3 | 50.4 MiB | 0 |
| file pruning + stage cache | 91 | 13 | 34 | 1 | 11 | 3 / 3 | 50.4 MiB | 3 |

- file pruning: no measurable end-to-end difference (under 10% or 5 ms; p50 67 vs 73 ms); footers were checked but no files could be ruled out, so none were skipped; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 83% faster end to end (p50 13 vs 73 ms); 3 of 3 staged files served from the cache

**o4-point-lookup** — single-order lookup on the clustered key, orders table

`SELECT o_orderkey, o_custkey, o_totalprice FROM orders WHERE o_orderkey = 17`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 13 | 13 | 16 | 2 | 11 | 1 / 1 | 7.2 MiB | 0 |
| file pruning | 12 | 13 | 166 | 2 | 10 | 1 / 1 | 7.2 MiB | 0 |
| file pruning + stage cache | 13 | 11 | 27 | 0 | 10 | 1 / 1 | 7.2 MiB | 1 |

- pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (13 vs 13 ms) is measurement noise; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): no measurable end-to-end difference (under 10% or 5 ms; p50 13 vs 11 ms); 1 of 1 staged files served from the cache

**o5-full-scan** — unfiltered aggregate: nothing to prune; shows the cache alone

`SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 31 | 90 | 250 | 65 | 24 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 36 | 107 | 494 | 79 | 25 | 3 / 3 | 50.4 MiB | 0 |
| file pruning + stage cache | 28 | 18 | 99 | 1 | 17 | 3 / 3 | 50.4 MiB | 3 |

- pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (90 vs 107 ms) is measurement noise; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 83% faster end to end (p50 18 vs 107 ms); 3 of 3 staged files served from the cache


### Ingestion throughput (CSV -> Parquet)

| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |
|---:|---:|---:|---:|---:|---:|---:|---|
| 300000 | 16.6 | 25.2 | 3 | 0.37 | 806315 | 44.7 | none |
| 300000 | 16.6 | 25.2 | 4 | 0.43 | 696605 | 38.6 | l_returnflag |

### Limitations of this run

- Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.
- Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.
- The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.
- The main latency, concurrency and layout sections use the baseline runner: every query stages (downloads) all files of every dataset it references, with no file pruning and no cache, so 'warm' means a warm OS page cache and warmed code paths, not a data cache. Pruning and the stage cache are measured only in the optimisation section, against that baseline.
- The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.
- 'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.
- Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.
- Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.
