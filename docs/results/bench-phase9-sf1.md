### Environment

| | |
|---|---|
| Generated | 2026-11-15T10:49:41Z |
| OS / arch | darwin / arm64 |
| CPU | Apple M4 (10 logical) |
| Memory | 16.0 GiB |
| Go | go1.26.3 |
| DuckDB | v1.4.1 |
| Commit | `1071148a0f2ad9f25a6649a82a32939a7ae8c103` (dirty: true) |
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
| q1-selective-filter | 403 | 354 | 2874 | 2874 | 2874 | 275 | 70 | 0 | 1 | 503.6 |
| q2-year-flat | 1438 | 875 | 3183 | 3183 | 3183 | 793 | 53 | 0 | 3 | 503.6 |
| q2p-year-partitioned | 2497 | 875 | 1990 | 1990 | 1990 | 687 | 69 | 0 | 3 | 458.0 |
| q3-full-aggregate | 2726 | 661 | 3181 | 3181 | 3181 | 475 | 160 | 0 | 6 | 503.6 |
| q3p-full-partitioned | 1045 | 727 | 4048 | 4048 | 4048 | 344 | 277 | 0 | 6 | 458.0 |
| q4-join | 101 | 39 | 97 | 97 | 97 | 16 | 21 | 0 | 5 | 77.4 |
| q5-three-way-join | 2254 | 1296 | 7352 | 7352 | 7352 | 1139 | 147 | 0 | 5 | 581.0 |
| q6-point-lookup | 35 | 121 | 629 | 629 | 629 | 102 | 15 | 0 | 1 | 72.1 |
| q7-large-result | 3044 | 929 | 3088 | 3088 | 3088 | 790 | 120 | 1 | 10000 (trunc.) | 503.6 |

### Throughput under concurrency

| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16 | 9.76 | 1.64 | 7733 | 9763 | 9763 | 0 |
| 2 | 16 | 4.17 | 3.84 | 2342 | 4170 | 4170 | 0 |
| 4 | 16 | 18.21 | 0.88 | 16785 | 18213 | 18213 | 0 |

### Partitioned vs unpartitioned

| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |
|---|---:|---:|---:|---:|---|---|---|
| filter on partition column (l_year = 1995) | 875 | 875 | 53 | 69 | 528069088 / 480267500 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | partitioned execute 31% slower (p50 69 vs 53 ms); INCONCLUSIVE: high variance |
| full scan, no pruning possible (control) | 661 | 727 | 160 | 277 | 528069088 / 480267500 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | partitioned execute 73% slower (p50 277 vs 160 ms); INCONCLUSIVE: high variance |

### File pruning and stage cache

Same dataset, same queries, three runner configurations measured interleaved. The answer of every variant was checked identical to the baseline's. 'First run' is the first execution of that variant (footers unread, cache empty: the one-time cost); p50/p95 are warm repeats, where 'cache' numbers are the repeated-identical-access best case (every file already cached).

**o1-partition-column** — filter on the hive partition column (l_year = 1995), partitioned layout

`SELECT l_returnflag, count(*) AS n FROM lineitem_part WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 155 | 180 | 347 | 113 | 42 | 210 / 210 | 458.0 MiB | 0 |
| file pruning | 32 | 43 | 99 | 15 | 18 | 30 / 210 | 65.4 MiB | 0 |
| file pruning + stage cache | 43 | 29 | 37 | 7 | 20 | 30 / 210 | 65.4 MiB | 30 |

- file pruning: 76% faster end to end (p50 43 vs 180 ms); staged 30 of 210 files, 392.7 MiB less (86% of the baseline bytes); INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 33% faster end to end (p50 29 vs 43 ms); 30 of 30 staged files served from the cache

**o2-clustered-range** — range on the column the flat files are clustered by (l_orderkey), flat layout

`SELECT count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem WHERE l_orderkey BETWEEN 1 AND 20000`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 485 | 759 | 3857 | 700 | 30 | 30 / 30 | 503.6 MiB | 0 |
| file pruning | 21 | 17 | 191 | 4 | 11 | 1 / 30 | 16.8 MiB | 0 |
| file pruning + stage cache | 21 | 11 | 15 | 0 | 10 | 1 / 30 | 16.8 MiB | 1 |

- file pruning: 98% faster end to end (p50 17 vs 759 ms); staged 1 of 30 files, 486.8 MiB less (97% of the baseline bytes); INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 32% faster end to end (p50 11 vs 17 ms); 1 of 1 staged files served from the cache

**o3-spread-column** — filter on a column present in every file (l_year), flat layout: cannot prune

`SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 585 | 858 | 4656 | 785 | 60 | 30 / 30 | 503.6 MiB | 0 |
| file pruning | 1067 | 1039 | 5134 | 864 | 60 | 30 / 30 | 503.6 MiB | 0 |
| file pruning + stage cache | 685 | 58 | 147 | 7 | 49 | 30 / 30 | 503.6 MiB | 30 |

- file pruning: 21% slower end to end (p50 1039 vs 858 ms); footers were checked but no files could be ruled out, so none were skipped; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 94% faster end to end (p50 58 vs 1039 ms); 30 of 30 staged files served from the cache

**o4-point-lookup** — single-order lookup on the clustered key, orders table

`SELECT o_orderkey, o_custkey, o_totalprice FROM orders WHERE o_orderkey = 17`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 46 | 92 | 731 | 75 | 15 | 8 / 8 | 72.1 MiB | 0 |
| file pruning | 13 | 14 | 80 | 2 | 10 | 1 / 8 | 9.6 MiB | 0 |
| file pruning + stage cache | 14 | 10 | 38 | 0 | 9 | 1 / 8 | 9.6 MiB | 1 |

- file pruning: 85% faster end to end (p50 14 vs 92 ms); staged 1 of 8 files, 62.5 MiB less (87% of the baseline bytes); INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): no measurable end-to-end difference (under 10% or 5 ms; p50 14 vs 10 ms); 1 of 1 staged files served from the cache

**o5-full-scan** — unfiltered aggregate: nothing to prune; shows the cache alone

`SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 3045 | 933 | 5255 | 795 | 90 | 30 / 30 | 503.6 MiB | 0 |
| file pruning | 601 | 986 | 7262 | 729 | 97 | 30 / 30 | 503.6 MiB | 0 |
| file pruning + stage cache | 295 | 76 | 86 | 7 | 66 | 30 / 30 | 503.6 MiB | 30 |

- pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (933 vs 986 ms) is measurement noise; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 92% faster end to end (p50 76 vs 986 ms); 30 of 30 staged files served from the cache


### Ingestion throughput (CSV -> Parquet)

| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |
|---:|---:|---:|---:|---:|---:|---:|---|
| 300000 | 16.6 | 25.2 | 3 | 0.40 | 755338 | 41.8 | none |
| 300000 | 16.6 | 25.2 | 4 | 0.42 | 712365 | 39.5 | l_returnflag |

### Limitations of this run

- Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.
- Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.
- The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.
- The main latency, concurrency and layout sections use the baseline runner: every query stages (downloads) all files of every dataset it references, with no file pruning and no cache, so 'warm' means a warm OS page cache and warmed code paths, not a data cache. Pruning and the stage cache are measured only in the optimisation section, against that baseline.
- The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.
- 'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.
- Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.
- Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.
