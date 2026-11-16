### Environment

| | |
|---|---|
| Generated | 2026-11-15T10:48:23Z |
| OS / arch | darwin / arm64 |
| CPU | Apple M4 (10 logical) |
| Memory | 16.0 GiB |
| Go | go1.26.3 |
| DuckDB | v1.4.1 |
| Commit | `1071148a0f2ad9f25a6649a82a32939a7ae8c103` (dirty: true) |
| Object store | S3-compatible object store from MINIO_* env (real HTTP requests, local loopback) |
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
| q1-selective-filter | 281 | 228 | 324 | 324 | 324 | 213 | 12 | 1 | 1 | 50.4 |
| q2-year-flat | 221 | 233 | 292 | 292 | 292 | 215 | 13 | 1 | 3 | 50.4 |
| q2p-year-partitioned | 232 | 234 | 269 | 269 | 269 | 214 | 15 | 1 | 3 | 45.8 |
| q3-full-aggregate | 231 | 263 | 566 | 566 | 566 | 240 | 20 | 2 | 6 | 50.4 |
| q3p-full-partitioned | 237 | 240 | 379 | 379 | 379 | 214 | 21 | 2 | 6 | 45.8 |
| q4-join | 53 | 50 | 54 | 54 | 54 | 35 | 13 | 1 | 5 | 7.7 |
| q5-three-way-join | 287 | 306 | 415 | 415 | 415 | 280 | 20 | 1 | 5 | 58.1 |
| q6-point-lookup | 47 | 49 | 58 | 58 | 58 | 35 | 11 | 1 | 1 | 7.2 |
| q7-large-result | 345 | 264 | 323 | 323 | 323 | 238 | 17 | 6 | 10000 (trunc.) | 50.4 |

### Throughput under concurrency

| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12 | 2.65 | 4.52 | 1341 | 2651 | 2651 | 0 |
| 2 | 12 | 2.50 | 4.80 | 1382 | 2498 | 2498 | 0 |

### Partitioned vs unpartitioned

| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |
|---|---:|---:|---:|---:|---|---|---|
| filter on partition column (l_year = 1995) | 233 | 234 | 13 | 15 | 52828240 / 48046142 | no measurable end-to-end difference (under 10% or 5 ms) | no measurable execute-time difference |
| full scan, no pruning possible (control) | 263 | 240 | 20 | 21 | 52828240 / 48046142 | no measurable end-to-end difference (under 10% or 5 ms); INCONCLUSIVE: high variance (p95 > 2x p50) | no measurable execute-time difference |

### File pruning and stage cache

Same dataset, same queries, three runner configurations measured interleaved. The answer of every variant was checked identical to the baseline's. 'First run' is the first execution of that variant (footers unread, cache empty: the one-time cost); p50/p95 are warm repeats, where 'cache' numbers are the repeated-identical-access best case (every file already cached).

**o1-partition-column** — filter on the hive partition column (l_year = 1995), partitioned layout

`SELECT l_returnflag, count(*) AS n FROM lineitem_part WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 398 | 266 | 376 | 247 | 16 | 21 / 21 | 45.8 MiB | 0 |
| file pruning | 66 | 47 | 70 | 33 | 13 | 3 / 21 | 6.5 MiB | 0 |
| file pruning + stage cache | 68 | 16 | 23 | 2 | 12 | 3 / 21 | 6.5 MiB | 3 |

- file pruning: 82% faster end to end (p50 47 vs 266 ms); staged 3 of 21 files, 39.3 MiB less (86% of the baseline bytes)
- stage cache (on top of pruning): 66% faster end to end (p50 16 vs 47 ms); 3 of 3 staged files served from the cache

**o2-clustered-range** — range on the column the flat files are clustered by (l_orderkey), flat layout

`SELECT count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem WHERE l_orderkey BETWEEN 1 AND 20000`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 281 | 247 | 391 | 231 | 12 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 94 | 91 | 108 | 76 | 12 | 1 / 3 | 16.8 MiB | 0 |
| file pruning + stage cache | 95 | 13 | 15 | 1 | 11 | 1 / 3 | 16.8 MiB | 1 |

- file pruning: 63% faster end to end (p50 91 vs 247 ms); staged 1 of 3 files, 33.6 MiB less (67% of the baseline bytes)
- stage cache (on top of pruning): 86% faster end to end (p50 13 vs 91 ms); 1 of 1 staged files served from the cache

**o3-spread-column** — filter on a column present in every file (l_year), flat layout: cannot prune

`SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 230 | 232 | 338 | 215 | 13 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 303 | 241 | 325 | 223 | 13 | 3 / 3 | 50.4 MiB | 0 |
| file pruning + stage cache | 248 | 15 | 18 | 1 | 12 | 3 / 3 | 50.4 MiB | 3 |

- file pruning: no measurable end-to-end difference (under 10% or 5 ms; p50 232 vs 241 ms); footers were checked but no files could be ruled out, so none were skipped
- stage cache (on top of pruning): 94% faster end to end (p50 15 vs 241 ms); 3 of 3 staged files served from the cache

**o4-point-lookup** — single-order lookup on the clustered key, orders table

`SELECT o_orderkey, o_custkey, o_totalprice FROM orders WHERE o_orderkey = 17`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 46 | 45 | 57 | 33 | 11 | 1 / 1 | 7.2 MiB | 0 |
| file pruning | 46 | 46 | 110 | 33 | 11 | 1 / 1 | 7.2 MiB | 0 |
| file pruning + stage cache | 52 | 13 | 14 | 1 | 10 | 1 / 1 | 7.2 MiB | 1 |

- pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (45 vs 46 ms) is measurement noise; INCONCLUSIVE: high variance (p95 > 2x p50)
- stage cache (on top of pruning): 72% faster end to end (p50 13 vs 46 ms); 1 of 1 staged files served from the cache

**o5-full-scan** — unfiltered aggregate: nothing to prune; shows the cache alone

`SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`

| Variant | first run ms | p50 ms | p95 ms | stage p50 | execute p50 | files staged / listed | bytes staged | cache hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (no pruning, no cache) | 234 | 265 | 337 | 243 | 18 | 3 / 3 | 50.4 MiB | 0 |
| file pruning | 232 | 258 | 290 | 237 | 18 | 3 / 3 | 50.4 MiB | 0 |
| file pruning + stage cache | 282 | 20 | 25 | 1 | 17 | 3 / 3 | 50.4 MiB | 3 |

- pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (265 vs 258 ms) is measurement noise
- stage cache (on top of pruning): 92% faster end to end (p50 20 vs 258 ms); 3 of 3 staged files served from the cache


### Ingestion throughput (CSV -> Parquet)

| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |
|---:|---:|---:|---:|---:|---:|---:|---|
| 100000 | 5.5 | 8.4 | 1 | 0.14 | 724330 | 39.9 | none |
| 100000 | 5.5 | 8.4 | 3 | 0.14 | 698595 | 38.5 | l_returnflag |

### Limitations of this run

- Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.
- Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.
- The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.
- The main latency, concurrency and layout sections use the baseline runner: every query stages (downloads) all files of every dataset it references, with no file pruning and no cache, so 'warm' means a warm OS page cache and warmed code paths, not a data cache. Pruning and the stage cache are measured only in the optimisation section, against that baseline.
- The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.
- 'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.
- Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.
- Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.
