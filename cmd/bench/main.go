// Command bench measures the query and ingestion path on a deterministic
// TPC-H-like workload and writes a self-describing report (JSON, optionally
// Markdown). It runs the real validator, job store, runner, staging, DuckDB
// engine and spooling in one process. See docs/evaluation.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/bench"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run() error {
	scale := flag.Float64("scale", 0.1, "workload scale (TPC-H-like cardinalities; 1 ~ 6M lineitem rows)")
	iters := flag.Int("iterations", 15, "warm iterations per query")
	conc := flag.String("concurrency", "1,2,4", "comma-separated worker counts for the throughput test")
	concJobs := flag.Int("concurrency-queries", 24, "queries submitted per concurrency level")
	ingestRows := flag.Int("ingest-rows", 300000, "rows in the ingestion benchmark upload (max 1000000)")
	out := flag.String("out", "", "write the JSON report here (default: stdout)")
	md := flag.String("md", "", "also write a Markdown report here")
	threads := flag.Int("threads", 4, "DuckDB threads per query")
	memory := flag.String("memory", "2GiB", "DuckDB memory limit per query")
	scanGiB := flag.Float64("max-scan-gib", 4, "per-query staging limit in GiB (raise for larger scales)")
	useS3 := flag.Bool("s3", false, "use the object store from MINIO_* env instead of memory (not covered by tests)")
	flag.Parse()

	var levels []int
	for _, s := range strings.Split(*conc, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 || n > 16 {
			return fmt.Errorf("invalid -concurrency value %q", s)
		}
		levels = append(levels, n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	lim := query.DefaultLimits()
	lim.Threads, lim.MemoryLimit = *threads, *memory
	lim.MaxScanBytes = int64(*scanGiB * (1 << 30))
	lim.Timeout = 300 * time.Second
	cfg := bench.Config{Limits: lim}
	store := "in-memory object store (no network latency; staging is a memory copy)"
	if *useS3 {
		cfg.Objects = objectstore.New(objectstore.Config{
			Endpoint: os.Getenv("MINIO_ENDPOINT"), AccessKey: os.Getenv("MINIO_ACCESS_KEY"), SecretKey: os.Getenv("MINIO_SECRET_KEY"),
			Bucket: os.Getenv("MINIO_BUCKET"), Region: "us-east-1", UseSSL: os.Getenv("MINIO_USE_SSL") == "true",
		})
		store = "S3-compatible object store from MINIO_* env"
	}

	fmt.Fprintf(os.Stderr, "generating workload (scale %g)...\n", *scale)
	t0 := time.Now()
	h, err := bench.NewHarness(ctx, *scale, cfg)
	if err != nil {
		return err
	}
	defer h.Close()
	fmt.Fprintf(os.Stderr, "generated in %s\n", time.Since(t0).Round(time.Millisecond))

	rep := &bench.Report{
		GeneratedAt: time.Now().UTC(),
		Environment: bench.CaptureEnvironment(bench.DuckDBVersion(ctx)),
		Config: bench.RunConfig{Scale: *scale, Seed: bench.Seed, Iterations: *iters, ConcurrencyLevels: levels,
			ConcurrencyJobs: *concJobs, IngestRows: *ingestRows, ObjectStore: store, Limits: lim,
			LimitsNote: "max rows/result bytes are the product defaults."},
		Dataset: h.Dataset,
		Limitations: []string{
			"Workload is TPC-H-LIKE (seeded, same table shapes and cardinality ratios), not TPC-H: it was not produced by dbgen and must not be reported as TPC-H.",
			"Single machine, single process; the API, worker and benchmark share one CPU and one memory pool.",
			"The object store is in memory unless -s3 is used, so staging cost excludes network latency and S3 throughput.",
			"Every query stages (downloads) all files of every dataset it references before DuckDB runs; there is no cache, so 'warm' means a warm OS page cache and warmed code paths, not a result or data cache.",
			"The OS page cache was not dropped, so 'first' is first-after-generation, not a guaranteed cold read.",
			"'Bytes scanned' is not available from the engine; 'bytes staged' is the bytes downloaded for the query and is an upper bound on what DuckDB read.",
			"Percentiles come from few samples; p95/p99 are at or near the maximum. Median differences under 10% or 5 ms are not reported as differences.",
			"Dates are ISO strings and money is double because of the in-repo Parquet writer's type support.",
		},
	}
	// Flat/partitioned pairs are measured interleaved so run order cannot
	// favour one layout.
	pairSecond := map[string]string{"q2-year-flat": "q2p-year-partitioned", "q3-full-aggregate": "q3p-full-partitioned"}
	isSecond := map[string]bool{"q2p-year-partitioned": true, "q3p-full-partitioned": true}
	all := bench.Queries()
	byID := map[string]bench.Query{}
	for _, q := range all {
		byID[q.ID] = q
	}
	results := map[string]bench.QueryResult{}
	for _, q := range all {
		if isSecond[q.ID] {
			continue
		}
		if second, ok := pairSecond[q.ID]; ok {
			fmt.Fprintf(os.Stderr, "measuring %s and %s (interleaved)...\n", q.ID, second)
			a, b, err := h.MeasurePair(ctx, q, byID[second], *iters)
			if err != nil {
				return err
			}
			results[a.ID], results[b.ID] = a, b
			continue
		}
		fmt.Fprintf(os.Stderr, "measuring %s...\n", q.ID)
		r, err := h.MeasureQuery(ctx, q, *iters)
		if err != nil {
			return err
		}
		results[r.ID] = r
	}
	for _, q := range all {
		rep.Queries = append(rep.Queries, results[q.ID])
	}
	var stmts []string
	for _, q := range bench.Queries() {
		if q.ID != "q7-large-result" {
			stmts = append(stmts, q.SQL)
		}
	}
	for _, w := range levels {
		fmt.Fprintf(os.Stderr, "concurrency %d...\n", w)
		r, err := h.MeasureConcurrency(ctx, stmts, *concJobs, w)
		if err != nil {
			return err
		}
		rep.Concurrency = append(rep.Concurrency, r)
	}
	for _, part := range []string{"", "l_returnflag"} {
		fmt.Fprintf(os.Stderr, "ingesting %d rows (partition %q)...\n", *ingestRows, part)
		r, err := bench.MeasureIngest(ctx, *ingestRows, part)
		if err != nil {
			return err
		}
		rep.Ingest = append(rep.Ingest, r)
	}
	rep.Derive()

	b, _ := json.MarshalIndent(rep, "", "  ")
	if *out == "" {
		fmt.Println(string(b))
	} else if err := os.WriteFile(*out, b, 0o644); err != nil {
		return err
	}
	if *md != "" {
		f, err := os.Create(*md)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		rep.Markdown(f)
	}
	return nil
}
