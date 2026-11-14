package bench

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

// IngestResult is the measured ingestion of one CSV upload.
type IngestResult struct {
	Rows            int     `json:"rows"`
	InputBytes      int     `json:"input_bytes"`
	OutputBytes     int64   `json:"output_parquet_bytes"`
	OutputFiles     int     `json:"output_files"`
	Seconds         float64 `json:"seconds"`
	RowsPerSecond   float64 `json:"rows_per_second"`
	InputMiBPerSec  float64 `json:"input_mib_per_second"`
	PartitionColumn string  `json:"partition_column,omitempty"`
}

// lineitemCSV renders n deterministic lineitem rows as CSV.
func lineitemCSV(n int) []byte {
	g := NewGenerator(SizesFor(0.01))
	var b bytes.Buffer
	for i, c := range LineitemCols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c.Name)
	}
	b.WriteByte('\n')
	written := 0
	for from := 0; written < n; from += 1000 {
		rows, _ := g.Lineitems(from, from+1000)
		for _, row := range rows {
			if written == n {
				break
			}
			for i, v := range row {
				if i > 0 {
					b.WriteByte(',')
				}
				switch x := v.(type) {
				case int64:
					b.WriteString(strconv.FormatInt(x, 10))
				case float64:
					b.WriteString(strconv.FormatFloat(x, 'f', -1, 64))
				case string:
					b.WriteString(x)
				}
			}
			b.WriteByte('\n')
			written++
		}
	}
	return b.Bytes()
}

// MeasureIngest uploads n generated rows through the real ingestion runner
// (parse, type inference, Parquet encoding, staged write, verified publish) and
// times it. partitionBy may be empty.
func MeasureIngest(ctx context.Context, n int, partitionBy string) (IngestResult, error) {
	csv := lineitemCSV(n)
	store := catalog.NewMemoryIngestStore()
	objs := objectstore.NewMemory()
	id, _ := catalog.NewUUID()
	key := "uploads/default/bench/" + id + "/input"
	if err := objs.Put(ctx, key, csv); err != nil {
		return IngestResult{}, err
	}
	req := catalog.CreateConvert{JobID: id, Name: "bench", InputRef: key, InputFormat: catalog.InputFormatCSV}
	if partitionBy != "" {
		req.PartitionColumns = []string{partitionBy}
	}
	if _, _, err := store.CreateConvert(ctx, req); err != nil {
		return IngestResult{}, err
	}
	r := ingest.NewRunner(store, objs, "bench", slog.New(slog.NewTextHandler(io.Discard, nil)))
	start := time.Now()
	if ran, err := r.RunOnce(ctx); err != nil || !ran {
		return IngestResult{}, fmt.Errorf("ingest runner: ran=%v err=%v", ran, err)
	}
	elapsed := time.Since(start)
	job, _ := store.GetJob(ctx, id)
	if job.Status != catalog.JobSucceeded {
		return IngestResult{}, fmt.Errorf("ingestion %s: %s %s", job.Status, job.FailureReason, job.ErrorSummary)
	}
	res := IngestResult{Rows: n, InputBytes: len(csv), Seconds: elapsed.Seconds(), PartitionColumn: partitionBy,
		RowsPerSecond: float64(n) / elapsed.Seconds(), InputMiBPerSec: float64(len(csv)) / (1 << 20) / elapsed.Seconds()}
	for _, k := range objs.Keys("lake/") {
		res.OutputFiles++
		b, err := objs.Get(ctx, k, 1<<40)
		if err != nil {
			return res, err
		}
		res.OutputBytes += int64(len(b))
	}
	return res, nil
}
