package api

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// maxProfileFiles bounds the footers read for one profile request. A dataset
// with more files is profiled from its schema only.
const (
	maxProfileFiles   = 200
	profileParallel   = 8
	maxProfileTextLen = 80
)

type profileColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Nullable    bool   `json:"nullable"`
	IsPartition bool   `json:"isPartition"`
	// HasRange is true only when every file of the dataset supplied min/max for
	// the column; Min and Max are then exact text renderings of the extremes.
	HasRange  bool    `json:"hasRange"`
	Min       *string `json:"min,omitempty"`
	Max       *string `json:"max,omitempty"`
	NullCount *int64  `json:"nullCount,omitempty"`
	// Truncated is true when Min or Max was shortened for display.
	Truncated bool `json:"truncated,omitempty"`
}

type profileResponse struct {
	Rows    int64           `json:"rows"`
	Files   int             `json:"files"`
	Columns []profileColumn `json:"columns"`
	// Note explains any limit that reduced what is reported.
	Note string `json:"note,omitempty"`
}

// handleProfile summarises a dataset from Parquet footer statistics: row count
// and, per column, min, max and null count. It reads only footers (never row
// data), so it is cheap at any size. Ranges are reported only when every file
// supplies them, so a range is never silently partial.
func (h *Handler) handleProfile(w http.ResponseWriter, r *http.Request) {
	if !h.requireCatalog(w) {
		return
	}
	if h.objects == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "object store not configured")
		return
	}
	id := r.PathValue("id")
	if _, err := h.store.Get(r.Context(), id); err != nil {
		writeMappedError(w, err)
		return
	}
	v, err := h.store.GetLatestReadyVersion(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	cols, err := h.store.GetSchema(r.Context(), v.ID)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	objs, err := catalog.ListParquetObjects(r.Context(), h.objects, v.ObjectPrefix)
	if err != nil {
		writeMappedError(w, err)
		return
	}

	resp := profileResponse{Files: len(objs), Columns: make([]profileColumn, 0, len(cols))}
	for _, c := range cols {
		resp.Columns = append(resp.Columns, profileColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, IsPartition: c.IsPartition})
	}
	if len(objs) == 0 {
		resp.Note = "no Parquet files under the dataset prefix"
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if len(objs) > maxProfileFiles {
		resp.Note = "dataset has more than " + strconv.Itoa(maxProfileFiles) + " files; showing the schema only"
		writeJSON(w, http.StatusOK, resp)
		return
	}

	stats, failed := readAllStats(r.Context(), h.objects, objs)
	if r.Context().Err() != nil {
		return
	}
	if failed > 0 {
		resp.Note = "statistics could not be read from " + strconv.Itoa(failed) + " of " + strconv.Itoa(len(objs)) + " files; showing the schema only"
		writeJSON(w, http.StatusOK, resp)
		return
	}
	merged := parquet.MergeStats(stats)
	resp.Rows = merged.NumRows
	missing := 0
	for i := range resp.Columns {
		cs, ok := merged.Columns[resp.Columns[i].Name]
		if !ok {
			continue
		}
		if cs.HasNullCount {
			n := cs.NullCount
			resp.Columns[i].NullCount = &n
		}
		if cs.HasMinMax {
			lo, hi, cut := renderRange(cs)
			resp.Columns[i].HasRange, resp.Columns[i].Min, resp.Columns[i].Max, resp.Columns[i].Truncated = true, &lo, &hi, cut
		} else {
			missing++
		}
	}
	if missing > 0 {
		resp.Note = strconv.Itoa(missing) + " column(s) have no range: their type is not summarised, or the files carry no statistics for them (files written before statistics were added have none; uploading the data again adds them)"
	}
	writeJSON(w, http.StatusOK, resp)
}

// readAllStats reads every file's footer statistics with bounded parallelism.
// failed counts files whose footer could not be read or parsed.
func readAllStats(ctx context.Context, store parquet.ObjectReader, objs []objectstore.Object) ([]*parquet.FileStats, int) {
	out := make([]*parquet.FileStats, len(objs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, profileParallel)
	for i, o := range objs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, o objectstore.Object) {
			defer wg.Done()
			defer func() { <-sem }()
			if st, err := parquet.ReadStats(ctx, store, o.Key, o.Size); err == nil {
				out[i] = st
			}
		}(i, o)
	}
	wg.Wait()
	failed := 0
	for _, s := range out {
		if s == nil {
			failed++
		}
	}
	return out, failed
}

func renderRange(cs parquet.ColumnStats) (lo, hi string, truncated bool) {
	switch cs.Type {
	case "int32", "int64":
		return strconv.FormatInt(cs.MinInt, 10), strconv.FormatInt(cs.MaxInt, 10), false
	case "double":
		return strconv.FormatFloat(cs.MinFloat, 'g', -1, 64), strconv.FormatFloat(cs.MaxFloat, 'g', -1, 64), false
	case "boolean":
		return strconv.FormatBool(cs.MinBool), strconv.FormatBool(cs.MaxBool), false
	}
	lo, t1 := clipText(cs.MinStr)
	hi, t2 := clipText(cs.MaxStr)
	return lo, hi, t1 || t2
}

func clipText(s string) (string, bool) {
	if utf8.RuneCountInString(s) <= maxProfileTextLen {
		return s, false
	}
	r := []rune(s)
	return string(r[:maxProfileTextLen]), true
}
