package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

// QueryLimits are the API-side bounds of design §9 and §12.
type QueryLimits struct {
	MaxPageBytes   int64
	MaxExportBytes int64
	MaxExportRows  int
	MaxQueued      int
}

// DefaultQueryLimits returns the design defaults.
func DefaultQueryLimits() QueryLimits {
	return QueryLimits{MaxPageBytes: 4 << 20, MaxExportBytes: 64 << 20, MaxExportRows: 50_000, MaxQueued: 100}
}

// QueryDeps carries the dependencies of the query endpoints. Objects is the
// read-only store: the API is the sole reader of result spools and never
// writes to object storage on this path.
type QueryDeps struct {
	Store     catalog.QueryStore
	Validator *query.Validator
	Objects   objectstore.Store
	Limits    QueryLimits
	// Metrics is optional.
	Metrics *telemetry.Metrics
}

// WithQueries enables the query endpoints.
func WithQueries(d QueryDeps) Option {
	return func(h *Handler) {
		if d.Limits.MaxPageBytes == 0 {
			d.Limits = DefaultQueryLimits()
		}
		h.queries = d
	}
}

const (
	maxQueryBody     = 2 * query.MaxSQLBytes
	defaultPageRows  = 1000
	maxPageRows      = 10_000
	defaultHistory   = 25
	maxHistory       = 100
	maxResultRowsAPI = 100_000
)

func (h *Handler) registerQueries(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/queries", h.handleSubmitQuery)
	mux.HandleFunc("GET /api/v1/queries", h.handleListQueries)
	mux.HandleFunc("GET /api/v1/queries/{id}", h.handleGetQuery)
	mux.HandleFunc("POST /api/v1/queries/{id}/cancel", h.handleCancelQuery)
	mux.HandleFunc("GET /api/v1/queries/{id}/results", h.handleQueryResults)
	mux.HandleFunc("GET /api/v1/queries/{id}/export", h.handleQueryExport)
}

func (h *Handler) requireQueries(w http.ResponseWriter) bool {
	if h.queries.Store == nil || h.queries.Validator == nil || h.queries.Objects == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "queries are not configured")
		return false
	}
	return true
}

type submitRequest struct {
	Statement string `json:"statement"`
}

type queryListResponse struct {
	Queries []catalog.QueryJob `json:"queries"`
	Offset  int                `json:"offset"`
	Limit   int                `json:"limit"`
}

type resultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type resultsResponse struct {
	Columns    []resultColumn `json:"columns"`
	Rows       [][]any        `json:"rows"`
	Offset     int            `json:"offset"`
	Returned   int            `json:"returned"`
	NextOffset *int           `json:"nextOffset"`
	TotalRows  int64          `json:"totalRows"`
	Truncated  bool           `json:"truncated"`
}

func (h *Handler) handleSubmitQuery(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req submitRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusBadRequest, "query_denied", fmt.Sprintf("statement exceeds %d bytes", query.MaxSQLBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_argument", "request body must be JSON with a statement field")
		return
	}

	v, err := h.queries.Validator.Validate(r.Context(), req.Statement)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	bindings := make(map[string]string, len(v.Tables))
	for _, t := range v.Tables {
		rel, err := h.queries.Store.ResolveRelation(r.Context(), t.Name)
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(w, http.StatusBadRequest, "query_denied", "a referenced relation does not exist or has no READY version")
			return
		}
		if err != nil {
			writeMappedError(w, err)
			return
		}
		bindings[t.Name] = rel.VersionID
	}
	id, err := catalog.NewUUID()
	if err != nil {
		writeMappedError(w, err)
		return
	}
	job, err := h.queries.Store.Submit(r.Context(), catalog.SubmitQuery{
		ID: id, Statement: req.Statement, StatementHash: query.StatementHash(req.Statement),
		Bindings: bindings, MaxQueued: h.queries.Limits.MaxQueued,
	})
	if err != nil {
		writeQueryError(w, err)
		return
	}
	w.Header().Set("Location", "/api/v1/queries/"+job.ID)
	writeJSON(w, http.StatusAccepted, job)
}

func (h *Handler) handleListQueries(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	offset, ok := intParam(w, r, "offset", 0, 0, 1_000_000)
	if !ok {
		return
	}
	limit, ok := intParam(w, r, "limit", defaultHistory, 1, maxHistory)
	if !ok {
		return
	}
	jobs, err := h.queries.Store.ListQueries(r.Context(), offset, limit)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queryListResponse{Queries: jobs, Offset: offset, Limit: limit})
}

func (h *Handler) handleGetQuery(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	job, err := h.queries.Store.GetQuery(r.Context(), r.PathValue("id"))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) handleCancelQuery(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	job, err := h.queries.Store.RequestCancel(r.Context(), r.PathValue("id"))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if h.queries.Metrics != nil {
		h.queries.Metrics.QueryCancels.Inc()
	}
	writeJSON(w, http.StatusAccepted, job)
}

func intParam(w http.ResponseWriter, r *http.Request, name string, def, min, max int) (int, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		writeError(w, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("%s must be an integer between %d and %d", name, min, max))
		return 0, false
	}
	return n, true
}

// writeQueryError maps query-domain errors to the §11 contract.
func writeQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, query.ErrDenied):
		msg := strings.TrimPrefix(err.Error(), query.ErrDenied.Error()+": ")
		writeError(w, http.StatusBadRequest, "query_denied", msg)
	case errors.Is(err, catalog.ErrInvalidState):
		writeError(w, http.StatusConflict, "invalid_state", strings.TrimPrefix(err.Error(), catalog.ErrInvalidState.Error()+": "))
	case errors.Is(err, catalog.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "resource_exhausted", "the query queue is full; retry later")
	default:
		writeMappedError(w, err)
	}
}

// loadSucceeded returns a query whose spool is readable, or writes the error.
func (h *Handler) loadSucceeded(w http.ResponseWriter, r *http.Request) (catalog.QueryJob, bool) {
	job, err := h.queries.Store.GetQuery(r.Context(), r.PathValue("id"))
	if err != nil {
		writeQueryError(w, err)
		return job, false
	}
	switch job.Status {
	case catalog.QuerySucceeded:
	case catalog.QueryQueued, catalog.QueryRunning:
		writeError(w, http.StatusConflict, "invalid_state", "the query has not finished")
		return job, false
	default:
		writeError(w, http.StatusConflict, "invalid_state", "the query did not succeed, so it has no results")
		return job, false
	}
	if job.ResultRef == "" || job.ResultBytes == nil {
		writeError(w, http.StatusGone, "result_expired", "the query result is no longer available")
		return job, false
	}
	return job, true
}

func (h *Handler) readRows(ctx context.Context, job catalog.QueryJob, offset, limit int) (*parquet.RowsResult, error) {
	res, err := parquet.ReadRows(ctx, h.queries.Objects, job.ResultRef, *job.ResultBytes, offset, limit)
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil, errResultExpired
	}
	return res, err
}

var errResultExpired = errors.New("result expired")

func (h *Handler) writeReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, errResultExpired) {
		writeError(w, http.StatusGone, "result_expired", "the query result is no longer available")
		return
	}
	if errors.Is(err, parquet.ErrMalformed) {
		writeError(w, http.StatusInternalServerError, "internal", "the stored result could not be read")
		return
	}
	writeMappedError(w, err)
}

func rowValues(cols []parquet.Column, row map[string]any) []any {
	out := make([]any, len(cols))
	for i, c := range cols {
		out[i] = row[c.Name]
	}
	return out
}

func (h *Handler) handleQueryResults(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	offset, ok := intParam(w, r, "offset", 0, 0, maxResultRowsAPI)
	if !ok {
		return
	}
	limit, ok := intParam(w, r, "limit", defaultPageRows, 1, maxPageRows)
	if !ok {
		return
	}
	job, ok := h.loadSucceeded(w, r)
	if !ok {
		return
	}
	res, err := h.readRows(r.Context(), job, offset, limit)
	if err != nil {
		h.writeReadError(w, err)
		return
	}

	// Emit whole rows until the row limit or the page byte budget is reached.
	// Each row is measured in its own JSON encoding.
	out := resultsResponse{Offset: offset, Rows: [][]any{}, TotalRows: res.NumRows, Truncated: job.Truncated}
	for _, c := range res.Columns {
		out.Columns = append(out.Columns, resultColumn{Name: c.Name, Type: c.Type})
	}
	var used int64
	for _, row := range res.Rows {
		vals := rowValues(res.Columns, row)
		b, err := json.Marshal(vals)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "a result value could not be encoded")
			return
		}
		size := int64(len(b)) + 1
		if used+size > h.queries.Limits.MaxPageBytes {
			if len(out.Rows) == 0 {
				writeError(w, http.StatusTooManyRequests, "resource_exhausted", "a single row exceeds the page size limit")
				return
			}
			break
		}
		used += size
		out.Rows = append(out.Rows, vals)
	}
	out.Returned = len(out.Rows)
	if next := offset + out.Returned; int64(next) < res.NumRows {
		out.NextOffset = &next
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, out)
}

// csvCell renders one value. Strings that a spreadsheet would execute as a
// formula are prefixed with a single quote (CSV injection defence).
func csvCell(typ string, v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		if typ == parquet.TypeString && x != "" && strings.ContainsRune("=+-@\t\r", rune(x[0])) {
			return "'" + x
		}
		return x
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	return fmt.Sprint(v)
}

func (h *Handler) handleQueryExport(w http.ResponseWriter, r *http.Request) {
	if !h.requireQueries(w) {
		return
	}
	job, ok := h.loadSucceeded(w, r)
	if !ok {
		return
	}
	lim := h.queries.Limits
	rowCap := lim.MaxExportRows
	if job.RowsReturned != nil && int64(rowCap) > *job.RowsReturned {
		rowCap = int(*job.RowsReturned)
	}
	res, err := h.readRows(r.Context(), job, 0, rowCap)
	if err != nil {
		h.writeReadError(w, err)
		return
	}

	// Render to a buffer first so the byte budget is exact, every emitted row
	// is complete, and truncation is known before the headers go out.
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	head := make([]string, len(res.Columns))
	for i, c := range res.Columns {
		head[i] = csvCell(parquet.TypeString, c.Name)
	}
	_ = cw.Write(head)
	cw.Flush()
	if int64(buf.Len()) > lim.MaxExportBytes {
		writeError(w, http.StatusTooManyRequests, "resource_exhausted", "the header row exceeds the export size limit")
		return
	}
	truncated := job.Truncated || int64(len(res.Rows)) < res.NumRows
	exported := 0
	var line bytes.Buffer
	lw := csv.NewWriter(&line)
	for _, row := range res.Rows {
		line.Reset()
		cells := make([]string, len(res.Columns))
		for i, c := range res.Columns {
			cells[i] = csvCell(c.Type, row[c.Name])
		}
		if err := lw.Write(cells); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "a result value could not be encoded")
			return
		}
		lw.Flush()
		if int64(buf.Len()+line.Len()) > lim.MaxExportBytes {
			if exported == 0 {
				writeError(w, http.StatusTooManyRequests, "resource_exhausted", "a single row exceeds the export size limit")
				return
			}
			truncated = true
			break
		}
		buf.Write(line.Bytes())
		exported++
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="query-`+job.ID+`.csv"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Export-Rows", strconv.Itoa(exported))
	w.Header().Set("X-Export-Truncated", strconv.FormatBool(truncated))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, &buf)
}
