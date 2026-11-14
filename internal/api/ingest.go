package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

// IngestDeps carries the dependencies of the upload/ingestion endpoints.
type IngestDeps struct {
	Jobs    catalog.IngestStore
	Objects objectstore.ReadWriter
	Limits  ingest.Limits
}

// Option customizes the handler built by New.
type Option func(*Handler)

// WithIngest enables the upload and ingestion-job endpoints.
func WithIngest(d IngestDeps) Option {
	return func(h *Handler) {
		if d.Limits.MaxBytes == 0 {
			d.Limits = ingest.DefaultLimits
		}
		h.ingest = d
	}
}

const inspectSampleRows = 5

type uploadParams struct {
	name       string
	format     string
	filename   string
	partitions []string
	retain     bool
}

type inspectResponse struct {
	Format   string           `json:"format"`
	Columns  []inspectColumn  `json:"columns"`
	RowCount int              `json:"rowCount"`
	Sample   []map[string]any `json:"sample"`
}

type inspectColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type jobListResponse struct {
	Jobs []catalog.Job `json:"jobs"`
}

func (h *Handler) registerIngest(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/ingestions/inspect", h.handleInspect)
	mux.HandleFunc("POST /api/v1/ingestions", h.handleUpload)
	mux.HandleFunc("GET /api/v1/ingestions", h.handleListJobs)
	mux.HandleFunc("GET /api/v1/ingestions/{id}", h.handleGetJob)
	mux.HandleFunc("POST /api/v1/ingestions/{id}/retry", h.handleRetryJob)
}

func (h *Handler) requireIngest(w http.ResponseWriter) bool {
	if h.ingest.Jobs == nil || h.ingest.Objects == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "ingestion is not configured")
		return false
	}
	return true
}

// formatFromParams resolves the input format from ?format= or the filename
// extension.
func formatFromParams(format, filename string) (string, error) {
	f := strings.ToLower(strings.TrimSpace(format))
	if f == "" {
		switch strings.ToLower(path.Ext(filename)) {
		case ".csv":
			f = catalog.InputFormatCSV
		case ".jsonl", ".ndjson":
			f = catalog.InputFormatJSONL
		case ".json":
			f = catalog.InputFormatJSON
		}
	}
	switch f {
	case catalog.InputFormatCSV, catalog.InputFormatJSONL, catalog.InputFormatJSON:
		return f, nil
	case "":
		return "", fmt.Errorf("%w: specify format=csv|jsonl|json or a filename with a known extension", catalog.ErrInvalidArgument)
	default:
		return "", fmt.Errorf("%w: unsupported format %q", catalog.ErrInvalidArgument, f)
	}
}

func parseUploadParams(r *http.Request, needName bool) (uploadParams, error) {
	q := r.URL.Query()
	p := uploadParams{filename: q.Get("filename")}
	var err error
	if p.format, err = formatFromParams(q.Get("format"), p.filename); err != nil {
		return p, err
	}
	if needName {
		if p.name, err = catalog.ValidateDatasetName(q.Get("name")); err != nil {
			return p, fmt.Errorf("%w: %v", catalog.ErrInvalidArgument, err)
		}
		if v := q.Get("partitionColumns"); v != "" {
			for _, c := range strings.Split(v, ",") {
				if verr := ingest.ValidateColumnName(c); verr != nil {
					return p, fmt.Errorf("%w: invalid partition column name", catalog.ErrInvalidArgument)
				}
				p.partitions = append(p.partitions, c)
			}
		}
		p.retain, _ = strconv.ParseBool(q.Get("retainOriginal"))
	}
	return p, nil
}

// readBody reads the upload body up to the limit, mapping overflow to 413.
func (h *Handler) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, h.ingest.Limits.MaxBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("file exceeds the %d MiB upload limit", h.ingest.Limits.MaxBytes>>20))
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "invalid_argument", "could not read the request body")
		return nil, false
	}
	if len(data) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_argument", "the file is empty")
		return nil, false
	}
	return data, true
}

func writeIngestError(w http.ResponseWriter, err error) {
	if ingest.IsUserError(err) {
		writeError(w, http.StatusUnprocessableEntity, "invalid_input", err.Error())
		return
	}
	writeMappedError(w, err)
}

func (h *Handler) handleInspect(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngest(w) {
		return
	}
	p, err := parseUploadParams(r, false)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	data, ok := h.readBody(w, r)
	if !ok {
		return
	}
	table, err := ingest.Parse(p.format, data, h.ingest.Limits)
	if err != nil {
		writeIngestError(w, err)
		return
	}
	resp := inspectResponse{Format: p.format, RowCount: len(table.Rows), Columns: make([]inspectColumn, len(table.Columns))}
	for i, c := range table.Columns {
		resp.Columns[i] = inspectColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
	}
	for i := 0; i < len(table.Rows) && i < inspectSampleRows; i++ {
		row := make(map[string]any, len(table.Columns))
		for j, c := range table.Columns {
			row[c.Name] = table.Rows[i][j]
		}
		resp.Sample = append(resp.Sample, row)
	}
	writeJSON(w, http.StatusOK, resp)
}

func sanitizeFilename(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 64 {
			break
		}
	}
	out := strings.Trim(b.String(), ".")
	if out == "" {
		return "input"
	}
	return out
}

func (h *Handler) handleUpload(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngest(w) {
		return
	}
	p, err := parseUploadParams(r, true)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	data, ok := h.readBody(w, r)
	if !ok {
		return
	}
	// Fail fast on structurally invalid input so the caller gets an immediate,
	// actionable error rather than a queued job that is certain to fail.
	table, err := ingest.Parse(p.format, data, h.ingest.Limits)
	if err != nil {
		writeIngestError(w, err)
		return
	}
	if err := ingest.ValidatePartitionColumns(table.Columns, p.partitions); err != nil {
		writeIngestError(w, err)
		return
	}

	jobID, err := catalog.NewUUID()
	if err != nil {
		writeMappedError(w, err)
		return
	}
	key := fmt.Sprintf("uploads/%s/%s/%s/%s", catalog.NamespaceDefault, p.name, jobID, sanitizeFilename(p.filename))
	if err := h.ingest.Objects.Put(r.Context(), key, data); err != nil {
		writeMappedError(w, err)
		return
	}
	job, _, err := h.ingest.Jobs.CreateConvert(r.Context(), catalog.CreateConvert{
		JobID: jobID, Name: p.name, InputRef: key, InputFormat: p.format,
		PartitionColumns: p.partitions, RetainOriginal: p.retain,
	})
	if err != nil {
		// The job was never created: release the orphaned upload.
		_ = h.ingest.Objects.Delete(context.WithoutCancel(r.Context()), key)
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (h *Handler) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngest(w) {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := h.ingest.Jobs.ListJobs(r.Context(), limit)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if jobs == nil {
		jobs = []catalog.Job{}
	}
	writeJSON(w, http.StatusOK, jobListResponse{Jobs: jobs})
}

func (h *Handler) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngest(w) {
		return
	}
	job, err := h.ingest.Jobs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngest(w) {
		return
	}
	newID, err := catalog.NewUUID()
	if err != nil {
		writeMappedError(w, err)
		return
	}
	job, err := h.ingest.Jobs.Retry(r.Context(), r.PathValue("id"), newID)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}
