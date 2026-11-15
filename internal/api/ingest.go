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
	mode       string
}

type inspectResponse struct {
	Format   string           `json:"format"`
	Columns  []inspectColumn  `json:"columns"`
	RowCount int              `json:"rowCount"`
	Sample   []map[string]any `json:"sample"`
	// Note explains a limit on what is shown (for example no sample for a file
	// the reader cannot decode).
	Note string `json:"note,omitempty"`
	// Append is present when the request named a target dataset (?target=).
	Append *appendCheck `json:"append,omitempty"`
}

// appendCheck is the verdict of checking the file against a dataset's schema.
type appendCheck struct {
	Dataset       string   `json:"dataset"`
	Compatible    bool     `json:"compatible"`
	Issues        []string `json:"issues"`
	DatasetRows   *int64   `json:"datasetRows,omitempty"`
	LatestVersion int      `json:"latestVersion,omitempty"`
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
		case ".parquet":
			f = catalog.InputFormatParquet
		}
	}
	switch f {
	case catalog.InputFormatCSV, catalog.InputFormatJSONL, catalog.InputFormatJSON, catalog.InputFormatParquet:
		return f, nil
	case "":
		return "", fmt.Errorf("%w: specify format=csv|jsonl|json|parquet or a filename with a known extension", catalog.ErrInvalidArgument)
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
		switch m := strings.ToLower(strings.TrimSpace(q.Get("mode"))); m {
		case "", catalog.ModeReplace:
			p.mode = catalog.ModeReplace
		case catalog.ModeAppend:
			p.mode = catalog.ModeAppend
		default:
			return p, fmt.Errorf("%w: mode must be replace or append", catalog.ErrInvalidArgument)
		}
		if v := q.Get("partitionColumns"); v != "" {
			if p.format == catalog.InputFormatParquet {
				return p, fmt.Errorf("%w: partitionColumns does not apply to a Parquet import; the file keeps its own layout", catalog.ErrInvalidArgument)
			}
			if p.mode == catalog.ModeAppend {
				return p, fmt.Errorf("%w: partitionColumns cannot be set for an append; it keeps the dataset's partitioning", catalog.ErrInvalidArgument)
			}
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

// incomingOf parses an upload just far enough to know its columns, so it can be
// reported or checked against a dataset. For Parquet that is its footer; for
// CSV/JSON the file is parsed and types are inferred.
type incomingFile struct {
	columns []catalog.IncomingColumn
	table   *ingest.Table // CSV/JSON only
	rows    int
	sample  []map[string]any
	note    string
}

func (h *Handler) parseIncoming(format string, data []byte, withSample bool) (*incomingFile, error) {
	f := &incomingFile{}
	if format == catalog.InputFormatParquet {
		info, err := ingest.ParseParquet(data, h.ingest.Limits)
		if err != nil {
			return nil, err
		}
		f.columns, f.rows = info.Columns, int(info.Rows)
		if withSample {
			f.sample, f.note = ingest.ParquetSample(data, inspectSampleRows)
		}
		return f, nil
	}
	table, err := ingest.Parse(format, data, h.ingest.Limits)
	if err != nil {
		return nil, err
	}
	f.table, f.columns, f.rows = table, ingest.IncomingColumns(table), len(table.Rows)
	if withSample {
		for i := 0; i < len(table.Rows) && i < inspectSampleRows; i++ {
			row := make(map[string]any, len(table.Columns))
			for j, c := range table.Columns {
				row[c.Name] = table.Rows[i][j]
			}
			f.sample = append(f.sample, row)
		}
	}
	return f, nil
}

// checkAppend compares incoming columns with a dataset's latest READY schema.
// missing is true when the dataset does not exist (or has no READY version).
func (h *Handler) checkAppend(ctx context.Context, name string, incoming []catalog.IncomingColumn, widen bool) (check *appendCheck, missing bool, err error) {
	if _, verr := catalog.ValidateDatasetName(name); verr != nil {
		return nil, false, fmt.Errorf("%w: %v", catalog.ErrInvalidArgument, verr)
	}
	v, cols, err := h.ingest.Jobs.ReadySchema(ctx, name)
	if errors.Is(err, catalog.ErrNotFound) {
		return &appendCheck{Dataset: name, Issues: []string{"the dataset does not exist or has no ready version"}}, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	check = &appendCheck{Dataset: name, DatasetRows: v.RowCount, LatestVersion: v.Version, Issues: []string{}}
	if aerr := catalog.CheckAppendable(name, v); aerr != nil {
		check.Issues = append(check.Issues, strings.TrimPrefix(aerr.Error(), catalog.ErrInvalidArgument.Error()+": "))
	}
	check.Issues = append(check.Issues, catalog.CheckAppendSchema(cols, incoming, widen)...)
	check.Compatible = len(check.Issues) == 0
	return check, false, nil
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
	f, err := h.parseIncoming(p.format, data, true)
	if err != nil {
		writeIngestError(w, err)
		return
	}
	resp := inspectResponse{Format: p.format, RowCount: f.rows, Columns: make([]inspectColumn, len(f.columns)), Sample: f.sample, Note: f.note}
	for i, c := range f.columns {
		resp.Columns[i] = inspectColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
	}
	if target := r.URL.Query().Get("target"); target != "" {
		check, _, err := h.checkAppend(r.Context(), target, f.columns, p.format != catalog.InputFormatParquet)
		if err != nil {
			writeMappedError(w, err)
			return
		}
		resp.Append = check
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
	f, err := h.parseIncoming(p.format, data, false)
	if err != nil {
		writeIngestError(w, err)
		return
	}
	if p.mode == catalog.ModeAppend {
		check, missing, err := h.checkAppend(r.Context(), p.name, f.columns, p.format != catalog.InputFormatParquet)
		switch {
		case err != nil:
			writeMappedError(w, err)
			return
		case missing:
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("dataset %q does not exist; an append needs an existing dataset", p.name))
			return
		case !check.Compatible:
			writeError(w, http.StatusUnprocessableEntity, "invalid_input", "cannot append: "+strings.Join(check.Issues, "; "))
			return
		}
	} else if f.table != nil {
		if err := ingest.ValidatePartitionColumns(f.table.Columns, p.partitions); err != nil {
			writeIngestError(w, err)
			return
		}
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
		PartitionColumns: p.partitions, RetainOriginal: p.retain, Mode: p.mode,
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
