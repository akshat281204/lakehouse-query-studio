package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

const maxRequestBytes = 1 << 20

// Handler serves the /api/v1/datasets endpoints.
type Handler struct {
	store     catalog.Store
	objects   objectstore.Store
	registrar *catalog.Registrar
	ingest    IngestDeps
}

// New builds the dataset handler. A nil store or object store reports
// unavailable rather than crashing.
func New(store catalog.Store, objects objectstore.Store, opts ...Option) http.Handler {
	h := &Handler{store: store, objects: objects}
	for _, o := range opts {
		o(h)
	}
	if store != nil && objects != nil {
		h.registrar = catalog.NewRegistrar(store, objects)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/datasets", h.handleRegister)
	mux.HandleFunc("GET /api/v1/datasets", h.handleList)
	mux.HandleFunc("GET /api/v1/datasets/{id}", h.handleGet)
	mux.HandleFunc("GET /api/v1/datasets/{id}/schema", h.handleSchema)
	mux.HandleFunc("GET /api/v1/datasets/{id}/preview", h.handlePreview)
	h.registerIngest(mux)
	return mux
}

type registerRequest struct {
	Name             string   `json:"name"`
	Prefix           string   `json:"prefix"`
	Format           string   `json:"format,omitempty"`
	PartitionColumns []string `json:"partitionColumns,omitempty"`
}

type datasetDetail struct {
	catalog.Dataset
	Version catalog.DatasetVersion `json:"version"`
	Columns []catalog.SchemaColumn `json:"schema"`
}

type datasetListItem struct {
	ID            string    `json:"id"`
	Namespace     string    `json:"namespace"`
	Name          string    `json:"name"`
	SourcePrefix  string    `json:"sourcePrefix"`
	Format        string    `json:"format"`
	Status        string    `json:"status"`
	Version       int       `json:"version,omitempty"`
	VersionStatus string    `json:"versionStatus,omitempty"`
	RowCount      *int64    `json:"rowCount,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
}

type datasetListResponse struct {
	Datasets []datasetListItem `json:"datasets"`
	Offset   int               `json:"offset"`
	Limit    int               `json:"limit"`
	Total    int               `json:"total"`
}

type schemaResponse struct {
	Columns []catalog.SchemaColumn `json:"columns"`
}

type previewResponse struct {
	Columns   []catalog.SchemaColumn `json:"columns"`
	Rows      []map[string]any       `json:"rows"`
	TotalRows int64                  `json:"totalRows,omitempty"`
	Truncated bool                   `json:"truncated"`
	Note      string                 `json:"note,omitempty"`
}

func (h *Handler) requireCatalog(w http.ResponseWriter) bool {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "catalog not configured")
		return false
	}
	return true
}

func (h *Handler) handleRegister(w http.ResponseWriter, r *http.Request) {
	if h.registrar == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "catalog not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid JSON body")
		return
	}

	res, err := h.registrar.Register(r.Context(), catalog.RegisterRequest{
		Name:             req.Name,
		Prefix:           req.Prefix,
		Format:           req.Format,
		PartitionColumns: req.PartitionColumns,
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, datasetDetail{
		Dataset: res.Dataset,
		Version: res.Version,
		Columns: res.Columns,
	})
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	if !h.requireCatalog(w) {
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	res, err := h.store.List(r.Context(), catalog.ListOptions{
		Offset: offset,
		Limit:  limit,
		Query:  r.URL.Query().Get("q"),
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}

	items := make([]datasetListItem, 0, len(res.Datasets))
	for _, d := range res.Datasets {
		item := datasetListItem{
			ID:           d.ID,
			Namespace:    d.NamespaceSlug,
			Name:         d.Name,
			SourcePrefix: d.SourcePrefix,
			Format:       d.Format,
			Status:       d.Status,
			CreatedAt:    d.CreatedAt,
		}
		if v, err := h.store.GetLatestVersion(r.Context(), d.ID); err == nil {
			item.Version = v.Version
			item.VersionStatus = v.Status
			item.RowCount = v.RowCount
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, datasetListResponse{
		Datasets: items,
		Offset:   offset,
		Limit:    limit,
		Total:    res.Total,
	})
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	if !h.requireCatalog(w) {
		return
	}
	id := r.PathValue("id")
	d, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	// The detail shows the newest version of any status (so a pending or failed
	// ingestion is visible) but the schema of the newest READY version only.
	v, err := h.store.GetLatestVersion(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	cols := []catalog.SchemaColumn{}
	ready := v
	if v.Status != catalog.VersionReady {
		ready, err = h.store.GetLatestReadyVersion(r.Context(), id)
		if err != nil && !errors.Is(err, catalog.ErrNotFound) {
			writeMappedError(w, err)
			return
		}
	}
	if err == nil {
		cols, err = h.store.GetSchema(r.Context(), ready.ID)
		if err != nil {
			writeMappedError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, datasetDetail{Dataset: d, Version: v, Columns: cols})
}

func (h *Handler) handleSchema(w http.ResponseWriter, r *http.Request) {
	if !h.requireCatalog(w) {
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
	writeJSON(w, http.StatusOK, schemaResponse{Columns: cols})
}

func (h *Handler) handlePreview(w http.ResponseWriter, r *http.Request) {
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

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	objects, err := catalog.ListParquetObjects(r.Context(), h.objects, v.ObjectPrefix)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if len(objects) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "no Parquet objects under version prefix")
		return
	}

	first := objects[0]
	preview, err := parquet.Preview(r.Context(), h.objects, first.Key, first.Size, limit)
	if err != nil {
		writeMappedError(w, err)
		return
	}

	cols, err := h.store.GetSchema(r.Context(), v.ID)
	if err != nil {
		writeMappedError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, previewResponse{
		Columns:   cols,
		Rows:      preview.Rows,
		TotalRows: preview.NumRows,
		Truncated: preview.Truncated,
		Note:      preview.Note,
	})
}
