package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

// SavedDeps carries the dependencies of the saved-query endpoints.
type SavedDeps struct {
	Store     catalog.SavedQueryStore
	Validator *query.Validator
}

// WithSavedQueries enables the saved-query endpoints.
func WithSavedQueries(d SavedDeps) Option {
	return func(h *Handler) { h.saved = d }
}

func (h *Handler) registerSaved(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/saved-queries", h.handleSavedCreate)
	mux.HandleFunc("GET /api/v1/saved-queries", h.handleSavedList)
	mux.HandleFunc("GET /api/v1/saved-queries/{id}", h.handleSavedGet)
	mux.HandleFunc("PUT /api/v1/saved-queries/{id}", h.handleSavedUpdate)
	mux.HandleFunc("DELETE /api/v1/saved-queries/{id}", h.handleSavedDelete)
}

func (h *Handler) requireSaved(w http.ResponseWriter) bool {
	if h.saved.Store == nil || h.saved.Validator == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "saved queries are not configured")
		return false
	}
	return true
}

type savedRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Statement   string `json:"statement"`
}

type savedListResponse struct {
	SavedQueries []catalog.SavedQuery `json:"savedQueries"`
	Total        int                  `json:"total"`
	Offset       int                  `json:"offset"`
	Limit        int                  `json:"limit"`
}

// decodeSaved reads and validates a save request. The statement must pass the
// same safety boundary as a submitted query, so a saved query can never hold SQL
// the engine would refuse; it is checked again whenever it is run.
func (h *Handler) decodeSaved(w http.ResponseWriter, r *http.Request) (catalog.SavedQueryInput, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req savedRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusBadRequest, "query_denied", fmt.Sprintf("statement exceeds %d bytes", query.MaxSQLBytes))
			return catalog.SavedQueryInput{}, false
		}
		writeError(w, http.StatusBadRequest, "invalid_argument", "request body must be JSON with name, description and statement")
		return catalog.SavedQueryInput{}, false
	}
	in, err := catalog.NormalizeSavedQuery(catalog.SavedQueryInput{Name: req.Name, Description: req.Description, Statement: req.Statement})
	if err != nil {
		writeMappedError(w, err)
		return in, false
	}
	if _, err := h.saved.Validator.Validate(r.Context(), in.Statement); err != nil {
		writeQueryError(w, err)
		return in, false
	}
	return in, true
}

func (h *Handler) handleSavedCreate(w http.ResponseWriter, r *http.Request) {
	if !h.requireSaved(w) {
		return
	}
	in, ok := h.decodeSaved(w, r)
	if !ok {
		return
	}
	q, err := h.saved.Store.Create(r.Context(), in)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	w.Header().Set("Location", "/api/v1/saved-queries/"+q.ID)
	writeJSON(w, http.StatusCreated, q)
}

func (h *Handler) handleSavedList(w http.ResponseWriter, r *http.Request) {
	if !h.requireSaved(w) {
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, total, err := h.saved.Store.List(r.Context(), offset, limit)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if offset < 0 {
		offset = 0
	}
	writeJSON(w, http.StatusOK, savedListResponse{SavedQueries: items, Total: total, Offset: offset, Limit: len(items)})
}

func (h *Handler) handleSavedGet(w http.ResponseWriter, r *http.Request) {
	if !h.requireSaved(w) {
		return
	}
	q, err := h.saved.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (h *Handler) handleSavedUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.requireSaved(w) {
		return
	}
	in, ok := h.decodeSaved(w, r)
	if !ok {
		return
	}
	q, err := h.saved.Store.Update(r.Context(), r.PathValue("id"), in)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (h *Handler) handleSavedDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireSaved(w) {
		return
	}
	if err := h.saved.Store.Delete(r.Context(), r.PathValue("id")); err != nil {
		writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
