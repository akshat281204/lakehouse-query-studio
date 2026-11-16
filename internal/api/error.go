// Package api implements the versioned dataset HTTP handlers.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

type errorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// classify maps domain errors to an HTTP status and a stable error code under
// the docs/design.md §11 contract.
func classify(err error) (int, string) {
	switch {
	case errors.Is(err, catalog.ErrNotFound), errors.Is(err, objectstore.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, catalog.ErrAlreadyExists):
		return http.StatusConflict, "already_exists"
	case errors.Is(err, objectstore.ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "payload_too_large"
	case errors.Is(err, catalog.ErrInvalidArgument):
		return http.StatusBadRequest, "invalid_argument"
	case errors.Is(err, catalog.ErrUnavailable),
		errors.Is(err, objectstore.ErrUnavailable),
		errors.Is(err, objectstore.ErrAccessDenied):
		return http.StatusServiceUnavailable, "unavailable"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

func writeMappedError(w http.ResponseWriter, err error) {
	status, code := classify(err)
	msg := err.Error()
	if len(msg) > 512 {
		msg = msg[:512]
	}
	writeError(w, status, code, msg)
}
