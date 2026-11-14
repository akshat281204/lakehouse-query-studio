package catalog

import "errors"

// Sentinel errors mapped by the API to stable HTTP error codes.
var (
	ErrNotFound        = errors.New("catalog: not found")
	ErrAlreadyExists   = errors.New("catalog: already exists")
	ErrInvalidArgument = errors.New("catalog: invalid argument")
	ErrUnavailable     = errors.New("catalog: unavailable")
)
