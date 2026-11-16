package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

const (
	maxListPages = 10
	listPageSize = 1000
)

// RegisterRequest is a registration request.
type RegisterRequest struct {
	Name             string
	Prefix           string
	Format           string
	PartitionColumns []string
}

// RegisterResult is the durable result of a registration.
type RegisterResult struct {
	Dataset Dataset
	Version DatasetVersion
	Columns []SchemaColumn
}

// Registrar orchestrates registration: it validates the request, lists the
// prefix (bounded), inspects a Parquet file's schema, detects partitions, and
// persists a versioned catalog record.
type Registrar struct {
	Store   Store
	Objects objectstore.Store

	inspect inspectFunc
}

type inspectFunc func(ctx context.Context, r parquet.ObjectReader, key string, size int64) (*parquet.Schema, error)

// NewRegistrar builds a Registrar.
func NewRegistrar(store Store, objects objectstore.Store) *Registrar {
	return &Registrar{Store: store, Objects: objects, inspect: parquet.Inspect}
}

// Register validates and persists a registration.
func (r *Registrar) Register(ctx context.Context, req RegisterRequest) (*RegisterResult, error) {
	name, err := ValidateDatasetName(req.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	prefix, err := ValidatePrefix(req.Prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	format := req.Format
	if format == "" {
		format = FormatParquet
	}
	if format != FormatParquet {
		return nil, fmt.Errorf("%w: unsupported format %q", ErrInvalidArgument, format)
	}

	objects, err := ListParquetObjects(ctx, r.Objects, prefix)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, fmt.Errorf("%w: no Parquet objects under prefix", ErrNotFound)
	}

	partitions := req.PartitionColumns
	if len(partitions) == 0 {
		partitions = DetectPartitionColumns(prefix, objectKeys(objects))
	}

	first := objects[0]
	schema, err := r.inspect(ctx, r.Objects, first.Key, first.Size)
	if err != nil {
		return nil, mapInspectError(err)
	}

	cols := make([]SchemaColumn, len(schema.Columns))
	colIndex := make(map[string]int, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = SchemaColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
		colIndex[c.Name] = i
	}
	for _, p := range partitions {
		if _, ok := colIndex[p]; !ok {
			return nil, fmt.Errorf("%w: partition column %q not present in schema", ErrInvalidArgument, p)
		}
	}

	rec := RegisterRecord{
		Name:             name,
		SourcePrefix:     prefix,
		ObjectPrefix:     prefix,
		Format:           format,
		PartitionColumns: partitions,
		Columns:          cols,
	}
	if schema.NumRows > 0 {
		n := schema.NumRows
		rec.RowCount = &n
	}

	dataset, version, storedCols, err := r.Store.Register(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &RegisterResult{Dataset: dataset, Version: version, Columns: storedCols}, nil
}

// ListParquetObjects returns the Parquet objects under prefix, bounded by
// maxListPages listing pages, sorted by key. It maps object-store failures to
// catalog sentinel errors.
func ListParquetObjects(ctx context.Context, objects objectstore.Store, prefix string) ([]objectstore.Object, error) {
	var out []objectstore.Object
	token := ""
	for page := 0; page < maxListPages; page++ {
		res, err := objects.List(ctx, objectstore.ListOptions{Prefix: prefix, MaxKeys: listPageSize, Token: token})
		if err != nil {
			return nil, mapObjectError(err)
		}
		for _, o := range res.Objects {
			if strings.HasSuffix(strings.ToLower(o.Key), ".parquet") {
				out = append(out, o)
			}
		}
		if !res.Truncated || res.NextToken == "" {
			break
		}
		token = res.NextToken
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func mapObjectError(err error) error {
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return fmt.Errorf("%w: bucket or prefix not found", ErrNotFound)
	case errors.Is(err, objectstore.ErrAccessDenied):
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	case errors.Is(err, objectstore.ErrUnavailable):
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	default:
		return err
	}
}

func mapInspectError(err error) error {
	if errors.Is(err, parquet.ErrMalformed) {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return mapObjectError(err)
}

func objectKeys(objects []objectstore.Object) []string {
	keys := make([]string, len(objects))
	for i, o := range objects {
		keys[i] = o.Key
	}
	return keys
}
