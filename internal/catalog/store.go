package catalog

import "context"

// RegisterRecord is the durable state produced by a successful registration.
type RegisterRecord struct {
	Name             string
	SourcePrefix     string
	ObjectPrefix     string
	Format           string
	PartitionColumns []string
	RowCount         *int64
	Columns          []SchemaColumn
}

// ListOptions controls dataset listing.
type ListOptions struct {
	Offset int
	Limit  int
	Query  string
}

// ListResult is one page of datasets.
type ListResult struct {
	Datasets []Dataset
	Total    int
}

// Store is the durable catalog. The Postgres implementation stores metadata in
// PostgreSQL; unit tests use an in-memory fake.
type Store interface {
	// Register creates a dataset, its first version (version 1), the schema
	// columns, and a SUCCEEDED register job in one transaction.
	Register(ctx context.Context, rec RegisterRecord) (Dataset, DatasetVersion, []SchemaColumn, error)
	// List returns a page of datasets ordered by name.
	List(ctx context.Context, opts ListOptions) (ListResult, error)
	// Get returns a dataset by id.
	Get(ctx context.Context, id string) (Dataset, error)
	// GetLatestVersion returns the highest-numbered version of a dataset.
	GetLatestVersion(ctx context.Context, datasetID string) (DatasetVersion, error)
	// GetSchema returns the schema columns of a version ordered by position.
	GetSchema(ctx context.Context, versionID string) ([]SchemaColumn, error)
}
