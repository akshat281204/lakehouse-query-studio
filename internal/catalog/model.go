// Package catalog implements the durable dataset metadata catalog and the
// registration orchestration that inspects Parquet data and persists a
// versioned catalog record.
package catalog

import "time"

const (
	// NamespaceDefault is the single, permanent namespace of the v1 deployment.
	NamespaceDefault = "default"

	// Dataset states.
	DatasetActive   = "ACTIVE"
	DatasetArchived = "ARCHIVED"

	// Dataset version states.
	VersionPending  = "PENDING"
	VersionBuilding = "BUILDING"
	VersionReady    = "READY"
	VersionFailed   = "FAILED"

	// Ingestion job states.
	JobQueued    = "QUEUED"
	JobRunning   = "RUNNING"
	JobSucceeded = "SUCCEEDED"
	JobFailed    = "FAILED"
	JobCancelled = "CANCELLED"

	// Ingestion job kinds.
	KindRegister = "register"
	KindConvert  = "convert"

	// FormatParquet is the only dataset format supported in this phase.
	FormatParquet = "parquet"
)

// Namespace is the logical grouping boundary.
type Namespace struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
}

// Dataset is a named, namespace-scoped collection of Parquet data.
type Dataset struct {
	ID             string    `json:"id"`
	NamespaceID    string    `json:"namespaceId"`
	NamespaceSlug  string    `json:"namespace"`
	Name           string    `json:"name"`
	SourcePrefix   string    `json:"sourcePrefix"`
	Format         string    `json:"format"`
	Status         string    `json:"status"`
	RetainOriginal bool      `json:"retainOriginal"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// DatasetVersion is an immutable versioned snapshot of a dataset.
type DatasetVersion struct {
	ID               string    `json:"id"`
	DatasetID        string    `json:"datasetId"`
	Version          int       `json:"version"`
	ObjectPrefix     string    `json:"objectPrefix"`
	Format           string    `json:"format"`
	PartitionColumns []string  `json:"partitionColumns"`
	Status           string    `json:"status"`
	ErrorSummary     string    `json:"errorSummary,omitempty"`
	RowCount         *int64    `json:"rowCount,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
}

// SchemaColumn is a captured column description for a dataset version.
type SchemaColumn struct {
	ID          string `json:"-"`
	VersionID   string `json:"-"`
	Position    int    `json:"position"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Nullable    bool   `json:"nullable"`
	IsPartition bool   `json:"isPartition"`
}
