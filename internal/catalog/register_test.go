package catalog

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

type fakeObjectStore struct {
	bucket  string
	keys    []objectstore.Object
	listErr error
}

func (f *fakeObjectStore) Bucket() string { return f.bucket }

func (f *fakeObjectStore) List(_ context.Context, opts objectstore.ListOptions) (objectstore.ListResult, error) {
	if f.listErr != nil {
		return objectstore.ListResult{}, f.listErr
	}
	var out []objectstore.Object
	for _, o := range f.keys {
		if strings.HasPrefix(o.Key, opts.Prefix) {
			out = append(out, o)
		}
	}
	return objectstore.ListResult{Objects: out}, nil
}

func (f *fakeObjectStore) Range(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return nil, objectstore.ErrNotFound
}

type fakeStore struct {
	registerResult RegisterResult
	registerErr    error
	registered     []RegisterRecord
}

func (f *fakeStore) Register(_ context.Context, rec RegisterRecord) (Dataset, DatasetVersion, []SchemaColumn, error) {
	f.registered = append(f.registered, rec)
	if f.registerErr != nil {
		return Dataset{}, DatasetVersion{}, nil, f.registerErr
	}
	return f.registerResult.Dataset, f.registerResult.Version, f.registerResult.Columns, nil
}

func (f *fakeStore) List(context.Context, ListOptions) (ListResult, error) {
	return ListResult{}, nil
}

func (f *fakeStore) Get(context.Context, string) (Dataset, error) {
	return Dataset{}, ErrNotFound
}

func (f *fakeStore) GetLatestVersion(context.Context, string) (DatasetVersion, error) {
	return DatasetVersion{}, ErrNotFound
}

func (f *fakeStore) GetLatestReadyVersion(context.Context, string) (DatasetVersion, error) {
	return DatasetVersion{}, ErrNotFound
}

func (f *fakeStore) GetSchema(context.Context, string) ([]SchemaColumn, error) {
	return nil, nil
}

func sampleSchema() *parquet.Schema {
	return &parquet.Schema{
		NumRows: 5,
		Columns: []parquet.Column{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "year", Type: "int32", Nullable: false},
			{Name: "month", Type: "int32", Nullable: false},
			{Name: "amount", Type: "double", Nullable: true},
		},
	}
}

func newTestRegistrar(store Store, objects *fakeObjectStore) *Registrar {
	r := NewRegistrar(store, objects)
	r.inspect = func(context.Context, parquet.ObjectReader, string, int64) (*parquet.Schema, error) {
		return sampleSchema(), nil
	}
	return r
}

func TestRegistrarRegister(t *testing.T) {
	store := &fakeStore{
		registerResult: RegisterResult{
			Dataset: Dataset{ID: "d1", Name: "sales"},
			Version: DatasetVersion{ID: "v1", Version: 1},
		},
	}
	objects := &fakeObjectStore{
		bucket: "lakehouse",
		keys: []objectstore.Object{
			{Key: "data/sales/year=2024/month=01/part-000.parquet", Size: 100},
		},
	}

	res, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:   "sales",
		Prefix: "data/sales",
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if res.Dataset.Name != "sales" || res.Version.Version != 1 {
		t.Errorf("result = %+v", res)
	}

	rec := store.registered[0]
	if rec.Name != "sales" || rec.ObjectPrefix != "data/sales" {
		t.Errorf("record = %+v", rec)
	}
	if len(rec.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(rec.Columns))
	}
	if rec.RowCount == nil || *rec.RowCount != 5 {
		t.Errorf("row count = %v, want 5", rec.RowCount)
	}
	// Partition columns were auto-detected from the key layout.
	if len(rec.PartitionColumns) != 2 || rec.PartitionColumns[0] != "year" || rec.PartitionColumns[1] != "month" {
		t.Errorf("partition columns = %v", rec.PartitionColumns)
	}
}

func TestRegistrarDuplicate(t *testing.T) {
	store := &fakeStore{registerErr: ErrAlreadyExists}
	objects := &fakeObjectStore{
		keys: []objectstore.Object{{Key: "data/sales/part-000.parquet", Size: 100}},
	}
	_, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:   "sales",
		Prefix: "data/sales",
	})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Register() error = %v, want ErrAlreadyExists", err)
	}
}

func TestRegistrarMissingPrefix(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjectStore{keys: nil}
	_, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:   "sales",
		Prefix: "data/sales",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Register() error = %v, want ErrNotFound", err)
	}
}

func TestRegistrarObjectStoreError(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjectStore{listErr: objectstore.ErrAccessDenied}
	_, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:   "sales",
		Prefix: "data/sales",
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Register() error = %v, want ErrUnavailable", err)
	}
}

func TestRegistrarMalformedSchema(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjectStore{
		keys: []objectstore.Object{{Key: "data/sales/part-000.parquet", Size: 100}},
	}
	r := NewRegistrar(store, objects)
	r.inspect = func(context.Context, parquet.ObjectReader, string, int64) (*parquet.Schema, error) {
		return nil, parquet.ErrMalformed
	}
	_, err := r.Register(context.Background(), RegisterRequest{Name: "sales", Prefix: "data/sales"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register() error = %v, want ErrInvalidArgument", err)
	}
}

func TestRegistrarInvalidName(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjectStore{keys: []objectstore.Object{{Key: "x/part.parquet", Size: 1}}}
	_, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:   "Sales_Invalid",
		Prefix: "x",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register() error = %v, want ErrInvalidArgument", err)
	}
}

func TestRegistrarRejectsUnknownPartitionColumn(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjectStore{
		keys: []objectstore.Object{{Key: "data/sales/part-000.parquet", Size: 100}},
	}
	_, err := newTestRegistrar(store, objects).Register(context.Background(), RegisterRequest{
		Name:             "sales",
		Prefix:           "data/sales",
		PartitionColumns: []string{"not_a_column"},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register() error = %v, want ErrInvalidArgument", err)
	}
}
