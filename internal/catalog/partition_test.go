package catalog

import (
	"reflect"
	"testing"
)

func TestDetectPartitionColumns(t *testing.T) {
	prefix := "data/sales"
	keys := []string{
		"data/sales/year=2024/month=01/part-000.parquet",
		"data/sales/year=2024/month=02/part-000.parquet",
		"data/sales/year=2025/month=01/part-001.parquet",
	}
	got := DetectPartitionColumns(prefix, keys)
	want := []string{"year", "month"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DetectPartitionColumns() = %v, want %v", got, want)
	}
}

func TestDetectPartitionColumnsUnpartitioned(t *testing.T) {
	prefix := "data/sales"
	keys := []string{"data/sales/part-000.parquet"}
	if got := DetectPartitionColumns(prefix, keys); len(got) != 0 {
		t.Errorf("DetectPartitionColumns() = %v, want empty", got)
	}
}

func TestDetectPartitionColumnsStopsAtNonPartition(t *testing.T) {
	prefix := "data/sales"
	keys := []string{"data/sales/year=2024/random/part-000.parquet"}
	got := DetectPartitionColumns(prefix, keys)
	want := []string{"year"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DetectPartitionColumns() = %v, want %v", got, want)
	}
}

func TestDetectPartitionColumnsDecodes(t *testing.T) {
	prefix := "data/sales"
	keys := []string{"data/sales/re%2Fgion=us-east/part-000.parquet"}
	got := DetectPartitionColumns(prefix, keys)
	want := []string{"re/gion"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DetectPartitionColumns() = %v, want %v", got, want)
	}
}
