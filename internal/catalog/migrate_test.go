package catalog

import "testing"

func TestSplitStatements(t *testing.T) {
	body := "-- comment\nCREATE TABLE a (x int);\nINSERT INTO a VALUES (1);\n"
	got := splitStatements(body)
	if len(got) != 2 {
		t.Fatalf("splitStatements() = %d statements, want 2: %#v", len(got), got)
	}
	if got[1] != "INSERT INTO a VALUES (1)" {
		t.Errorf("second statement = %q", got[1])
	}
}

func TestMigrationNames(t *testing.T) {
	names, err := migrationNames()
	if err != nil {
		t.Fatalf("migrationNames() error = %v", err)
	}
	if len(names) != 5 {
		t.Fatalf("migrationNames() = %d names, want 5: %v", len(names), names)
	}
	if names[0] != "0001_namespaces.sql" {
		t.Errorf("first migration = %q, want 0001_namespaces.sql", names[0])
	}
	if names[4] != "0005_ingestion_jobs.sql" {
		t.Errorf("last migration = %q, want 0005_ingestion_jobs.sql", names[4])
	}
}
