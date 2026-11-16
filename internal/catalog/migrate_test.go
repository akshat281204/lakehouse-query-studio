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
	if len(names) != 6 {
		t.Fatalf("migrationNames() = %d names, want 6: %v", len(names), names)
	}
	if names[0] != "0001_namespaces.sql" {
		t.Errorf("first migration = %q, want 0001_namespaces.sql", names[0])
	}
	if names[5] != "0006_ingestion_inputs.sql" {
		t.Errorf("last migration = %q, want 0006_ingestion_inputs.sql", names[5])
	}
}

func TestSplitStatementsIgnoresSemicolonsInCommentsAndQuotes(t *testing.T) {
	body := "-- a note; with a semicolon\nCREATE TABLE a (x text DEFAULT 'a;b'); -- trailing; comment\nINSERT INTO a VALUES ('it''s');\n"
	got := splitStatements(body)
	if len(got) != 2 {
		t.Fatalf("splitStatements() = %d statements, want 2: %#v", len(got), got)
	}
	if got[0] != "CREATE TABLE a (x text DEFAULT 'a;b')" {
		t.Errorf("first statement = %q", got[0])
	}
	if got[1] != "INSERT INTO a VALUES ('it''s')" {
		t.Errorf("second statement = %q", got[1])
	}
}
