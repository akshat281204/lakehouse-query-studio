package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type policy struct {
	Statement []struct {
		Sid      string
		Effect   string
		Action   []string
		Resource []string
	}
}

func load(t *testing.T, name string) policy {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "ops", "minio-policies", name))
	if err != nil {
		t.Fatal(err)
	}
	var p policy
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return p
}

// allows reports whether any statement grants action on a key under prefix.
func (p policy) allows(action, key string) bool {
	for _, s := range p.Statement {
		if s.Effect != "Allow" {
			continue
		}
		for _, a := range s.Action {
			if a != action && a != "s3:*" {
				continue
			}
			for _, r := range s.Resource {
				pre := strings.TrimSuffix(strings.TrimPrefix(r, "arn:aws:s3:::lakehouse/"), "*")
				if strings.HasPrefix(r, "arn:aws:s3:::lakehouse/") && strings.HasPrefix(key, pre) {
					return true
				}
			}
		}
	}
	return false
}

// These pin the least-privilege separation documented in docs/security.md.
func TestLeastPrivilegePolicies(t *testing.T) {
	api, ingest, queryW := load(t, "api.json"), load(t, "ingest-worker.json"), load(t, "query-worker.json")

	// Nobody gets a wildcard.
	for name, p := range map[string]policy{"api": api, "ingest": ingest, "query": queryW} {
		for _, s := range p.Statement {
			for _, a := range s.Action {
				if a == "s3:*" || a == "*" {
					t.Errorf("%s grants %s", name, a)
				}
			}
			for _, r := range s.Resource {
				if r == "*" || r == "arn:aws:s3:::*" {
					t.Errorf("%s grants resource %s", name, r)
				}
			}
		}
	}

	// The query credential can read datasets and spool results, nothing else.
	for _, k := range []string{"lake/default/x/part.parquet", "data/x/part.parquet"} {
		if !queryW.allows("s3:GetObject", k) {
			t.Errorf("query worker cannot read %s", k)
		}
		if queryW.allows("s3:PutObject", k) || queryW.allows("s3:DeleteObject", k) {
			t.Errorf("query worker can modify %s", k)
		}
	}
	if !queryW.allows("s3:PutObject", "exports/default/q/attempt=1/result.parquet") {
		t.Error("query worker cannot spool results")
	}
	for _, k := range []string{"uploads/x", "staging/x"} {
		if queryW.allows("s3:GetObject", k) || queryW.allows("s3:PutObject", k) {
			t.Errorf("query worker can touch %s", k)
		}
	}

	// The API never writes datasets, staging or exports.
	for _, k := range []string{"lake/x", "staging/x", "exports/x", "data/x"} {
		if api.allows("s3:PutObject", k) || api.allows("s3:DeleteObject", k) {
			t.Errorf("API can modify %s", k)
		}
	}
	if !api.allows("s3:GetObject", "exports/default/q/attempt=1/result.parquet") || !api.allows("s3:PutObject", "uploads/x") {
		t.Error("API lost a permission it needs")
	}
	if api.allows("s3:GetObject", "staging/x") {
		t.Error("API can read staging")
	}

	// Ingestion writes lake/ and staging/ but cannot write exports/.
	if !ingest.allows("s3:PutObject", "lake/x") || !ingest.allows("s3:PutObject", "staging/x") {
		t.Error("ingest worker cannot write its prefixes")
	}
	if ingest.allows("s3:PutObject", "exports/x") {
		t.Error("ingest worker can write exports")
	}
}
