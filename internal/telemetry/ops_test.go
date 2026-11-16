package telemetry

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The ops dashboard and alert rules may only reference metrics this code
// actually exports, so a rename can never silently blank a panel.
func TestOpsFilesReferenceOnlyExportedMetrics(t *testing.T) {
	m := New("t")
	var buf bytes.Buffer
	m.Registry.Render(&buf)
	exported := map[string]bool{}
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(l, "# TYPE ") {
			exported[strings.Fields(l)[2]] = true
		}
	}
	ident := regexp.MustCompile(`lqs_[a-z_]+`)
	suffix := regexp.MustCompile(`_(bucket|sum|count)$`)

	files, _ := filepath.Glob("../../ops/grafana/dashboards/*.json")
	alerts, _ := filepath.Glob("../../ops/prometheus/alerts.yml")
	files = append(files, alerts...)
	if len(files) < 2 {
		t.Fatalf("ops files not found: %v", files)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		found := ident.FindAllString(string(b), -1)
		if len(found) == 0 {
			t.Fatalf("%s references no lqs_ metrics", f)
		}
		for _, name := range found {
			base := suffix.ReplaceAllString(name, "")
			if !exported[name] && !exported[base] {
				t.Errorf("%s references %q, which is not exported", f, name)
			}
		}
	}
}

func TestDashboardJSONIsValidAndUsesNoCallerValuedLabels(t *testing.T) {
	b, err := os.ReadFile("../../ops/grafana/dashboards/lakehouse-ops.json")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, bad := range []string{"query_id", "dataset", "statement", "object_key", "job_id"} {
		if strings.Contains(s, bad) {
			t.Errorf("dashboard groups by or mentions %q", bad)
		}
	}
	if !strings.Contains(s, `"uid": "lqs-ops"`) {
		t.Error("unexpected dashboard")
	}
}
