package catalog

import "testing"

func TestValidateDatasetName(t *testing.T) {
	valid := []string{"sales", "sales-2024", "a", "a1", "events-raw-01"}
	for _, name := range valid {
		got, err := ValidateDatasetName(name)
		if err != nil {
			t.Errorf("ValidateDatasetName(%q) error = %v", name, err)
			continue
		}
		if got != name {
			t.Errorf("ValidateDatasetName(%q) = %q, want identity", name, got)
		}
	}

	invalid := []string{
		"",
		"Sales",      // uppercase
		"sales data", // space
		"-sales",     // leading hyphen
		"sales-",     // trailing hyphen
		"sales_2024", // underscore
		"sales/team", // slash
		"销售",         // non-ASCII
		"a-very-long-name-that-exceeds-the-sixty-three-character-limit-abcdefghijklmnop",
	}
	for _, name := range invalid {
		if _, err := ValidateDatasetName(name); err == nil {
			t.Errorf("ValidateDatasetName(%q) expected error, got nil", name)
		}
	}
}

func TestValidatePrefix(t *testing.T) {
	valid := []string{"data/sales", "uploads/in", "x", "a/b/c/version=1"}
	for _, p := range valid {
		if _, err := ValidatePrefix(p); err != nil {
			t.Errorf("ValidatePrefix(%q) error = %v", p, err)
		}
	}

	invalid := []string{"", "/data", "data/", "data//sales", "data/../x", "data/./x", "data/\x00/x"}
	for _, p := range invalid {
		if _, err := ValidatePrefix(p); err == nil {
			t.Errorf("ValidatePrefix(%q) expected error, got nil", p)
		}
	}
}
