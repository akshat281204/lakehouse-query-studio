package catalog

import (
	"fmt"
	"regexp"
	"strings"
)

// datasetNamePattern matches the canonical dataset key grammar: lowercase ASCII
// [a-z0-9-], no leading/trailing hyphen, at most 63 characters.
var datasetNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// ValidateDatasetName returns the canonical name if name already satisfies the
// canonical key grammar, or an error otherwise. The mapping from name to
// object-key component is the identity function: no normalization is applied,
// so two distinct accepted names can never collide on a key prefix.
func ValidateDatasetName(name string) (string, error) {
	n := strings.TrimSpace(name)
	if n == "" {
		return "", fmt.Errorf("dataset name must not be empty")
	}
	if len(n) > 63 {
		return "", fmt.Errorf("dataset name must be at most 63 characters")
	}
	if !datasetNamePattern.MatchString(n) {
		return "", fmt.Errorf("dataset name must be lowercase ASCII [a-z0-9-] with no leading or trailing hyphen")
	}
	return n, nil
}

// ValidatePrefix validates an object-store prefix. It must be a clean relative
// prefix without leading/trailing slashes or path traversal segments.
func ValidatePrefix(prefix string) (string, error) {
	p := strings.TrimSpace(prefix)
	if p == "" {
		return "", fmt.Errorf("prefix must not be empty")
	}
	if strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return "", fmt.Errorf("prefix must not begin or end with '/'")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("prefix must not contain empty, '.', or '..' segments")
		}
		if strings.ContainsRune(seg, 0) {
			return "", fmt.Errorf("prefix must not contain NUL bytes")
		}
	}
	return p, nil
}
