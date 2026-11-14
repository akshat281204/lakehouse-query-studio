package catalog

import (
	"net/url"
	"strings"
)

// DetectPartitionColumns extracts Hive-style "name=value" partition column
// names from object keys relative to the dataset prefix. Only the leading run
// of partition segments (before the file name) is considered, and the same
// column name is reported once, in first-seen order.
func DetectPartitionColumns(prefix string, keys []string) []string {
	var order []string
	seen := map[string]bool{}
	for _, key := range keys {
		rel := strings.TrimPrefix(key, prefix)
		rel = strings.TrimPrefix(rel, "/")
		parts := strings.Split(rel, "/")
		for i := 0; i < len(parts)-1; i++ {
			name, _, ok := splitPartitionSegment(parts[i])
			if !ok {
				break
			}
			if !seen[name] {
				seen[name] = true
				order = append(order, name)
			}
		}
	}
	return order
}

func splitPartitionSegment(seg string) (name, value string, ok bool) {
	idx := strings.IndexByte(seg, '=')
	if idx <= 0 {
		return "", "", false
	}
	name, err := percentDecode(seg[:idx])
	if err != nil || name == "" {
		return "", "", false
	}
	value, err = percentDecode(seg[idx+1:])
	if err != nil {
		return "", "", false
	}
	return name, value, true
}

func percentDecode(s string) (string, error) { return url.PathUnescape(s) }
