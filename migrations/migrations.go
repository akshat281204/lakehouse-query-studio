// Package migrations embeds the ordered SQL migration files applied to
// PostgreSQL by the catalog migration runner. Files are applied in filename
// order, so the numeric prefixes define the sequence.
package migrations

import "embed"

// FS holds the migration SQL files, applied in lexicographic (filename) order.
//
//go:embed *.sql
var FS embed.FS
