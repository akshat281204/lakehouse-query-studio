package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/migrations"
)

// migrationLockID is the arbitrary advisory-lock key guarding migrations.
const migrationLockID = 727274

// Migrate applies pending migrations to db in filename order, each in its own
// transaction, recording applied migrations in schema_migrations.
//
// A session-level advisory lock serializes concurrent runners (the API and the
// worker both migrate at startup), so the second one waits and then finds
// everything applied.
func Migrate(ctx context.Context, db *sql.DB) error {
	lock, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration lock connection: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if _, err := lock.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = lock.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		applied, err := migrationApplied(ctx, db, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if err := applyMigration(ctx, db, name, string(body)); err != nil {
			return err
		}
	}
	return nil
}

func migrationNames() ([]string, error) {
	var names []string
	err := fs.WalkDir(migrations.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".sql") {
			return nil
		}
		names = append(names, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func migrationApplied(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check migration %s: %w", name, err)
	}
	return exists, nil
}

func applyMigration(ctx context.Context, db *sql.DB, name, body string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range splitStatements(body) {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}

// splitStatements splits a SQL script into individual statements on semicolons.
// The migration files in this repository do not contain semicolons inside
// string literals, so a plain split is sufficient.
func splitStatements(body string) []string {
	// A small scanner rather than strings.Split: a ';' inside a '--' comment or a
	// single-quoted literal must not end a statement. Dollar-quoting is not used
	// by our migrations and is intentionally unsupported.
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	inQuote := false
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case inQuote:
			cur.WriteByte(c)
			if c == '\'' {
				inQuote = false // a doubled '' re-enters on the next byte
			}
		case c == '\'':
			inQuote = true
			cur.WriteByte(c)
		case c == '-' && i+1 < len(body) && body[i+1] == '-':
			for i < len(body) && body[i] != '\n' {
				i++
			}
			cur.WriteByte('\n')
		case c == ';':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return out
}
