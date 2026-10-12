// Package migrations embeds the versioned SQL migrations of the wallet schema.
// Each NNNNNN_name.up.sql has a matching .down.sql that reverts it; a migration
// file runs as a single implicit transaction, so it applies entirely or not at all.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed *.sql
var files embed.FS

// New returns a migrator over db, which must be connected as the schema owner.
// Concurrent migrators are serialized by a Postgres advisory lock.
func New(db *sql.DB) (*migrate.Migrate, error) {
	source, err := iofs.New(files, ".")
	if err != nil {
		return nil, fmt.Errorf("load embedded migrations: %w", err)
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return nil, fmt.Errorf("prepare migration driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return nil, fmt.Errorf("create migrator: %w", err)
	}
	return m, nil
}
