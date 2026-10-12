// Command migrate applies and reverts the versioned database migrations.
//
// Usage:
//
//	migrate up          apply every pending migration
//	migrate down [N]    revert the last N applied migrations (default 1)
//	migrate version     print the current schema version
//	migrate force V     mark version V as clean after repairing a failed migration
//
// MIGRATIONS_DATABASE_URL must connect as the schema owner, not as the
// restricted role the service uses.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/IkkiDK/backend-challenge-go/migrations"
)

const usage = "usage: migrate up | down [N] | version | force V"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	dsn := os.Getenv("MIGRATIONS_DATABASE_URL")
	if dsn == "" {
		return errors.New("MIGRATIONS_DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	m, err := migrations.New(db)
	if err != nil {
		return errors.Join(err, db.Close())
	}
	defer m.Close()

	switch command, operands := args[0], args[1:]; {
	case command == "up" && len(operands) == 0:
		err = m.Up()
	case command == "down" && len(operands) <= 1:
		steps := 1
		if len(operands) == 1 {
			if steps, err = strconv.Atoi(operands[0]); err != nil || steps < 1 {
				return fmt.Errorf("down needs a positive number of steps, got %q", operands[0])
			}
		}
		err = m.Steps(-steps)
	case command == "force" && len(operands) == 1:
		version, convErr := strconv.Atoi(operands[0])
		if convErr != nil {
			return fmt.Errorf("force needs a version number, got %q", operands[0])
		}
		err = m.Force(version)
	case command == "version" && len(operands) == 0:
	default:
		return errors.New(usage)
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	version, dirty, err := m.Version()
	switch {
	case errors.Is(err, migrate.ErrNilVersion):
		fmt.Println("schema version: none (no migration applied)")
	case err != nil:
		return err
	default:
		fmt.Printf("schema version: %d (dirty: %t)\n", version, dirty)
	}
	return nil
}
