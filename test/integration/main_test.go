//go:build integration

// Package integration runs the wallet service against real PostgreSQL,
// Keycloak and MiniStack containers, started once per test run from the same
// provisioning files as docker-compose.yml. Tests run serially because some
// of them pause dependencies or kill service processes.
//
//	go test -tags=integration -count=1 ./test/integration/...
package integration

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/IkkiDK/backend-challenge-go/migrations"
)

const (
	postgresImage  = "postgres:18.6-alpine"
	ministackImage = "ministackorg/ministack:1.5.26"
	keycloakImage  = "quay.io/keycloak/keycloak:26.8.0"

	ownerPassword       = "integration-owner"
	appPassword         = "integration-app"
	credentialsFilePath = "/shared/aws/credentials"
	containerStartLimit = 4 * time.Minute

	rootAccessKey = "test"
	rootSecretKey = "test"
)

// env is the shared test environment started by TestMain.
var env *environment

type environment struct {
	ownerDSN    string
	appDSN      string
	issuer      string
	jwksURL     string
	tokenURL    string
	sqsEndpoint string
	// credentials maps each provisioned IAM identity (profile) to its keys.
	credentials map[string]awsKeys
	owner       *pgxpool.Pool
	postgres    *testcontainers.DockerContainer
	ministack   *testcontainers.DockerContainer
	keycloak    *testcontainers.DockerContainer
}

type awsKeys struct {
	accessKeyID     string
	secretAccessKey string
}

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration environment:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerStartLimit)
	defer cancel()

	e, err := startEnvironment(ctx)
	defer e.terminate()
	if err != nil {
		return 0, err
	}
	env = e
	return m.Run(), nil
}

func repoPath(parts ...string) string {
	path, err := filepath.Abs(filepath.Join(append([]string{"..", ".."}, parts...)...))
	if err != nil {
		panic(err)
	}
	return path
}

func startEnvironment(ctx context.Context) (*environment, error) {
	e := &environment{}
	type started struct {
		name      string
		container *testcontainers.DockerContainer
		err       error
	}
	results := make(chan started, 3)
	go func() {
		c, err := testcontainers.Run(ctx, postgresImage,
			testcontainers.WithEnv(map[string]string{
				"POSTGRES_DB":            "wallet",
				"POSTGRES_USER":          "wallet_owner",
				"POSTGRES_PASSWORD":      ownerPassword,
				"WALLET_APP_DB_PASSWORD": appPassword,
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      repoPath("deploy", "postgres", "initdb", "01-app-role.sh"),
				ContainerFilePath: "/docker-entrypoint-initdb.d/01-app-role.sh",
				FileMode:          0o755,
			}),
			testcontainers.WithExposedPorts("5432/tcp"),
			// The image restarts PostgreSQL once after running the init scripts.
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(containerStartLimit)),
		)
		results <- started{"postgres", c, err}
	}()
	go func() {
		c, err := testcontainers.Run(ctx, ministackImage,
			testcontainers.WithEnv(map[string]string{
				"AUTH":                    "true",
				"WALLET_CREDENTIALS_FILE": credentialsFilePath,
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      repoPath("deploy", "ministack", "ready.d", "10-provision.sh"),
				ContainerFilePath: "/etc/localstack/init/ready.d/10-provision.sh",
				FileMode:          0o755,
			}),
			testcontainers.WithExposedPorts("4566/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/_ministack/ready").WithPort("4566/tcp").
				WithStartupTimeout(containerStartLimit).
				WithResponseMatcher(func(body io.Reader) bool {
					raw, _ := io.ReadAll(body)
					return strings.Contains(string(raw), `"failed": 0`)
				})),
		)
		results <- started{"ministack", c, err}
	}()
	go func() {
		c, err := testcontainers.Run(ctx, keycloakImage,
			testcontainers.WithCmd("start-dev", "--import-realm"),
			testcontainers.WithEnv(map[string]string{
				"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
				"KC_BOOTSTRAP_ADMIN_PASSWORD": "admin",
				"KC_HEALTH_ENABLED":           "true",
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      repoPath("deploy", "keycloak", "wallet-realm.json"),
				ContainerFilePath: "/opt/keycloak/data/import/wallet-realm.json",
				FileMode:          0o644,
			}),
			testcontainers.WithExposedPorts("8080/tcp", "9000/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/health/ready").WithPort("9000/tcp").
				WithStartupTimeout(containerStartLimit)),
		)
		results <- started{"keycloak", c, err}
	}()

	var errs []error
	for range 3 {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("start %s: %w", r.name, r.err))
		}
		switch r.name {
		case "postgres":
			e.postgres = r.container
		case "ministack":
			e.ministack = r.container
		case "keycloak":
			e.keycloak = r.container
		}
	}
	if len(errs) > 0 {
		return e, errors.Join(errs...)
	}
	keycloak := e.keycloak

	postgresEndpoint, err := e.postgres.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		return e, err
	}
	e.ownerDSN = fmt.Sprintf("postgres://wallet_owner:%s@%s/wallet?sslmode=disable", ownerPassword, postgresEndpoint)
	e.appDSN = fmt.Sprintf("postgres://wallet_app:%s@%s/wallet?sslmode=disable", appPassword, postgresEndpoint)

	keycloakEndpoint, err := keycloak.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		return e, err
	}
	// Without a fixed hostname, Keycloak issues tokens for the URL they were
	// requested from; tests and service processes both use this one.
	e.issuer = keycloakEndpoint + "/realms/wallet"
	e.jwksURL = e.issuer + "/protocol/openid-connect/certs"
	e.tokenURL = e.issuer + "/protocol/openid-connect/token"

	if e.sqsEndpoint, err = e.ministack.PortEndpoint(ctx, "4566/tcp", "http"); err != nil {
		return e, err
	}
	if e.credentials, err = readCredentials(ctx, e.ministack); err != nil {
		return e, err
	}
	if err := migrateUp(e.ownerDSN); err != nil {
		return e, err
	}
	if e.owner, err = pgxpool.New(ctx, e.ownerDSN); err != nil {
		return e, err
	}
	return e, nil
}

func (e *environment) terminate() {
	if e == nil {
		return
	}
	if e.owner != nil {
		e.owner.Close()
	}
	for _, c := range []*testcontainers.DockerContainer{e.postgres, e.ministack, e.keycloak} {
		if err := testcontainers.TerminateContainer(c); err != nil {
			fmt.Fprintln(os.Stderr, "terminate container:", err)
		}
	}
}

func migrateUp(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	m, err := migrations.New(db)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// readCredentials parses the AWS credentials file written by the MiniStack
// provisioning script.
func readCredentials(ctx context.Context, ministack *testcontainers.DockerContainer) (map[string]awsKeys, error) {
	reader, err := ministack.CopyFileFromContainer(ctx, credentialsFilePath)
	if err != nil {
		return nil, fmt.Errorf("read provisioned credentials: %w", err)
	}
	defer reader.Close()
	credentials := make(map[string]awsKeys)
	var profile string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "[") {
			profile = strings.Trim(line, "[]")
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		keys := credentials[profile]
		switch strings.TrimSpace(key) {
		case "aws_access_key_id":
			keys.accessKeyID = strings.TrimSpace(value)
		case "aws_secret_access_key":
			keys.secretAccessKey = strings.TrimSpace(value)
		}
		credentials[profile] = keys
	}
	for _, profile := range []string{"wallet-service", "wager-producer", "events-reader"} {
		if credentials[profile].accessKeyID == "" {
			return nil, fmt.Errorf("profile %s missing from the provisioned credentials", profile)
		}
	}
	return credentials, scanner.Err()
}
