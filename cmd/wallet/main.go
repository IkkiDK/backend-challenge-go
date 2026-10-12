// Command wallet runs the wallet service: the HTTP API plus, as enabled by
// configuration, the SQS consumer, the outbox relay and the pending-reference
// worker. SIGINT and SIGTERM trigger a graceful shutdown.
package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"github.com/IkkiDK/backend-challenge-go/internal/bootstrap"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

func main() {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration:\n%v\n", err)
		os.Exit(1)
	}
	fx.New(bootstrap.Options(cfg)).Run()
}
