package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server is the HTTP server with an explicit start and graceful stop.
type Server struct {
	server    *http.Server
	readiness *Readiness
	logger    *slog.Logger
	serving   chan struct{}
}

// NewServer prepares a server on addr; nothing listens until Start.
func NewServer(addr string, handler http.Handler, readHeaderTimeout time.Duration, readiness *Readiness, logger *slog.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: readHeaderTimeout,
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		},
		readiness: readiness,
		logger:    logger,
		serving:   make(chan struct{}),
	}
}

// Start binds the address, so a taken port fails startup, then serves in the
// background.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.server.Addr, err)
	}
	s.logger.Info("http server listening", "addr", listener.Addr().String())
	go func() {
		defer close(s.serving)
		if err := s.server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http server stopped unexpectedly", "error", err)
		}
	}()
	return nil
}

// Stop fails readiness, stops accepting connections and waits for in-flight
// requests until ctx expires; requests still running then are cut off.
func (s *Server) Stop(ctx context.Context) error {
	s.readiness.StartDraining()
	err := s.server.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.server.Close())
	}
	<-s.serving
	s.logger.Info("http server stopped")
	return err
}
