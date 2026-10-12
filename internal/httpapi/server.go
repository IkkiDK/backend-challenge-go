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

// Timeouts bound every phase of a connection, so slow or idle clients cannot
// hold connections and their goroutines indefinitely.
type Timeouts struct {
	// ReadHeader bounds reading the request headers and Read the whole
	// request, body included. Read also bounds the server's draining of a
	// body the handler did not read, as when it refuses the credentials.
	ReadHeader, Read time.Duration
	// Write bounds handling the request and writing the response.
	Write time.Duration
	// Idle bounds how long a keep-alive connection waits for its next request.
	Idle time.Duration
}

// maxHeaderBytes caps the request headers; an access token takes about 2 KiB.
const maxHeaderBytes = 32 << 10

// Server is the HTTP server with an explicit start and graceful stop.
type Server struct {
	server    *http.Server
	readiness *Readiness
	logger    *slog.Logger
	serving   chan struct{}
}

// NewServer prepares a server on addr; nothing listens until Start.
func NewServer(addr string, handler http.Handler, timeouts Timeouts, readiness *Readiness, logger *slog.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: timeouts.ReadHeader,
			ReadTimeout:       timeouts.Read,
			WriteTimeout:      timeouts.Write,
			IdleTimeout:       timeouts.Idle,
			MaxHeaderBytes:    maxHeaderBytes,
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
