package httpapi

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	// shortTimeout is the timeout under test; clientPatience is how long the
	// client waits for the server to close a connection, many times longer.
	shortTimeout   = 200 * time.Millisecond
	clientPatience = 3 * time.Second
)

// startServer serves a handler that refuses every request without reading its
// body, as the API does with missing credentials.
func startServer(t *testing.T, timeouts Timeouts) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	refuse := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	server := NewServer(addr, refuse, timeouts, NewReadiness(), slog.New(slog.DiscardHandler))
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(t.Context()) })
	return addr
}

// readUntilClosed returns what the server sent before closing the connection,
// failing if it keeps the connection open beyond the client's patience.
func readUntilClosed(t *testing.T, conn net.Conn, reader io.Reader) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(clientPatience)); err != nil {
		t.Fatal(err)
	}
	received, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("the server kept the connection open for %v (received %q): %v", clientPatience, received, err)
	}
	return string(received)
}

func TestAStalledRequestBodyDoesNotHoldTheConnection(t *testing.T) {
	addr := startServer(t, Timeouts{ReadHeader: shortTimeout, Read: shortTimeout, Write: clientPatience, Idle: clientPatience})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "POST /wallets HTTP/1.1\r\nHost: wallet\r\nContent-Length: 65536\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}

	if received := readUntilClosed(t, conn, conn); !strings.HasPrefix(received, "HTTP/1.1 401") {
		t.Fatalf("received %q, want the 401 before the connection closes", received)
	}
}

func TestAnIdleConnectionIsClosed(t *testing.T) {
	// Without an idle timeout the server would fall back to the read timeout,
	// so that one is longer than the client's patience.
	addr := startServer(t, Timeouts{ReadHeader: shortTimeout, Read: 2 * clientPatience, Write: clientPatience, Idle: shortTimeout})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /wallets/w HTTP/1.1\r\nHost: wallet\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if rest := readUntilClosed(t, conn, reader); rest != "" {
		t.Fatalf("unexpected data after the response: %q", rest)
	}
}
