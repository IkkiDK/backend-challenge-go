package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestLoopRunsUntilStoppedAndLetsTheStepFinish(t *testing.T) {
	var (
		calls    atomic.Int32
		finished atomic.Bool
		started  = make(chan struct{})
	)
	loop := NewLoop("test", func(ctx context.Context) (bool, error) {
		if calls.Add(1) == 3 {
			close(started)
			time.Sleep(50 * time.Millisecond) // in-flight work when Stop arrives
			finished.Store(true)
		}
		return true, nil
	}, time.Hour, time.Hour, discard)
	loop.Start()
	<-started

	if err := loop.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !finished.Load() {
		t.Fatal("Stop must let the in-flight step finish")
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("calls = %d, want no new step after Stop", n)
	}
}

func TestLoopCancelsTheStepWhenTheStopDeadlineExpires(t *testing.T) {
	var cancelled atomic.Bool
	started := make(chan struct{})
	loop := NewLoop("test", func(ctx context.Context) (bool, error) {
		close(started)
		<-ctx.Done()
		cancelled.Store(true)
		return false, ctx.Err()
	}, time.Hour, time.Hour, discard)
	loop.Start()
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := loop.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want DeadlineExceeded", err)
	}
	if !cancelled.Load() {
		t.Fatal("Stop must return only after the cancelled step has finished")
	}
}

func TestLoopBacksOffAfterErrorsAndWaitsWhenIdle(t *testing.T) {
	var calls atomic.Int32
	loop := NewLoop("test", func(ctx context.Context) (bool, error) {
		if calls.Add(1) == 1 {
			return false, errors.New("boom")
		}
		return false, nil
	}, 30*time.Millisecond, 50*time.Millisecond, discard)
	loop.Start()
	time.Sleep(100 * time.Millisecond)
	if err := loop.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	// One failure, then idle waits: a handful of calls, not a busy spin.
	if n := calls.Load(); n < 2 || n > 5 {
		t.Fatalf("calls = %d, want a few spaced calls", n)
	}
}
