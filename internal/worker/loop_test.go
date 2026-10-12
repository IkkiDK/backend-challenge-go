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
	errBoom := errors.New("boom")
	// atLeast is the shortest gap accepted for a wait: timers never fire
	// early, and the margin absorbs the granularity of the clock.
	atLeast := func(wait time.Duration) time.Duration { return wait * 4 / 5 }

	t.Run("each failure doubles the wait up to the cap", func(t *testing.T) {
		const (
			idle       = 10 * time.Millisecond
			maxBackoff = 40 * time.Millisecond
			run        = 300 * time.Millisecond
			// minCalls makes the waits reach the cap: 10ms, 20ms, then 40ms.
			minCalls = 4
		)
		next := func(wait time.Duration) time.Duration { return min(wait*2, maxBackoff) }
		var calls []time.Time
		loop := NewLoop("test", func(context.Context) (bool, error) {
			calls = append(calls, time.Now())
			return false, errBoom
		}, idle, maxBackoff, discard)
		started := time.Now()
		loop.Start()
		time.Sleep(run)
		// Stop returns once the loop has exited, so calls is complete.
		if err := loop.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)

		// With no wait cut short, an exact schedule fits the most calls.
		maxCalls := 0
		for start, wait := time.Duration(0), idle; start <= elapsed; start, wait = start+wait, next(wait) {
			maxCalls++
		}
		if len(calls) < minCalls || len(calls) > maxCalls {
			t.Fatalf("%d calls in %v, want %d to %d", len(calls), elapsed, minCalls, maxCalls)
		}
		for i, wait := 1, idle; i < len(calls); i, wait = i+1, next(wait) {
			if gap := calls[i].Sub(calls[i-1]); gap < atLeast(wait) {
				t.Errorf("wait %d lasted %v, want %v", i, gap, wait)
			}
		}
	})

	t.Run("a success resets the wait to idle", func(t *testing.T) {
		const (
			idle       = 5 * time.Millisecond
			maxBackoff = time.Second
			// failures grow the wait the next failure would get to idle<<failures (640ms).
			failures = 7
		)
		grown := idle << failures
		for name, busy := range map[string]bool{"busy step": true, "idle step": false} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				var calls []time.Time
				done := make(chan struct{})
				// Calls 1-7 fail, call 8 succeeds, call 9 fails again, call 10 ends the run.
				loop := NewLoop("test", func(context.Context) (bool, error) {
					calls = append(calls, time.Now())
					switch len(calls) {
					case failures + 1:
						return busy, nil
					case failures + 3:
						close(done)
						return false, nil
					}
					return false, errBoom
				}, idle, maxBackoff, discard)
				loop.Start()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the loop stopped calling the step")
				}
				if err := loop.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}

				if gap := calls[failures].Sub(calls[failures-1]); gap < atLeast(grown/2) {
					t.Fatalf("the last failure before the success waited %v, want the grown %v", gap, grown/2)
				}
				// Far below the grown wait, yet far above an idle wait under any load.
				if gap := calls[failures+2].Sub(calls[failures+1]); gap >= grown/2 {
					t.Fatalf("the first failure after the success waited %v, want the idle %v instead of %v", gap, idle, grown)
				}
			})
		}
	})
}
