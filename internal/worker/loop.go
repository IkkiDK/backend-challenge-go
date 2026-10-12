// Package worker runs background polling loops with an explicit, observable
// lifecycle.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Step performs one unit of background work. It reports whether it found
// work, so the loop can continue at once instead of waiting.
type Step func(ctx context.Context) (busy bool, err error)

// Loop calls a step until stopped, waiting Idle between steps that found
// nothing and backing off exponentially (up to MaxBackoff) after errors.
type Loop struct {
	name       string
	step       Step
	idle       time.Duration
	maxBackoff time.Duration
	logger     *slog.Logger

	stopping   chan struct{}
	stopOnce   sync.Once
	work       context.Context
	cancelWork context.CancelFunc
	done       chan struct{}
}

// NewLoop returns a loop that is not started yet.
func NewLoop(name string, step Step, idle, maxBackoff time.Duration, logger *slog.Logger) *Loop {
	work, cancelWork := context.WithCancel(context.Background())
	return &Loop{
		name: name, step: step, idle: idle, maxBackoff: maxBackoff,
		logger:   logger.With("worker", name),
		stopping: make(chan struct{}), work: work, cancelWork: cancelWork, done: make(chan struct{}),
	}
}

// Start runs the loop in the background.
func (l *Loop) Start() {
	l.logger.Info("worker started")
	go l.run()
}

// Stop lets the current step finish and returns once the loop has exited. If
// ctx expires first, the step's context is cancelled (its SQL transaction
// rolls back and its claims are released) and Stop still waits for the exit.
func (l *Loop) Stop(ctx context.Context) error {
	l.stopOnce.Do(func() { close(l.stopping) })
	defer l.logger.Info("worker stopped")
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		l.cancelWork()
		<-l.done
		return fmt.Errorf("worker %s cancelled its in-flight step: %w", l.name, ctx.Err())
	}
}

// Done is closed once the loop has exited.
func (l *Loop) Done() <-chan struct{} {
	return l.done
}

func (l *Loop) run() {
	defer close(l.done)
	backoff := l.idle
	for {
		select {
		case <-l.stopping:
			return
		default:
		}
		busy, err := l.step(l.work)
		var wait time.Duration
		switch {
		case err != nil:
			l.logger.Warn("worker step failed", "error", err, "retryIn", backoff.String())
			wait = backoff
			backoff = min(backoff*2, l.maxBackoff)
		case busy:
			backoff = l.idle
			continue
		default:
			backoff = l.idle
			wait = l.idle
		}
		select {
		case <-l.stopping:
			return
		case <-time.After(wait):
		}
	}
}
