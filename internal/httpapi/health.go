package httpapi

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// readinessCheckTimeout bounds each dependency check.
const readinessCheckTimeout = 2 * time.Second

// Check probes one dependency.
type Check struct {
	Name  string
	Probe func(ctx context.Context) error
}

// Readiness reports whether the instance should receive traffic: every
// dependency answers and the instance is not shutting down.
type Readiness struct {
	checks   []Check
	draining atomic.Bool
}

// NewReadiness returns readiness over checks.
func NewReadiness(checks ...Check) *Readiness {
	return &Readiness{checks: checks}
}

// StartDraining makes readiness fail from now on, so load balancers stop
// routing new requests while in-flight ones complete.
func (rd *Readiness) StartDraining() {
	rd.draining.Store(true)
}

func (rd *Readiness) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rd.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "DRAINING"})
		return
	}
	status, results := http.StatusOK, make(map[string]string, len(rd.checks))
	for _, check := range rd.checks {
		ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
		err := check.Probe(ctx)
		cancel()
		results[check.Name] = "UP"
		if err != nil {
			results[check.Name] = "DOWN"
			status = http.StatusServiceUnavailable
		}
	}
	overall := "UP"
	if status != http.StatusOK {
		overall = "DOWN"
	}
	writeJSON(w, status, map[string]any{"status": overall, "checks": results})
}

// live reports that the process is running; it never checks dependencies,
// so a database outage does not get healthy instances restarted.
func live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}
