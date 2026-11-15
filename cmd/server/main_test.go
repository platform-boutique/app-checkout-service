package main

import (
	"testing"
	"time"

	"checkout-service/internal/obs"
)

// CODE-04 (decision 2026-11-14): checkout may drain 15 s because a card charge must finish, and
// then needs a Kubernetes terminationGracePeriodSeconds of at least 25 s. The worst case after
// SIGTERM is the drain, plus the Kafka producer flush, plus the telemetry flush (both bounded by
// obs.DefaultShutdownTimeout). This keeps the three numbers inside that 25 s.
func TestShutdownBudgetFitsTheGracePeriod(t *testing.T) {
	const grace = 25 * time.Second
	worst := drainTimeout + 2*obs.DefaultShutdownTimeout
	if worst > grace {
		t.Fatalf("worst-case shutdown %v exceeds the %v grace period (drain %v + 2 x flush %v)", worst, grace, drainTimeout, obs.DefaultShutdownTimeout)
	}
	if drainTimeout > 15*time.Second {
		t.Fatalf("drainTimeout %v exceeds the 15 s allowed for checkout", drainTimeout)
	}
}
