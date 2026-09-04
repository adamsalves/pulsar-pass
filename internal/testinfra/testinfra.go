// Package testinfra boots real infrastructure for test binaries: a
// thin wrapper over testcontainers with a retry for the reaper boot
// flake (#54, #58). Test-only support — no production code imports it.
package testinfra

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// StartContainerE runs a testcontainer start, retrying once when the
// reaper (ryuk) is not ready in time: the first boot of a session
// waits for the reaper container against a 60s internal deadline, and
// on a cold or busy runner that wait can blow up while the code under
// test is perfectly healthy — seen on the v0.5.0 release gate (#54),
// same runner-contention class as #47. The abandoned first container
// is not the caller's concern: CI runners are ephemeral and a healthy
// reaper on the retry claims the session.
//
// The error-returning variant exists for setup paths that stash the
// error for later reporting (a sync.Once shared across tests must
// complete with the error recorded, not exit the first caller's test).
func StartContainerE[T testcontainers.Container](what string, run func(context.Context) (T, error)) (T, error) {
	ctx := context.Background()
	c, err := run(ctx)
	if err != nil && strings.Contains(err.Error(), "reaper") {
		// stderr, not t.Logf: CI runs without -v, and the successful
		// retry is exactly the signal worth auditing (flake recurrence
		// of #54) — a passing test would swallow the log line.
		fmt.Fprintf(os.Stderr, "[testinfra] %s boot flaked on the testcontainers reaper; retrying once: %v\n", what, err)
		c, err = run(ctx)
	}
	return c, err
}

// StartContainer is StartContainerE with t.Fatalf on failure. Do not
// use inside a sync.Once: a Fatalf there exits the first caller's test
// while marking the once as done, hiding the failure from every later
// test — use StartContainerE and record the error instead.
func StartContainer[T testcontainers.Container](t *testing.T, what string, run func(context.Context) (T, error)) T {
	t.Helper()
	c, err := StartContainerE(what, run)
	if err != nil {
		t.Fatalf("start %s: %v", what, err)
	}
	return c
}
