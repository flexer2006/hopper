//go:build integration

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestLiveATOPS01WorkerSIGTERM(t *testing.T) {
	requireDisruptive(t)
	_ = requireCompose(t)

	elapsed := composeStopStart(t, "worker", 40)
	if elapsed > 40*time.Second {
		t.Fatalf("worker stop took %s, want ≤40s (NFR-01 + grace)", elapsed)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	if !pollHealthz(ctx) {
		t.Fatal("healthz down after worker start")
	}
}

func TestLiveATOPS02APISIGTERM(t *testing.T) {
	requireDisruptive(t)
	_ = requireCompose(t)

	elapsed := composeStopStart(t, "api", 20)
	if elapsed > 25*time.Second {
		t.Fatalf("api stop took %s, want ≤25s (OPS-06 + grace)", elapsed)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	if !pollHealthz(ctx) {
		t.Fatal("healthz down after api start")
	}
}

func requireDisruptive(t *testing.T) {
	t.Helper()

	if os.Getenv("HOPPER_INT_DISRUPTIVE") != "1" {
		t.Skip("set HOPPER_INT_DISRUPTIVE=1 to run AT-OPS compose stop")
	}
}

func composeStopStart(t *testing.T, service string, stopTimeoutSec int) time.Duration {
	t.Helper()

	compose := filepath.Join(moduleRoot(t), "deploy", "compose.yaml")
	started := time.Now()
	stop := exec.CommandContext(
		t.Context(),
		"docker",
		"compose",
		"-f",
		compose,
		"stop",
		"-t",
		strconv.Itoa(stopTimeoutSec),
		service,
	)
	out, err := stop.CombinedOutput()
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("compose stop %s: %v %s", service, err, redactSecrets.ReplaceAllString(string(out), "[redacted]"))
	}

	start := exec.CommandContext(t.Context(), "docker", "compose", "-f", compose, "start", service)
	out, err = start.CombinedOutput()
	if err != nil {
		t.Fatalf("compose start %s: %v %s", service, err, redactSecrets.ReplaceAllString(string(out), "[redacted]"))
	}

	return elapsed
}
