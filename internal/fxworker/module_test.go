package fxworker_test

import (
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/fxworker"
	"github.com/flexer2006/hopper/internal/platform"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewAppStartStop(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	err = platform.StartStop(t.Context(), fxworker.NewApp(fx.NopLogger))
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewAppStopTimeoutFromYAML(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.WorkerShutdownTimeoutEnv, "8s")

	app := fxworker.NewApp(fx.NopLogger)
	if app.StopTimeout() != 8*time.Second {
		t.Fatalf("StopTimeout = %s, want 8s", app.StopTimeout())
	}
}
