package fboot_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/fxx/fboot"
	"github.com/flexer2006/hopper/internal/platform"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewAppUsesPickedStopTimeoutOnValidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "api_shutdown_timeout: 6s\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	var got *platform.Config

	app := fboot.NewApp(
		10*time.Second,
		platform.APIStopTimeout,
		fx.Options(),
		fx.NopLogger,
		fx.Populate(&got),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err() = %v", err)
	}

	if app.StopTimeout() != 6*time.Second {
		t.Fatalf("StopTimeout() = %s, want 6s", app.StopTimeout())
	}

	if got == nil || got.APIShutdownTimeout != 6*time.Second {
		t.Fatalf("populated config = %+v", got)
	}
}

func TestNewAppFallsBackStopTimeoutAndSurfacesLoadError(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, "")

	app := fboot.NewApp(
		12*time.Second,
		platform.APIStopTimeout,
		fx.Options(),
		fx.NopLogger,
		fx.Invoke(func(*platform.Config) {}),
	)

	if app.StopTimeout() != 12*time.Second {
		t.Fatalf("StopTimeout() = %s, want fallback 12s", app.StopTimeout())
	}

	if !errors.Is(app.Err(), platform.ErrConfig) {
		t.Fatalf("app.Err() = %v, want ErrConfig", app.Err())
	}
}
