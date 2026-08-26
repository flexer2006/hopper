package fxapi_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/fxapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
)

type nopPublisher struct{}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewAppStartStop(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	err = platform.StartStop(t.Context(), fxapi.NewApp(fx.NopLogger))
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewAppRejectsShortToken(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), "short")
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	err = fxapi.NewApp(fx.NopLogger).Start(t.Context())
	if err == nil {
		t.Fatal("expected start error for short token")
	}
}

func TestNewAppStopTimeoutFromYAML(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.APIShutdownTimeoutEnv, "5s")

	app := fxapi.NewApp(fx.NopLogger)
	if app.StopTimeout() != 5*time.Second {
		t.Fatalf("StopTimeout = %s, want 5s", app.StopTimeout())
	}
}

func (nopPublisher) PublishJob(context.Context, string, string) error {
	return nil
}

func TestNewAppRelayLifecycle(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)

	err = platform.StartStop(t.Context(), fxapi.NewApp(
		fx.NopLogger,
		fx.Provide(func() dispatch.Jobs { return st }),
		fx.Provide(func() dispatch.Publisher { return nopPublisher{} }),
	))
	if err != nil {
		t.Fatal(err)
	}
}
