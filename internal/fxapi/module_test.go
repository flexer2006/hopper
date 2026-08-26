package fxapi_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/fxapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type nopPublisher struct{}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func bindAPI(t *testing.T) {
	t.Helper()

	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.HTTPAddrEnv, "127.0.0.1:0")
}

func TestNewAppStartStop(t *testing.T) {
	bindAPI(t)
	t.Setenv(platform.HTTPAddrEnv, "127.0.0.1:0")

	err := platform.StartStop(t.Context(), fxapi.NewApp(fx.NopLogger))
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
	t.Setenv(platform.HTTPAddrEnv, "127.0.0.1:0")

	err = fxapi.NewApp(fx.NopLogger).Start(t.Context())
	if err == nil {
		t.Fatal("expected start error for short token")
	}
}

func TestNewAppStopTimeoutFromYAML(t *testing.T) {
	bindAPI(t)
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
	bindAPI(t)
	t.Setenv(platform.HTTPAddrEnv, "127.0.0.1:0")

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)

	err := platform.StartStop(t.Context(), fxapi.NewApp(
		fx.NopLogger,
		fx.Provide(func() dispatch.Jobs { return st }),
		fx.Provide(func() dispatch.Publisher { return nopPublisher{} }),
		fx.Provide(func() enqueue.Store { return st }),
		fx.Provide(func() query.Store { return st }),
		fx.Provide(func() replay.Store { return st }),
	))
	if err != nil {
		t.Fatal(err)
	}
}
