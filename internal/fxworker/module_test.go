package fxworker_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/fxworker"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/worker"
)

type nopPublisher struct{}

type blockSource struct{}

type okHTTP struct{}

type ackDelivery struct {
	body []byte
	done chan struct{}
}

type onceSource struct {
	msg  worker.Delivery
	sent atomic.Bool
}

func (nopPublisher) PublishJob(context.Context, string, string) error {
	return nil
}

func (blockSource) Next(ctx context.Context) (worker.Delivery, error) { //nolint:ireturn // test fake Source
	<-ctx.Done()

	return nil, ctx.Err()
}

func (okHTTP) Post(context.Context, deliver.HTTPRequest) (deliver.HTTPResult, error) {
	return deliver.HTTPResult{StatusCode: http.StatusOK}, nil
}

func (d *ackDelivery) Body() []byte {
	return d.body
}

func (d *ackDelivery) Ack() error {
	select {
	case <-d.done:
	default:
		close(d.done)
	}

	return nil
}

func (s *onceSource) Next(ctx context.Context) (worker.Delivery, error) { //nolint:ireturn // test fake Source
	if s.sent.CompareAndSwap(false, true) {
		return s.msg, nil
	}

	<-ctx.Done()

	return nil, ctx.Err()
}

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

func TestNewAppConsumeLifecycle(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)

	err = platform.StartStop(t.Context(), fxworker.NewApp(
		fx.NopLogger,
		fx.Provide(func() dispatch.Jobs { return st }),
		fx.Provide(func() deliver.Jobs { return st }),
		fx.Provide(func() dispatch.Publisher { return nopPublisher{} }),
		fx.Provide(func() worker.Source { return blockSource{} }),
	))
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewAppConsumeWithoutPublisherAcks(t *testing.T) {
	path, err := platform.WriteTempConfig(t.TempDir(), platform.ValidToken())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	err = st.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          "aaaaaaaaaaaaaaaaaaaaaaaa",
		Target:      "https://example.com/hook",
		ProducerKey: "idem-fxworker",
		RequestHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := broker.MarshalEnqueue("aaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	src := &onceSource{msg: &ackDelivery{body: body, done: done}}
	app := fxworker.NewApp(
		fx.NopLogger,
		fx.Provide(func() deliver.Jobs { return st }),
		fx.Provide(func() worker.Source { return src }),
		fx.Decorate(func(deliver.HTTP) deliver.HTTP { return okHTTP{} }),
	)

	err = app.Err()
	if err != nil {
		t.Fatal(err)
	}

	err = app.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		stopErr := app.Stop(context.WithoutCancel(t.Context()))
		if stopErr != nil {
			t.Errorf("Stop() err = %v", stopErr)
		}
	})

	wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	select {
	case <-done:
	case <-wait.Done():
		t.Fatal("ack not observed")
	}

	got, err := st.Get(t.Context(), "aaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || got.Status != domain.StatusSucceeded {
		t.Fatalf("Get() = %+v err=%v", got, err)
	}
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

	err = platform.StartStop(t.Context(), fxworker.NewApp(
		fx.NopLogger,
		fx.Provide(func() dispatch.Jobs { return st }),
		fx.Provide(func() dispatch.Publisher { return nopPublisher{} }),
	))
	if err != nil {
		t.Fatal(err)
	}
}
