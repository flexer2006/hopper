package platform_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/platform"
)

type stubGraph struct {
	err      error
	startErr error
	stopErr  error
	started  bool
	stopped  bool
}

type stubProcess struct {
	stubGraph

	startTo time.Duration
	stopTo  time.Duration
	done    chan os.Signal
}

func (s *stubGraph) Err() error { return s.err }

func (s *stubGraph) Start(context.Context) error {
	s.started = true

	return s.startErr
}

func (s *stubGraph) Stop(context.Context) error {
	s.stopped = true

	return s.stopErr
}

func (s *stubProcess) StartTimeout() time.Duration { return s.startTo }

func (s *stubProcess) StopTimeout() time.Duration { return s.stopTo }

func (s *stubProcess) Done() <-chan os.Signal { return s.done }

func TestStartStop(t *testing.T) {
	t.Parallel()

	graphErr := errors.New("graph")
	startErr := errors.New("start")
	stopErr := errors.New("stop")

	t.Run("ok", func(t *testing.T) {
		t.Parallel()

		app := new(stubGraph)
		err := platform.StartStop(t.Context(), app)
		if err != nil {
			t.Fatal(err)
		}

		if !app.started || !app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("graph error skips start", func(t *testing.T) {
		t.Parallel()

		app := new(stubGraph{err: graphErr})
		err := platform.StartStop(t.Context(), app)
		if !errors.Is(err, graphErr) {
			t.Fatalf("err = %v, want graph", err)
		}

		if app.started || app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("start error skips stop", func(t *testing.T) {
		t.Parallel()

		app := new(stubGraph{startErr: startErr})
		err := platform.StartStop(t.Context(), app)
		if !errors.Is(err, startErr) {
			t.Fatalf("err = %v, want start", err)
		}

		if !app.started || app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("stop error", func(t *testing.T) {
		t.Parallel()

		app := new(stubGraph{stopErr: stopErr})
		err := platform.StartStop(t.Context(), app)
		if !errors.Is(err, stopErr) {
			t.Fatalf("err = %v, want stop", err)
		}
	})
}

func TestWriteTempConfig(t *testing.T) {
	t.Parallel()

	token := platform.ValidToken()

	path, err := platform.WriteTempConfig(t.TempDir(), token)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := platform.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.APIToken != token {
		t.Fatalf("APIToken = %q", cfg.APIToken)
	}
}

func TestRunProcess(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()

		done := make(chan os.Signal)
		close(done)

		app := new(stubProcess{
			startTo: time.Second,
			stopTo:  time.Second,
			done:    done,
		})

		err := platform.RunProcess("api", app)
		if err != nil {
			t.Fatal(err)
		}

		if !app.started || !app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("graph error", func(t *testing.T) {
		t.Parallel()

		graphErr := errors.New("graph")
		app := new(stubProcess{
			err:     graphErr,
			startTo: time.Second,
			stopTo:  time.Second,
			done:    make(chan os.Signal),
		})

		err := platform.RunProcess("api", app)
		if !errors.Is(err, graphErr) {
			t.Fatalf("err = %v, want graph", err)
		}
	})
}
