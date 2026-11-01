package platform_test

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sys/unix"

	"github.com/flexer2006/hopper/internal/platform"
)

type stubGraph struct {
	err, startErr, stopErr error
	started, stopped       bool
}

type stubProcess struct {
	stubGraph

	startTo, stopTo time.Duration
	done            chan os.Signal
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

func leakUnreachableGoroutine() {
	ch := make(chan struct{})
	<-ch
}

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

func TestStartStopCycles(t *testing.T) {
	t.Parallel()

	zeroApp := new(stubGraph)
	err := platform.StartStopCycles(t.Context(), zeroApp, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !zeroApp.started || !zeroApp.stopped {
		t.Fatalf("n=0 started=%v stopped=%v", zeroApp.started, zeroApp.stopped)
	}

	app := new(stubGraph)
	err = platform.StartStopCycles(t.Context(), app, 2)
	if err != nil {
		t.Fatal(err)
	}

	if !app.started || !app.stopped {
		t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
	}

	fail := new(stubGraph{startErr: errors.New("start")})
	err = platform.StartStopCycles(t.Context(), fail, 2)
	if err == nil {
		t.Fatal("want cycle error")
	}
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

	t.Run("start error", func(t *testing.T) {
		t.Parallel()

		startErr := errors.New("start boom")
		app := new(stubProcess{
			startErr: startErr,
			startTo:  time.Second,
			stopTo:   time.Second,
			done:     make(chan os.Signal),
		})

		err := platform.RunProcessWithContext(t.Context(), "api", app)
		if !errors.Is(err, startErr) {
			t.Fatalf("err = %v, want startErr", err)
		}

		if !app.started || app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("stop error", func(t *testing.T) {
		t.Parallel()

		done := make(chan os.Signal)
		close(done)

		stopErr := errors.New("stop boom")
		app := new(stubProcess{
			stopErr: stopErr,
			startTo: time.Second,
			stopTo:  time.Second,
			done:    done,
		})

		err := platform.RunProcessWithContext(t.Context(), "worker", app)
		if !errors.Is(err, stopErr) {
			t.Fatalf("err = %v, want stopErr", err)
		}

		if !app.started || !app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})

	t.Run("context cancel stops process cleanly", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		app := new(stubProcess{
			startTo: time.Second,
			stopTo:  time.Second,
			done:    make(chan os.Signal),
		})

		err := platform.RunProcessWithContext(ctx, "worker", app)
		if err != nil {
			t.Fatalf("RunProcessWithContext: %v", err)
		}

		if !app.started || !app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})
}

func TestStartStopCyclesSynctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		app := new(stubGraph)

		err := platform.StartStopCycles(t.Context(), app, 3)
		if err != nil {
			t.Fatalf("StartStopCycles: %v", err)
		}

		synctest.Wait()

		if !app.started || !app.stopped {
			t.Fatalf("started=%v stopped=%v", app.started, app.stopped)
		}
	})
}

func TestShutdownSignalsUnix(t *testing.T) {
	t.Parallel()

	sigs := platform.ShutdownSignals()
	if len(sigs) != 2 {
		t.Fatalf("len(ShutdownSignals) = %d, want 2", len(sigs))
	}

	if !platform.IsShutdownSignal(unix.SIGTERM) || !platform.IsShutdownSignal(unix.SIGINT) {
		t.Fatal("SIGTERM and SIGINT must be recognized shutdown signals")
	}

	if platform.IsShutdownSignal(unix.SIGUSR1) {
		t.Fatal("SIGUSR1 must not be recognized as a shutdown signal")
	}

	sigCtx, stop := platform.NotifyShutdown(t.Context())
	if sigCtx.Err() != nil {
		stop()
		t.Fatalf("NotifyShutdown initial Err() = %v, want nil", sigCtx.Err())
	}

	stop()

	if !errors.Is(sigCtx.Err(), context.Canceled) {
		t.Fatalf("NotifyShutdown after stop() Err() = %v, want context.Canceled", sigCtx.Err())
	}
}

func TestGoroutineLeakDiagnostics(t *testing.T) {
	t.Parallel()

	if platform.GoroutineCount() == 0 {
		t.Fatal("GoroutineCount() = 0, want > 0")
	}

	before, err := platform.DetectLeakedGoroutines()
	if err != nil {
		t.Fatalf("DetectLeakedGoroutines(before): %v", err)
	}

	if before.Count != 0 {
		t.Fatalf("initial leaked = %d, profile:\n%s", before.Count, before.Profile)
	}

	go leakUnreachableGoroutine()

	for range 20 {
		runtime.Gosched()
		runtime.GC()

		after, detectErr := platform.DetectLeakedGoroutines()
		if detectErr != nil {
			t.Fatalf("DetectLeakedGoroutines(after): %v", detectErr)
		}

		if after.Count > before.Count && strings.Contains(after.Profile, "leakUnreachableGoroutine") {
			return
		}

		time.Sleep(time.Millisecond)
	}

	after, err := platform.DetectLeakedGoroutines()
	if err != nil {
		t.Fatalf("DetectLeakedGoroutines(final): %v", err)
	}

	t.Fatalf("expected goroutineleak profile to detect leaked goroutine, got count=%d:\n%s", after.Count, after.Profile)
}
