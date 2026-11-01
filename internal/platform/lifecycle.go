package platform

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/metrics"
	"runtime/pprof"
	"time"

	"golang.org/x/sys/unix"
)

type Graph interface {
	Err() error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type Process interface {
	Graph
	StartTimeout() time.Duration
	StopTimeout() time.Duration
	Done() <-chan os.Signal
}

type LeakReport struct {
	Profile string
	Count   int
}

const (
	goroutineMetricName = "/sched/goroutines:goroutines"
	goroutineLeakProf   = "goroutineleak"
)

var ErrLeakProfileMissing = errors.New("goroutineleak profile unavailable")

func ShutdownSignals() []os.Signal {
	return []os.Signal{unix.SIGTERM, unix.SIGINT}
}

func IsShutdownSignal(sig os.Signal) bool {
	return sig == unix.SIGTERM || sig == unix.SIGINT || sig == os.Interrupt
}

func NotifyShutdown(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, ShutdownSignals()...)
}

func StartStop(ctx context.Context, app Graph) error {
	err := app.Err()
	if err != nil {
		return err
	}

	err = app.Start(ctx)
	if err != nil {
		return err
	}

	return app.Stop(ctx)
}

func StartStopCycles(ctx context.Context, app Graph, n int) error {
	if n < 1 {
		n = 1
	}

	for cycle := range n {
		err := StartStop(ctx, app)
		if err != nil {
			return fmt.Errorf("cycle %d start/stop: %w", cycle, err)
		}
	}

	return nil
}

func RunProcess(name string, app Process) error {
	sigCtx, stop := NotifyShutdown(context.Background())
	defer stop()

	return RunProcessWithContext(sigCtx, name, app)
}

func RunProcessWithContext(ctx context.Context, name string, app Process) error {
	err := app.Err()
	if err != nil {
		return fmt.Errorf("%s fx graph: %w", name, err)
	}

	startCtx, startCancel := context.WithTimeout(context.WithoutCancel(ctx), app.StartTimeout())
	defer startCancel()

	startErr := app.Start(startCtx)
	if startErr != nil {
		return fmt.Errorf("%s fx start: %w", name, startErr)
	}

	select {
	case <-app.Done():
	case <-ctx.Done():
	}

	stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), app.StopTimeout())
	defer stopCancel()

	stopErr := app.Stop(stopCtx)
	if stopErr != nil {
		return fmt.Errorf("%s fx stop: %w", name, stopErr)
	}

	return nil
}

func GoroutineCount() uint64 {
	samples := [1]metrics.Sample{{Name: goroutineMetricName, Value: metrics.Value{}}}
	metrics.Read(samples[:])

	if samples[0].Value.Kind() == metrics.KindUint64 {
		return samples[0].Value.Uint64()
	}

	return 0
}

func DetectLeakedGoroutines() (LeakReport, error) {
	prof := pprof.Lookup(goroutineLeakProf)
	if prof == nil {
		return LeakReport{Profile: "", Count: 0}, ErrLeakProfileMissing
	}

	var buf bytes.Buffer

	err := prof.WriteTo(&buf, 1)
	if err != nil {
		return LeakReport{Profile: "", Count: 0}, fmt.Errorf("write goroutineleak profile: %w", err)
	}

	return LeakReport{
		Profile: buf.String(),
		Count:   prof.Count(),
	}, nil
}
