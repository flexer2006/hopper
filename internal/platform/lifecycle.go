package platform

import (
	"context"
	"fmt"
	"os"
	"time"
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
	err := app.Err()
	if err != nil {
		return fmt.Errorf("%s fx graph: %w", name, err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), app.StartTimeout())
	defer startCancel()

	startErr := app.Start(startCtx)
	if startErr != nil {
		return fmt.Errorf("%s fx start: %w", name, startErr)
	}

	<-app.Done()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), app.StopTimeout())
	defer stopCancel()

	stopErr := app.Stop(stopCtx)
	if stopErr != nil {
		return fmt.Errorf("%s fx stop: %w", name, stopErr)
	}

	return nil
}
