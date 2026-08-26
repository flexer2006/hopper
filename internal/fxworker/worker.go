package fxworker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/egress"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/worker"
)

type auxDLQ struct {
	pub *broker.Publisher
}

type workerLife struct {
	fx.In

	LC     fx.Lifecycle
	Log    *zap.Logger
	Holder *relayHolder
	Jobs   deliver.Jobs      `optional:"true"`
	Client deliver.HTTP      `optional:"true"`
	Pub    *broker.Publisher `optional:"true"`
	Source worker.Source     `optional:"true"`
}

const (
	claimLease     = 30 * time.Second
	httpTimeout    = 10 * time.Second
	outcomeTimeout = 5 * time.Second
	confirmTimeout = 5 * time.Second
	attemptBudget  = 25 * time.Second
)

func newHTTP() deliver.HTTP { //nolint:ireturn // fx provides the consumer-owned HTTP port
	return egress.New()
}

func (a *auxDLQ) Publish(ctx context.Context, body []byte) error {
	if a == nil || a.pub == nil {
		return worker.ErrAuxiliary
	}

	return a.pub.PublishDLQ(ctx, body)
}

func startWorker(in workerLife) error { //nolint:gocritic // hugeParam: fx.In composition
	err := persist.CheckLeaseBudget(claimLease, httpTimeout, outcomeTimeout, confirmTimeout)
	if err != nil {
		return fmt.Errorf("fr-46 lease budget: %w", err)
	}

	if in.Jobs == nil || in.Source == nil || in.Client == nil || in.Log == nil {
		return nil
	}

	var aux worker.AuxiliaryDLQ

	if in.Pub != nil {
		dlq := new(auxDLQ)
		dlq.pub = in.Pub
		aux = dlq
	}

	var relay worker.Relayer
	if in.Holder != nil && in.Holder.relay != nil {
		relay = in.Holder.relay
	}

	cfg := new(worker.Config)
	cfg.WorkerID = workerID()
	cfg.AttemptBudget = attemptBudget

	loop := worker.New(in.Jobs, in.Client, aux, relay, in.Log, *cfg)

	var (
		stop context.CancelFunc
		wg   sync.WaitGroup
	)

	in.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			//nolint:gosec // G118: cancel is stored in stop and invoked from OnStop.
			runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			stop = cancel

			wg.Go(func() {
				runErr := loop.Run(runCtx, in.Source)
				if runErr != nil && runCtx.Err() == nil {
					in.Log.Warn("worker run", zap.Error(runErr))
				}
			})

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if stop != nil {
				stop()
			}

			return waitDone(ctx, &wg)
		},
	})

	return nil
}

func workerID() string {
	const maxWorkerID = 128

	name, err := os.Hostname()
	if err != nil || name == "" {
		return "worker"
	}

	if len(name) > maxWorkerID {
		return name[:maxWorkerID]
	}

	return name
}

func waitDone(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
