package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (r *Relay) Run(ctx context.Context) error {
	grp, grpCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		return r.loop(grpCtx, r.interval, r.Tick)
	})
	grp.Go(func() error {
		return r.loop(grpCtx, r.lease, r.TickLeases)
	})

	err := grp.Wait()
	if err != nil {
		return fmt.Errorf("relay run: %w", err)
	}

	return nil
}

func (r *Relay) TickLeases(ctx context.Context) error {
	ids, err := r.jobs.ListExpiredLeases(ctx, r.limit)
	if err != nil {
		return fmt.Errorf("list expired leases: %w", err)
	}

	for i := range ids {
		ok, recErr := r.jobs.RecoverExpiredLease(ctx, ids[i])
		if recErr != nil {
			r.log.Warn("lease recover", zap.String("job_id", ids[i]), zap.Error(recErr))

			continue
		}

		if ok {
			r.log.Info("lease recovered",
				zap.String("job_id", ids[i]),
				zap.String("outcome", "queued"),
			)
		}
	}

	return nil
}

func (r *Relay) loop(ctx context.Context, period time.Duration, tick func(context.Context) error) error {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tickErr := tick(ctx)
			if tickErr != nil && !errors.Is(tickErr, context.Canceled) {
				r.log.Warn("relay tick", zap.Error(tickErr))
			}
		}
	}
}
