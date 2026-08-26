package dispatch

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Config struct {
	Interval time.Duration
	Healing  time.Duration
	Lease    time.Duration
	Limit    int
}

type Relay struct {
	jobs     Jobs
	pub      Publisher
	log      *zap.Logger
	interval time.Duration
	healing  time.Duration
	lease    time.Duration
	limit    int
}

const (
	DefaultInterval = 2 * time.Second
	DefaultHealing  = 30 * time.Second
	DefaultLease    = 5 * time.Second
	DefaultLimit    = 64
	maxLimit        = 256
)

func NewRelay(jobs Jobs, pub Publisher, cfg Config, log *zap.Logger) *Relay {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}

	if cfg.Healing <= 0 {
		cfg.Healing = DefaultHealing
	}

	if cfg.Lease <= 0 {
		cfg.Lease = DefaultLease
	}

	if log == nil {
		log = zap.NewNop()
	}

	rel := new(Relay)
	rel.jobs = jobs
	rel.pub = pub
	rel.log = log
	rel.interval = cfg.Interval
	rel.healing = cfg.Healing
	rel.lease = cfg.Lease
	rel.limit = clampLimit(cfg.Limit)

	return rel
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return DefaultLimit
	}

	if limit > maxLimit {
		return maxLimit
	}

	return limit
}

func (r *Relay) Tick(ctx context.Context) error {
	err := r.relayPending(ctx)
	if err != nil {
		return err
	}

	return r.healPublished(ctx)
}

func (r *Relay) Publish(ctx context.Context, in Intent) error {
	return r.publishOne(ctx, in)
}

func (r *Relay) Start(parent context.Context) func(context.Context) error {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))

	var wg sync.WaitGroup

	wg.Go(func() {
		err := r.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log.Warn("relay stop", zap.Error(err))
		}
	})

	return func(stopCtx context.Context) error {
		cancel()

		return waitDone(stopCtx, &wg)
	}
}

func (r *Relay) relayPending(ctx context.Context) error {
	items, err := r.jobs.ListPending(ctx, r.limit)
	if err != nil {
		return err
	}

	for i := range items {
		pubErr := r.publishOne(ctx, items[i])
		if pubErr != nil {
			r.logIntent("relay publish", items[i], pubErr)
		}
	}

	return nil
}

func (r *Relay) healPublished(ctx context.Context) error {
	items, err := r.jobs.ListDueHealing(ctx, r.healing, r.limit)
	if err != nil {
		return err
	}

	for i := range items {
		next, healErr := r.jobs.StartHealing(ctx, items[i].ID, items[i].Generation, r.healing)
		if skipIntent(healErr) {
			continue
		}

		if healErr != nil {
			r.logIntent("relay heal", items[i], healErr)

			continue
		}

		pubErr := r.publishOne(ctx, next)
		if pubErr != nil {
			r.logIntent("relay heal publish", next, pubErr)
		}
	}

	return nil
}

func (r *Relay) publishOne(ctx context.Context, in Intent) error {
	target, err := r.prepare(ctx, in)
	if skipIntent(err) {
		return nil
	}

	if err != nil {
		return err
	}

	pubErr := r.pub.PublishJob(ctx, target.Queue, target.ID)
	if pubErr != nil {
		return pubErr
	}

	return r.jobs.MarkPublished(ctx, target.ID, target.Generation)
}

func (r *Relay) prepare(ctx context.Context, in Intent) (Intent, error) {
	if in.Kind != IntentRetry {
		return in, nil
	}

	next, err := r.jobs.PromoteDueRetry(ctx, in.ID, in.Generation)
	if err != nil {
		return Intent{}, err
	}

	return next, nil
}

func skipIntent(err error) bool {
	return errors.Is(err, ErrStaleGeneration) || errors.Is(err, ErrNotFound)
}

func (r *Relay) logIntent(msg string, in Intent, err error) {
	r.log.Warn(msg,
		zap.String("job_id", in.ID),
		zap.Int("generation", in.Generation),
		zap.String("outcome", "pending"),
		zap.Error(err),
	)
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
