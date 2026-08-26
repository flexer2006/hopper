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
	Limit    int
}

type Relay struct {
	jobs     Jobs
	pub      Publisher
	log      *zap.Logger
	interval time.Duration
	healing  time.Duration
	limit    int
}

const (
	DefaultInterval = 2 * time.Second
	DefaultHealing  = 30 * time.Second
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

	if log == nil {
		log = zap.NewNop()
	}

	return &Relay{
		jobs:     jobs,
		pub:      pub,
		log:      log,
		interval: cfg.Interval,
		healing:  cfg.Healing,
		limit:    clampLimit(cfg.Limit),
	}
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

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tickErr := r.Tick(ctx)
			if tickErr != nil && !errors.Is(tickErr, context.Canceled) {
				r.log.Warn("relay tick", zap.Error(tickErr))
			}
		}
	}
}

func (r *Relay) Start(parent context.Context) func() {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))

	var wg sync.WaitGroup

	wg.Go(func() {
		err := r.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log.Warn("relay stop", zap.Error(err))
		}
	})

	return func() {
		cancel()
		wg.Wait()
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

	markErr := r.jobs.MarkPublished(ctx, target.ID, target.Generation)
	if skipIntent(markErr) {
		return nil
	}

	return markErr
}

func (r *Relay) prepare(ctx context.Context, in Intent) (Intent, error) {
	if in.Kind != IntentRetry || !in.Due {
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
