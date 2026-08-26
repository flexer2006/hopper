package dispatch

import (
	"context"
	"errors"
	"time"
)

type Intent struct {
	ID         string
	Queue      string
	Kind       string
	Generation int
	Due        bool
}

type Jobs interface {
	MarkPublished(ctx context.Context, id string, generation int) error
	RecoverExpiredLease(ctx context.Context, id string) (bool, error)
	ListPending(ctx context.Context, limit int) ([]Intent, error)
	ListDueHealing(ctx context.Context, age time.Duration, limit int) ([]Intent, error)
	PromoteDueRetry(ctx context.Context, id string, generation int) (Intent, error)
	StartHealing(ctx context.Context, id string, generation int, age time.Duration) (Intent, error)
}

type Publisher interface {
	PublishJob(ctx context.Context, queue, jobID string) error
}

const (
	IntentEnqueue = "enqueue"
	IntentRetry   = "retry"
	IntentDLQ     = "dlq"
)

var (
	ErrStaleGeneration = errors.New("stale dispatch generation")
	ErrNotFound        = errors.New("dispatch intent not found")
)
