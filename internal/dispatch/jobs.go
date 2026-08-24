package dispatch

import "context"

type Jobs interface {
	MarkPublished(ctx context.Context, id string, generation int) error
	RecoverExpiredLease(ctx context.Context, id string) (bool, error)
}
