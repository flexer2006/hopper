package deliver

import (
	"context"

	"github.com/flexer2006/hopper/internal/domain"
)

type ClaimIn struct{ ID, WorkerID string }

type ClaimOut struct {
	Payload                     []byte
	Attempts                    []domain.Attempt
	Target, FenceToken, ID      string
	Status                      domain.Status
	Cycle, Attempt, MaxAttempts int
}

type OutcomeIn struct {
	Attempts                          []domain.Attempt
	ID, FenceToken, Queue             string
	Status                            domain.Status
	DelaySeconds, AttemptsDone, Cycle int
}

type Jobs interface {
	Claim(ctx context.Context, in ClaimIn) (ClaimOut, error)
	CommitOutcome(ctx context.Context, in OutcomeIn) error
}
