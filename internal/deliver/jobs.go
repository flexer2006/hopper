package deliver

import (
	"context"

	"github.com/flexer2006/hopper/internal/domain"
)

type ClaimIn struct {
	ID       string
	WorkerID string
}

type ClaimOut struct {
	Payload     []byte
	Attempts    []domain.Attempt
	Target      string
	FenceToken  string
	ID          string
	Status      domain.Status
	Cycle       int
	Attempt     int
	MaxAttempts int
}

type OutcomeIn struct {
	Attempts     []domain.Attempt
	ID           string
	FenceToken   string
	Queue        string
	Status       domain.Status
	DelaySeconds int
	AttemptsDone int
	Cycle        int
}

type Jobs interface {
	Claim(ctx context.Context, in ClaimIn) (ClaimOut, error)
	CommitOutcome(ctx context.Context, in OutcomeIn) error
}
