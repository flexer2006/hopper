package query

import (
	"context"
	"time"

	"github.com/flexer2006/hopper/internal/domain"
)

type Job struct {
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ID           string
	Type         domain.JobType
	Target       string
	Status       domain.Status
	Cycle        int
	AttemptsDone int
	MaxAttempts  int
	ReplayCount  int
}

type Store interface {
	Get(ctx context.Context, id string) (Job, error)
}
