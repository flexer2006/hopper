package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flexer2006/hopper/internal/domain"
)

type Job struct {
	CreatedAt, UpdatedAt                          time.Time
	Payload                                       json.RawMessage
	Attempts                                      []Attempt
	ReplayHistory                                 []ReplayEvent
	ID, Target                                    string
	Type                                          domain.JobType
	Status                                        domain.Status
	Cycle, AttemptsDone, MaxAttempts, ReplayCount int
}

type Attempt struct {
	At                                    time.Time
	Error, Outcome, FailureClass          string
	Cycle, Number, DurationMS, StatusCode int
}

type ReplayEvent struct {
	At                 time.Time
	By                 string
	FromCycle, ToCycle int
}

type Store interface {
	Get(ctx context.Context, id string) (Job, error)
	ListDead(ctx context.Context, limit int) ([]Job, error)
}

type Service struct{ store Store }

const DefaultListLimit = 50

func NewService(store Store) *Service {
	svc := new(Service)
	svc.store = store

	return svc
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) ListDead(ctx context.Context) ([]Job, error) {
	return s.store.ListDead(ctx, DefaultListLimit)
}
