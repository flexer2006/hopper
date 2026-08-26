package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flexer2006/hopper/internal/domain"
)

type Job struct {
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Payload       json.RawMessage
	Attempts      []Attempt
	ReplayHistory []ReplayEvent
	ID            string
	Type          domain.JobType
	Target        string
	Status        domain.Status
	Cycle         int
	AttemptsDone  int
	MaxAttempts   int
	ReplayCount   int
}

type Attempt struct {
	At           time.Time
	Error        string
	Outcome      string
	FailureClass string
	Cycle        int
	Number       int
	DurationMS   int
	StatusCode   int
}

type ReplayEvent struct {
	At        time.Time
	By        string
	FromCycle int
	ToCycle   int
}

type Store interface {
	Get(ctx context.Context, id string) (Job, error)
	ListDead(ctx context.Context, limit int) ([]Job, error)
}

type Service struct {
	store Store
}

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
