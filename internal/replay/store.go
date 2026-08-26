package replay

import (
	"context"
	"errors"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
)

type Request struct {
	ID string
	By string
}

type Result struct {
	ID         string
	Status     domain.Status
	Cycle      int
	Generation int
	Accepted   bool
}

type Store interface {
	Replay(ctx context.Context, rec Request) (Result, error)
}

type Publisher interface {
	Publish(ctx context.Context, in dispatch.Intent) error
}

type Service struct {
	store Store
	pub   Publisher
}

var ErrInvalid = errors.New("invalid replay request")

func NewService(store Store, pub Publisher) *Service {
	svc := new(Service)
	svc.store = store
	svc.pub = pub

	return svc
}

func (s *Service) Replay(ctx context.Context, rec Request) (Result, error) {
	if rec.ID == "" {
		return Result{}, ErrInvalid
	}

	out, err := s.store.Replay(ctx, rec)
	if err != nil {
		return Result{}, err
	}

	if s.pub == nil {
		out.Accepted = false

		return out, nil
	}

	pubErr := s.pub.Publish(ctx, dispatch.Intent{
		ID:         out.ID,
		Queue:      domain.QueueJobs,
		Kind:       dispatch.IntentEnqueue,
		Generation: out.Generation,
	})
	if pubErr != nil {
		out.Accepted = false

		return out, nil //nolint:nilerr // 503 via Accepted
	}

	out.Accepted = true

	return out, nil
}
