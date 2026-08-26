package enqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
)

type Service struct {
	store Store
	pub   Publisher
	newID func() (string, error)
}

const (
	idBytes           = 12
	maxProducerKey    = 256
	maxInsertEstimate = 1 << 20
	insertOverhead    = 4096
)

func NewService(store Store, pub Publisher) *Service {
	svc := new(Service)
	svc.store = store
	svc.pub = pub
	svc.newID = randomID

	return svc
}

//nolint:gocritic // hugeParam: enqueue.Record
func (s *Service) Enqueue(ctx context.Context, rec Record) (Result, error) {
	err := s.prepare(&rec)
	if err != nil {
		return Result{}, err
	}

	err = s.store.Insert(ctx, rec)
	if err == nil {
		return s.confirm(ctx, rec.ID, 1, domain.QueueJobs, dispatch.IntentEnqueue)
	}

	if !errors.Is(err, ErrDuplicateKey) {
		return Result{}, err
	}

	return s.existing(ctx, rec)
}

func (s *Service) prepare(rec *Record) error {
	if rec.ProducerKey == "" || len(rec.ProducerKey) > maxProducerKey {
		return fmt.Errorf("%w: idempotency key", ErrInvalid)
	}

	if rec.RequestHash == "" {
		return fmt.Errorf("%w: request hash", ErrInvalid)
	}

	if rec.ID == "" {
		id, err := s.newID()
		if err != nil {
			return err
		}

		rec.ID = id
	}

	if rec.Type == "" {
		rec.Type = domain.TypeHTTPPost
	}

	_, err := domain.AdmitTarget(rec.Target)
	if err != nil {
		return err
	}

	if len(rec.Payload)+len(rec.Target)+len(rec.ProducerKey)+insertOverhead > maxInsertEstimate {
		return ErrTooLarge
	}

	return nil
}

//nolint:gocritic // hugeParam: enqueue.Record
func (s *Service) existing(ctx context.Context, rec Record) (Result, error) {
	got, err := s.store.ByProducerKey(ctx, rec.ProducerKey)
	if err != nil {
		return Result{}, err
	}

	if got.RequestHash != rec.RequestHash {
		return Result{}, ErrIdempotencyConflict
	}

	if got.DispatchStatus == dispatch.StatusPublished {
		return Result{ID: got.ID, Accepted: true}, nil
	}

	if got.Kind == dispatch.IntentRetry {
		return Result{ID: got.ID, Accepted: true}, nil
	}

	return s.confirm(ctx, got.ID, got.Generation, got.Queue, got.Kind)
}

func (s *Service) confirm(ctx context.Context, id string, generation int, queue, kind string) (Result, error) {
	if s.pub == nil {
		return Result{ID: id, Accepted: false}, nil
	}

	if queue == "" {
		queue = domain.QueueJobs
	}

	if kind == "" {
		kind = dispatch.IntentEnqueue
	}

	err := s.pub.Publish(ctx, dispatch.Intent{
		ID:         id,
		Queue:      queue,
		Kind:       kind,
		Generation: generation,
	})
	if err != nil {
		return Result{ID: id, Accepted: false}, nil //nolint:nilerr // 503 via Accepted
	}

	return Result{ID: id, Accepted: true}, nil
}

func randomID() (string, error) {
	buf := make([]byte, idBytes)

	_, err := rand.Read(buf)
	if err != nil {
		return "", fmt.Errorf("%w: id: %w", ErrInvalid, err)
	}

	return hex.EncodeToString(buf), nil
}
