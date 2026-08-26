package dispatch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/dispatch"
)

type stubJobs struct {
	pending    []dispatch.Intent
	healing    []dispatch.Intent
	listErr    error
	healList   error
	promoteErr error
	healErr    error
	markErr    error
}

func (s *stubJobs) MarkPublished(context.Context, string, int) error {
	return s.markErr
}

func (s *stubJobs) RecoverExpiredLease(context.Context, string) (bool, error) {
	return false, nil
}

func (s *stubJobs) ListPending(context.Context, int) ([]dispatch.Intent, error) {
	return s.pending, s.listErr
}

func (s *stubJobs) ListDueHealing(context.Context, time.Duration, int) ([]dispatch.Intent, error) {
	return s.healing, s.healList
}

func (s *stubJobs) PromoteDueRetry(context.Context, string, int) (dispatch.Intent, error) {
	return dispatch.Intent{}, s.promoteErr
}

func (s *stubJobs) StartHealing(context.Context, string, int, time.Duration) (dispatch.Intent, error) {
	if s.healErr != nil {
		return dispatch.Intent{}, s.healErr
	}

	if len(s.healing) == 0 {
		return dispatch.Intent{}, nil
	}

	return s.healing[0], nil
}

func TestRelayTickListErrors(t *testing.T) {
	t.Parallel()

	want := errors.New("list")
	rel := dispatch.NewRelay(&stubJobs{listErr: want}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.Tick(t.Context())
	if !errors.Is(err, want) {
		t.Fatalf("Tick() err = %v", err)
	}

	rel = dispatch.NewRelay(&stubJobs{healList: want}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())
	err = rel.Tick(t.Context())
	if !errors.Is(err, want) {
		t.Fatalf("Tick() heal list err = %v", err)
	}
}

func TestRelayHealErrorPaths(t *testing.T) {
	t.Parallel()

	item := dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs",
		Kind:       dispatch.IntentEnqueue,
		Generation: 2,
		Due:        true,
	}

	rel := dispatch.NewRelay(&stubJobs{
		healing: []dispatch.Intent{item},
		healErr: dispatch.ErrNotFound,
	}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	rel = dispatch.NewRelay(&stubJobs{
		healing: []dispatch.Intent{item},
		healErr: errors.New("mongo"),
	}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	rel = dispatch.NewRelay(&stubJobs{
		healing: []dispatch.Intent{item},
	}, &recPub{err: errors.New("nack")}, dispatch.Config{Limit: 8}, zap.NewNop())

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}
}

func TestRelayPromoteAndMarkSkip(t *testing.T) {
	t.Parallel()

	item := dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs.delay.1s",
		Kind:       dispatch.IntentRetry,
		Generation: 2,
		Due:        true,
	}

	rel := dispatch.NewRelay(&stubJobs{
		pending:    []dispatch.Intent{item},
		promoteErr: dispatch.ErrStaleGeneration,
	}, new(recPub), dispatch.Config{Limit: 4096}, zap.NewNop())

	err := rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}
}
