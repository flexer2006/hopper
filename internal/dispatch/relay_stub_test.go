package dispatch_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/platform"
)

type stubJobs struct {
	pending    []dispatch.Intent
	healing    []dispatch.Intent
	expired    []string
	recovered  []string
	mu         sync.Mutex
	listErr    error
	healList   error
	leaseErr   error
	recoverErr error
	promoteErr error
	healErr    error
	markErr    error
	recoverOK  bool
}

func (s *stubJobs) MarkPublished(context.Context, string, int) error {
	return s.markErr
}

func (s *stubJobs) RecoverExpiredLease(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	s.recovered = append(s.recovered, id)
	err := s.recoverErr
	ok := s.recoverOK
	s.mu.Unlock()

	if err != nil {
		return false, err
	}

	return ok, nil
}

func (s *stubJobs) ListExpiredLeases(context.Context, int) ([]string, error) {
	return s.expired, s.leaseErr
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

func (s *stubJobs) snapshotRecovered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.recovered)
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

func TestNewRelayFromRequiresDeps(t *testing.T) {
	t.Parallel()

	if dispatch.NewRelayFrom(nil, new(recPub), new(platform.Config), nil) != nil {
		t.Fatal("NewRelayFrom without jobs must be nil")
	}

	if dispatch.NewRelayFrom(&stubJobs{}, nil, new(platform.Config), nil) != nil {
		t.Fatal("NewRelayFrom without publisher must be nil")
	}

	if dispatch.NewRelayFrom(&stubJobs{}, new(recPub), nil, nil) != nil {
		t.Fatal("NewRelayFrom without config must be nil")
	}

	cfg := new(platform.Config)
	cfg.RelayInterval = time.Second
	cfg.HealingInterval = 2 * time.Second
	cfg.LeaseScanInterval = 3 * time.Second

	got := dispatch.ConfigFrom(cfg)
	if got.Interval != time.Second || got.Healing != 2*time.Second || got.Lease != 3*time.Second {
		t.Fatalf("ConfigFrom() = %+v", got)
	}

	rel := dispatch.NewRelayFrom(&stubJobs{}, new(recPub), cfg, nil)
	if rel == nil {
		t.Fatal("NewRelayFrom with deps must be non-nil")
	}
}

func TestRelayBindStartStop(t *testing.T) {
	t.Parallel()

	rel := dispatch.NewRelay(&stubJobs{}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())
	hooks := rel.BindStart()

	err := hooks.Stop(t.Context())
	if err != nil {
		t.Fatalf("stop before start = %v", err)
	}

	err = hooks.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = hooks.Stop(t.Context())
	if err != nil {
		t.Fatal(err)
	}
}

func TestRelayHealErrorPaths(t *testing.T) {
	t.Parallel()

	item := dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs",
		Kind:       dispatch.IntentEnqueue,
		Generation: 2,
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

func TestRelayNotDueRetryDoesNotPublish(t *testing.T) {
	t.Parallel()

	item := dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs.delay.60s",
		Kind:       dispatch.IntentRetry,
		Generation: 2,
	}

	pub := new(recPub)
	rel := dispatch.NewRelay(&stubJobs{
		pending:    []dispatch.Intent{item},
		promoteErr: dispatch.ErrNotFound,
	}, pub, dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if pub.last().n != 0 {
		t.Fatalf("not-due retry published n=%d", pub.last().n)
	}
}

func TestRelayHealSkipsStaleGeneration(t *testing.T) {
	t.Parallel()

	item := dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs",
		Kind:       dispatch.IntentEnqueue,
		Generation: 2,
	}

	pub := new(recPub)
	rel := dispatch.NewRelay(&stubJobs{
		healing: []dispatch.Intent{item},
		healErr: dispatch.ErrStaleGeneration,
	}, pub, dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if pub.last().n != 0 {
		t.Fatalf("stale heal published n=%d", pub.last().n)
	}
}
