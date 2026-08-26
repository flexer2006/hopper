package persist

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type collection interface { //nolint:interfacebloat // persist test double mirrors mongoColl.
	insert(ctx context.Context, doc *jobDoc) error
	byID(ctx context.Context, id string) (jobDoc, error)
	byProducerKey(ctx context.Context, key string) (jobDoc, error)
	claimDue(ctx context.Context, id, worker, fence string, lease time.Duration) (jobDoc, error)
	capDead(ctx context.Context, id string) (jobDoc, error)
	outcome(ctx context.Context, in deliver.OutcomeIn) (jobDoc, error)
	markPublished(ctx context.Context, id string, generation int) (jobDoc, error)
	recoverLease(ctx context.Context, id string) (jobDoc, error)
	replay(ctx context.Context, id, by string) (jobDoc, error)
	skipReason(ctx context.Context, id string) error
	listPending(ctx context.Context, limit int) ([]jobDoc, error)
	listDueHealing(ctx context.Context, age time.Duration, limit int) ([]jobDoc, error)
	promoteDueRetry(ctx context.Context, id string, generation int) (jobDoc, error)
	startHealing(ctx context.Context, id string, generation int, age time.Duration) (jobDoc, error)
}

type closer interface {
	Disconnect(ctx context.Context) error
}

type Store struct {
	coll     collection
	now      func() time.Time
	newFence func() (string, error)
	client   closer
	lease    time.Duration
}

var (
	_ enqueue.Store = (*Store)(nil)
	_ query.Store   = (*Store)(nil)
	_ deliver.Jobs  = (*Store)(nil)
	_ dispatch.Jobs = (*Store)(nil)
	_ replay.Store  = (*Store)(nil)
)

func NewMemory(now func() time.Time, lease time.Duration) *Store {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	if lease <= 0 {
		lease = defaultLease
	}

	return &Store{
		coll:     newMem(now),
		now:      now,
		newFence: randomFence,
		client:   nil,
		lease:    lease,
	}
}

//nolint:gocritic // hugeParam: enqueue.Store
func (s *Store) Insert(ctx context.Context, rec enqueue.Record) error {
	doc, err := insertDoc(rec, s.now().UTC())
	if err != nil {
		return err
	}

	return s.coll.insert(ctx, &doc)
}

func (s *Store) ByProducerKey(ctx context.Context, key string) (enqueue.Existing, error) {
	doc, err := s.coll.byProducerKey(ctx, key)
	if err != nil {
		return enqueue.Existing{}, err
	}

	return doc.existing(), nil
}

func (s *Store) Get(ctx context.Context, id string) (query.Job, error) {
	doc, err := s.coll.byID(ctx, id)
	if err != nil {
		return query.Job{}, err
	}

	return doc.queryJob(), nil
}

func (s *Store) Claim(ctx context.Context, in deliver.ClaimIn) (deliver.ClaimOut, error) {
	err := validateClaim(in)
	if err != nil {
		return deliver.ClaimOut{}, err
	}

	fence, fenceErr := s.newFence()
	if fenceErr != nil {
		return deliver.ClaimOut{}, fenceErr
	}

	doc, err := s.coll.claimDue(ctx, in.ID, in.WorkerID, fence, s.lease)
	if err == nil {
		return runningClaim(&doc), nil
	}

	if !errors.Is(err, ErrNotFound) {
		return deliver.ClaimOut{}, err
	}

	return s.claimAfterMiss(ctx, in.ID)
}

//nolint:gocritic // hugeParam: deliver.Jobs
func (s *Store) CommitOutcome(ctx context.Context, in deliver.OutcomeIn) error {
	err := validateOutcome(in)
	if err != nil {
		return err
	}

	_, err = s.coll.outcome(ctx, in)

	return err
}

func (s *Store) MarkPublished(ctx context.Context, id string, generation int) error {
	_, err := s.coll.markPublished(ctx, id, generation)
	if errors.Is(err, ErrNotFound) {
		return dispatch.ErrNotFound
	}

	return err
}

func (s *Store) RecoverExpiredLease(ctx context.Context, id string) (bool, error) {
	_, err := s.coll.recoverLease(ctx, id)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	return false, err
}

func (s *Store) Replay(ctx context.Context, rec replay.Request) error {
	if rec.ID == "" {
		return ErrInvalidID
	}

	if len(rec.By) > maxReplayBy {
		return ErrWorkerID
	}

	_, err := s.coll.replay(ctx, rec.ID, rec.By)

	return err
}

func (s *Store) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}

	err := s.client.Disconnect(ctx)
	if err != nil {
		return fmt.Errorf("mongo disconnect: %w", err)
	}

	return nil
}

func (s *Store) claimAfterMiss(ctx context.Context, id string) (deliver.ClaimOut, error) {
	dead, err := s.coll.capDead(ctx, id)
	if err == nil {
		return capClaim(&dead), ErrDeliveryCap
	}

	if !errors.Is(err, ErrNotFound) {
		return deliver.ClaimOut{}, err
	}

	return deliver.ClaimOut{}, s.coll.skipReason(ctx, id)
}

func validateClaim(in deliver.ClaimIn) error {
	if in.ID == "" {
		return ErrInvalidID
	}

	if in.WorkerID == "" || len(in.WorkerID) > maxClaimedBy {
		return ErrWorkerID
	}

	return nil
}

//nolint:gocritic // hugeParam: deliver.OutcomeIn
func validateOutcome(in deliver.OutcomeIn) error {
	if in.ID == "" || in.FenceToken == "" {
		return ErrStaleFence
	}

	switch in.Status {
	case domain.StatusSucceeded, domain.StatusQueued, domain.StatusDead:
		return nil
	default:
		return ErrInvalidStatus
	}
}

func runningClaim(doc *jobDoc) deliver.ClaimOut {
	return deliver.ClaimOut{
		FenceToken:      doc.FenceToken,
		ID:              doc.ID,
		Status:          domain.Status(doc.Status),
		Cycle:           doc.Cycle,
		Attempt:         doc.AttemptsDone + 1,
		DeadWithoutHTTP: false,
	}
}

func capClaim(doc *jobDoc) deliver.ClaimOut {
	return deliver.ClaimOut{
		FenceToken:      "",
		ID:              doc.ID,
		Status:          domain.StatusDead,
		Cycle:           doc.Cycle,
		Attempt:         doc.AttemptsDone + 1,
		DeadWithoutHTTP: true,
	}
}

func randomFence() (string, error) {
	buf := make([]byte, fenceBytes)

	_, err := rand.Read(buf)
	if err != nil {
		return "", fmt.Errorf("fence token: %w", err)
	}

	return hex.EncodeToString(buf), nil
}
