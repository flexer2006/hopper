package persist

import (
	"context"
	"sync"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

type mem struct {
	now  func() time.Time
	docs map[string]jobDoc
	keys map[string]string
	mu   sync.Mutex
}

func newMem(now func() time.Time) *mem {
	return &mem{
		now:  now,
		docs: make(map[string]jobDoc),
		keys: make(map[string]string),
		mu:   sync.Mutex{},
	}
}

func (m *mem) insert(_ context.Context, doc *jobDoc) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.docs[doc.ID]; ok {
		return ErrDuplicateKey
	}

	if _, ok := m.keys[doc.ProducerKey]; ok {
		return ErrDuplicateKey
	}

	m.docs[doc.ID] = *doc
	m.keys[doc.ProducerKey] = doc.ID

	return nil
}

func (m *mem) byID(_ context.Context, id string) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	return doc, nil
}

func (m *mem) byProducerKey(_ context.Context, key string) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, ok := m.keys[key]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	return m.docs[id], nil
}

func (m *mem) claimDue(_ context.Context, id, worker, fence string, lease time.Duration) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	now := m.now().UTC()
	if doc.Status != string(domain.StatusQueued) || !isDue(&doc, now) || !underStartCap(&doc) {
		return jobDoc{}, ErrNotFound
	}

	applyClaim(&doc, now, lease, fence, worker)
	m.docs[id] = doc

	return doc, nil
}

func (m *mem) capDead(_ context.Context, id string) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	now := m.now().UTC()
	if doc.Status != string(domain.StatusQueued) || !isDue(&doc, now) || underStartCap(&doc) {
		return jobDoc{}, ErrNotFound
	}

	applyCapDead(&doc, now)
	m.docs[id] = doc

	return doc, nil
}

//nolint:gocritic // hugeParam: deliver.OutcomeIn
func (m *mem) outcome(_ context.Context, in deliver.OutcomeIn) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[in.ID]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	if doc.Status != string(domain.StatusRunning) || doc.FenceToken != in.FenceToken || doc.Cycle != in.Cycle {
		return jobDoc{}, ErrStaleFence
	}

	applyOutcome(&doc, m.now().UTC(), in)
	m.docs[in.ID] = doc

	return doc, nil
}

func (m *mem) markPublished(_ context.Context, id string, generation int) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	if doc.Dispatch.Generation != generation || doc.Dispatch.Status != dispatchPending {
		return jobDoc{}, ErrStaleGeneration
	}

	applyMarkPublished(&doc, m.now().UTC())
	m.docs[id] = doc

	return doc, nil
}

func (m *mem) recoverLease(_ context.Context, id string) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	now := m.now().UTC()
	if !leaseExpired(&doc, now) {
		return jobDoc{}, ErrNotFound
	}

	applyLeaseRecover(&doc, now)
	m.docs[id] = doc

	return doc, nil
}

func (m *mem) replay(_ context.Context, id, by string) (jobDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return jobDoc{}, ErrNotFound
	}

	if doc.Status != string(domain.StatusDead) {
		return jobDoc{}, ErrReplayNotDead
	}

	if doc.ReplayCount >= domain.ReplayCap {
		return jobDoc{}, ErrReplayCap
	}

	applyReplay(&doc, m.now().UTC(), by)
	m.docs[id] = doc

	return doc, nil
}

func (m *mem) skipReason(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	doc, ok := m.docs[id]
	if !ok {
		return ErrNotFound
	}

	return classifySkip(&doc, m.now().UTC())
}
