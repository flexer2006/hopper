package enqueue_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
)

type stubPub struct {
	err error
}

type recIntents struct {
	err error
	mu  sync.Mutex
	got []dispatch.Intent
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (s stubPub) Publish(context.Context, dispatch.Intent) error {
	return s.err
}

func (p *recIntents) Publish(_ context.Context, in dispatch.Intent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.got = append(p.got, in)

	return p.err
}

func (p *recIntents) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.got)
}

func rec(key, hash, target string) enqueue.Record {
	return enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		Target:      target,
		ProducerKey: key,
		RequestHash: hash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	}
}

func TestEnqueueInsertConfirm(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, stubPub{})

	out, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !out.Accepted || out.ID == "" {
		t.Fatalf("out=%+v err=%v", out, err)
	}

	again, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !again.Accepted || again.ID != out.ID {
		t.Fatalf("retry out=%+v err=%v", again, err)
	}
}

func TestEnqueueIdempotencyConflict(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, stubPub{})

	_, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.Enqueue(t.Context(), rec("k1", strings.Repeat("b", 64), "https://example.invalid/h"))
	if !errors.Is(err, enqueue.ErrIdempotencyConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestEnqueueConfirmFail(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, stubPub{err: errors.New("nack")})

	out, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || out.Accepted || out.ID == "" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEnqueueRejects(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, stubPub{})

	_, err := svc.Enqueue(t.Context(), rec("", strings.Repeat("a", 64), "https://example.invalid/h"))
	if !errors.Is(err, enqueue.ErrInvalid) {
		t.Fatalf("empty key err = %v", err)
	}

	_, err = svc.Enqueue(t.Context(), rec("k", "", "https://example.invalid/h"))
	if !errors.Is(err, enqueue.ErrInvalid) {
		t.Fatalf("empty hash err = %v", err)
	}

	_, err = svc.Enqueue(t.Context(), rec("k", strings.Repeat("a", 64), "http://127.0.0.1/h"))
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("loopback err = %v", err)
	}

	big := rec("k", strings.Repeat("a", 64), "https://example.invalid/h")
	big.Payload = []byte(strings.Repeat("x", 1<<20))
	_, err = svc.Enqueue(t.Context(), big)
	if !errors.Is(err, enqueue.ErrTooLarge) {
		t.Fatalf("too large err = %v", err)
	}
}

func TestEnqueueNilPublisher(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, nil)

	out, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || out.Accepted {
		t.Fatalf("nil pub out=%+v err=%v", out, err)
	}
}

func TestEnqueueDuplicatePendingRetryDoesNotPublishJobs(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	pub := new(recIntents)
	svc := enqueue.NewService(st, pub)

	first, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !first.Accepted {
		t.Fatalf("first out=%+v err=%v", first, err)
	}

	claimed, err := st.Claim(t.Context(), deliver.ClaimIn{ID: first.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           first.ID,
		FenceToken:   claimed.FenceToken,
		Queue:        "jobs.delay.60s",
		Status:       domain.StatusQueued,
		DelaySeconds: 60,
		AttemptsDone: 1,
		Cycle:        claimed.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	before := pub.count()
	again, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !again.Accepted || again.ID != first.ID {
		t.Fatalf("retry out=%+v err=%v", again, err)
	}

	if pub.count() != before {
		t.Fatalf("pending retry published n=%d before=%d", pub.count(), before)
	}
}

func TestEnqueuePendingConfirmRetryAfterFail(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	pub := &recIntents{err: errors.New("nack")}
	svc := enqueue.NewService(st, pub)

	first, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || first.Accepted || first.ID == "" {
		t.Fatalf("first out=%+v err=%v", first, err)
	}

	if pub.count() != 1 {
		t.Fatalf("failed confirm publishes = %d", pub.count())
	}

	pub.err = nil

	again, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !again.Accepted || again.ID != first.ID {
		t.Fatalf("retry out=%+v err=%v", again, err)
	}

	if pub.count() != 2 {
		t.Fatalf("re-confirm publishes = %d, want 2", pub.count())
	}
}

func TestEnqueuePublishedShortCircuit(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	pub := new(recIntents)
	svc := enqueue.NewService(st, pub)

	first, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !first.Accepted {
		t.Fatalf("first out=%+v err=%v", first, err)
	}

	err = st.MarkPublished(t.Context(), first.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	before := pub.count()
	again, err := svc.Enqueue(t.Context(), rec("k1", strings.Repeat("a", 64), "https://example.invalid/h"))
	if err != nil || !again.Accepted || again.ID != first.ID {
		t.Fatalf("published retry out=%+v err=%v", again, err)
	}

	if pub.count() != before {
		t.Fatalf("published short-circuit published n=%d before=%d", pub.count(), before)
	}
}
