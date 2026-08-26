package dispatch_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
)

type recPub struct {
	err error
	mu  sync.Mutex
	got [][2]string
}

type pubHit struct {
	queue string
	id    string
	n     int
}

type frozenClock struct {
	mu sync.Mutex
	ts time.Time
}

type bumpPub struct {
	st *persist.Store
	mu sync.Mutex
	n  int
}

const (
	testJobID  = "aaaaaaaaaaaaaaaaaaaaaaaa"
	testHash   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testTarget = "https://example.com/hook"
	testKey    = "idem-relay"
	testWorker = "worker-1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (p *recPub) PublishJob(_ context.Context, queue, jobID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.got = append(p.got, [2]string{queue, jobID})

	return p.err
}

func (p *recPub) last() pubHit {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.got) == 0 {
		return pubHit{}
	}

	row := p.got[len(p.got)-1]

	return pubHit{queue: row[0], id: row[1], n: len(p.got)}
}

func newClock() *frozenClock {
	return &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
}

func (c *frozenClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ts
}

func (c *frozenClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ts = c.ts.Add(d)
}

func testRecord() enqueue.Record {
	return enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          testJobID,
		Target:      testTarget,
		ProducerKey: testKey,
		RequestHash: testHash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	}
}

func newRelay(t *testing.T, st *persist.Store, pub dispatch.Publisher) *dispatch.Relay {
	t.Helper()

	return dispatch.NewRelay(st, pub, dispatch.Config{
		Interval: time.Hour,
		Healing:  30 * time.Second,
		Limit:    dispatch.DefaultLimit,
	}, zap.NewNop())
}

func TestRelayPendingPublishMarksPublished(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 1 || hit.queue != "jobs" || hit.id != testJobID {
		t.Fatalf("publish = %s %s n=%d", hit.queue, hit.id, hit.n)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if !errors.Is(err, dispatch.ErrStaleGeneration) {
		t.Fatalf("second mark gen 1 err = %v, want stale", err)
	}
}

func TestRelayConfirmFailLeavesPending(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	pub := &recPub{err: errors.New("nack")}
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if err != nil {
		t.Fatalf("pending still markable err = %v", err)
	}
}

func TestRelayDueRetryPromotesToJobs(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.1s",
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	clk.add(2 * time.Second)

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 1 || hit.queue != "jobs" {
		t.Fatalf("promoted queue = %s n=%d, want jobs", hit.queue, hit.n)
	}
}

func TestRelayFutureRetryDoesNotPublishDelayQueue(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.1s",
		Status:       domain.StatusQueued,
		DelaySeconds: 60,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 0 {
		t.Fatalf("not-due retry published queue=%s n=%d", hit.queue, hit.n)
	}
}

func TestRelayHealsDuePublished(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if err != nil {
		t.Fatal(err)
	}

	clk.add(30 * time.Second)

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 1 || hit.queue != "jobs" {
		t.Fatalf("heal publish = %s n=%d", hit.queue, hit.n)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if !errors.Is(err, dispatch.ErrStaleGeneration) {
		t.Fatalf("heal marked gen 2 stale? err=%v", err)
	}
}

func TestRelayDoesNotHealFutureNotBefore(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.60s",
		Status:       domain.StatusQueued,
		DelaySeconds: 60,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if err != nil {
		t.Fatal(err)
	}

	clk.add(30 * time.Second)

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if hit := pub.last(); hit.n != 0 {
		t.Fatalf("healed early publishes=%d", hit.n)
	}
}

func (p *bumpPub) PublishJob(ctx context.Context, _, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.n++
	if p.n != 1 {
		return nil
	}

	out, err := p.st.Claim(ctx, deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		return err
	}

	return p.st.CommitOutcome(ctx, deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.1s",
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		AttemptsDone: 1,
	})
}

func TestRelayStaleConfirmKeepsNewerPending(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	rel := newRelay(t, st, &bumpPub{st: st})

	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if !errors.Is(err, dispatch.ErrStaleGeneration) {
		t.Fatalf("gen 1 after overlap err = %v, want stale", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if err != nil {
		t.Fatalf("gen 2 still pending err = %v", err)
	}
}

func TestRelayStartStopNoLeak(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	rel := newRelay(t, st, new(recPub))
	stop := rel.Start(t.Context())
	err := stop(t.Context())
	if err != nil {
		t.Fatal(err)
	}
}

func TestRelayPublishImmediate(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(newClock().now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	pub := new(recPub)
	rel := newRelay(t, st, pub)

	err = rel.Publish(t.Context(), dispatch.Intent{
		ID:         testJobID,
		Queue:      "jobs",
		Kind:       dispatch.IntentEnqueue,
		Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 1 || hit.queue != "jobs" {
		t.Fatalf("immediate = %s n=%d", hit.queue, hit.n)
	}
}

func TestNewRelayDefaults(t *testing.T) {
	t.Parallel()

	rel := dispatch.NewRelay(persist.NewMemory(newClock().now, time.Second), new(recPub), dispatch.Config{}, nil)
	if rel == nil {
		t.Fatal("NewRelay nil")
	}
}

func TestRelayTickSkipsNotDueRetry(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
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

	pub := new(recPub)
	rel := newRelay(t, st, pub)
	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if pub.last().n != 0 {
		t.Fatalf("not-due retry published = %+v", pub.last())
	}
}

func TestRelayTickPromotesDueRetry(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   claimed.FenceToken,
		Queue:        "jobs.delay.1s",
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		AttemptsDone: 1,
		Cycle:        claimed.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	clk.add(2 * time.Second)

	pub := new(recPub)
	rel := newRelay(t, st, pub)
	err = rel.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hit := pub.last()
	if hit.n != 1 || hit.queue != "jobs" {
		t.Fatalf("due retry publish = %+v", hit)
	}
}
