package persist_test

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type frozenClock struct {
	mu sync.Mutex
	ts time.Time
}

const (
	testJobID  = "aaaaaaaaaaaaaaaaaaaaaaaa"
	testHash   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testTarget = "https://example.com/hook"
	testKey    = "idem-1"
	testWorker = "worker-1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newClock(t *testing.T) *frozenClock {
	t.Helper()

	return &frozenClock{ts: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)}
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

func newStore(t *testing.T, clk *frozenClock, lease time.Duration) *persist.Store {
	t.Helper()

	if clk == nil {
		clk = newClock(t)
	}

	return persist.NewMemory(clk.now, lease)
}

func testRecord(id, key string, maxAttempts int) enqueue.Record {
	return enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          id,
		Target:      testTarget,
		ProducerKey: key,
		RequestHash: testHash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: maxAttempts,
	}
}

//nolint:gocritic // hugeParam: enqueue.Record fixture
func mustInsert(t *testing.T, st *persist.Store, rec enqueue.Record) {
	t.Helper()

	err := st.Insert(t.Context(), rec)
	if err != nil {
		t.Fatalf("Insert() err = %v", err)
	}
}

func mustClaim(t *testing.T, st *persist.Store) deliver.ClaimOut {
	t.Helper()

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatalf("Claim() err = %v", err)
	}

	if out.DeadWithoutHTTP {
		t.Fatal("Claim() DeadWithoutHTTP")
	}

	return out
}

func TestInsertQueuedAndDuplicateKey(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	got, err := st.Get(t.Context(), testJobID)
	if err != nil {
		t.Fatalf("Get() err = %v", err)
	}

	if got.Status != domain.StatusQueued || got.Cycle != 0 || got.AttemptsDone != 0 {
		t.Fatalf("Get() = %+v", got)
	}

	existing, err := st.ByProducerKey(t.Context(), testKey)
	if err != nil {
		t.Fatalf("ByProducerKey() err = %v", err)
	}

	if existing.ID != testJobID || existing.DispatchStatus != "pending" || existing.RequestHash != testHash {
		t.Fatalf("ByProducerKey() = %+v", existing)
	}

	err = st.Insert(t.Context(), testRecord("cccccccccccccccccccccccc", testKey, 5))
	if !errors.Is(err, persist.ErrDuplicateKey) {
		t.Fatalf("duplicate Insert() err = %v, want ErrDuplicateKey", err)
	}

	again, err := st.Get(t.Context(), testJobID)
	if err != nil || again.ID != testJobID {
		t.Fatalf("original job missing after dup: %v %+v", err, again)
	}

	_, err = st.Get(t.Context(), "cccccccccccccccccccccccc")
	if !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("dup insert wrote second job: %v", err)
	}
}

func TestInsertValidationLeavesStoreEmpty(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	err := st.Insert(t.Context(), testRecord("not-hex", testKey, 5))
	if !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("Insert() err = %v, want ErrInvalidID", err)
	}

	_, err = st.Get(t.Context(), "not-hex")
	if !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("Get() after failed insert err = %v", err)
	}

	_, err = st.ByProducerKey(t.Context(), testKey)
	if !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("ByProducerKey() after failed insert err = %v", err)
	}

	oversize := testRecord(testJobID, testKey, 5)
	oversize.Payload = []byte(`[]`)
	err = st.Insert(t.Context(), oversize)
	if !errors.Is(err, persist.ErrPayload) {
		t.Fatalf("Insert() array payload err = %v, want ErrPayload", err)
	}
}

func TestClaimSuccessAndStaleFence(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	out := mustClaim(t, st)

	if out.Status != domain.StatusRunning || out.Attempt != 1 || out.FenceToken == "" {
		t.Fatalf("Claim() = %+v", out)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusRunning {
		t.Fatalf("Get() after claim = %+v %v", got, err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   "deadbeefdeadbeefdeadbeefdeadbeef",
		Status:       domain.StatusSucceeded,
		AttemptsDone: 1,
	})
	if !errors.Is(err, persist.ErrStaleFence) {
		t.Fatalf("stale fence err = %v", err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusSucceeded,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatalf("CommitOutcome() err = %v", err)
	}

	got, err = st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusSucceeded {
		t.Fatalf("Get() after success = %+v %v", got, err)
	}

	_, err = st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if !errors.Is(err, persist.ErrTerminal) {
		t.Fatalf("Claim() terminal err = %v", err)
	}
}

func TestSuccessOutcomeDoesNotBumpGeneration(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	err := st.MarkPublished(t.Context(), testJobID, 1)
	if err != nil {
		t.Fatalf("MarkPublished(1) err = %v", err)
	}

	out := mustClaim(t, st)
	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusSucceeded,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatalf("CommitOutcome() err = %v", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if !errors.Is(err, persist.ErrStaleGeneration) {
		t.Fatalf("MarkPublished(2) after success err = %v, want stale", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if !errors.Is(err, persist.ErrStaleGeneration) {
		t.Fatalf("MarkPublished(1) already published err = %v", err)
	}
}

func TestRetryOutcomeNotBeforeAndGenerationCAS(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	err := st.MarkPublished(t.Context(), testJobID, 1)
	if err != nil {
		t.Fatalf("MarkPublished(1) err = %v", err)
	}

	out := mustClaim(t, st)
	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.1s",
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatalf("CommitOutcome() err = %v", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if !errors.Is(err, persist.ErrStaleGeneration) {
		t.Fatalf("stale gen 1 err = %v", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if err != nil {
		t.Fatalf("MarkPublished(2) err = %v", err)
	}

	_, err = st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if !errors.Is(err, persist.ErrNotDue) {
		t.Fatalf("Claim() before due err = %v, want ErrNotDue", err)
	}

	clk.add(2 * time.Second)
	out = mustClaim(t, st)
	if out.Attempt != 2 {
		t.Fatalf("retry Claim() attempt = %d", out.Attempt)
	}
}

func TestClaimCompetingOneWinner(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	outs := make([]deliver.ClaimOut, n)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			outs[i], errs[i] = st.Claim(t.Context(), deliver.ClaimIn{
				ID:       testJobID,
				WorkerID: testWorker,
			})
		}(i)
	}
	wg.Wait()

	wins := 0
	for i := range n {
		if errs[i] == nil && !outs[i].DeadWithoutHTTP {
			wins++
			continue
		}
		if !errors.Is(errs[i], persist.ErrLeaseHeld) {
			t.Fatalf("loser err = %v, want ErrLeaseHeld", errs[i])
		}
	}

	if wins != 1 {
		t.Fatalf("winners = %d, want 1", wins)
	}
}

func TestDeliveryStartsCapDeadWithoutHTTP(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 1))

	for range 4 {
		_ = mustClaim(t, st)
		clk.add(2 * time.Second)
		ok, err := st.RecoverExpiredLease(t.Context(), testJobID)
		if err != nil || !ok {
			t.Fatalf("RecoverExpiredLease() ok=%v err=%v", ok, err)
		}
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if !errors.Is(err, persist.ErrDeliveryCap) || !out.DeadWithoutHTTP || out.Status != domain.StatusDead {
		t.Fatalf("cap Claim() out=%+v err=%v", out, err)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusDead {
		t.Fatalf("Get() after cap = %+v %v", got, err)
	}
}

func TestRecoverExpiredLeaseFakeClock(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	mustClaim(t, st)

	ok, err := st.RecoverExpiredLease(t.Context(), testJobID)
	if err != nil || ok {
		t.Fatalf("unexpired recover ok=%v err=%v", ok, err)
	}

	clk.add(31 * time.Second)
	ok, err = st.RecoverExpiredLease(t.Context(), testJobID)
	if err != nil || !ok {
		t.Fatalf("expired recover ok=%v err=%v", ok, err)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusQueued {
		t.Fatalf("Get() after lease recover = %+v %v", got, err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 1)
	if !errors.Is(err, persist.ErrStaleGeneration) {
		t.Fatalf("gen 1 after recover err = %v", err)
	}

	err = st.MarkPublished(t.Context(), testJobID, 2)
	if err != nil {
		t.Fatalf("MarkPublished(2) err = %v", err)
	}

	ok, err = st.RecoverExpiredLease(t.Context(), "dddddddddddddddddddddddd")
	if err != nil || ok {
		t.Fatalf("missing recover ok=%v err=%v", ok, err)
	}
}

func TestDeadOutcomeReplayAndCap(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	err := st.Replay(t.Context(), replay.Request{ID: testJobID, By: "ops"})
	if !errors.Is(err, persist.ErrReplayNotDead) {
		t.Fatalf("replay queued err = %v", err)
	}

	for range domain.ReplayCap {
		out := mustClaim(t, st)
		err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
			ID:           testJobID,
			FenceToken:   out.FenceToken,
			Status:       domain.StatusDead,
			AttemptsDone: 1,
			Cycle:        out.Cycle,
		})
		if err != nil {
			t.Fatalf("dead outcome err = %v", err)
		}
		err = st.Replay(t.Context(), replay.Request{ID: testJobID, By: "ops"})
		if err != nil {
			t.Fatalf("Replay() err = %v", err)
		}
	}

	out := mustClaim(t, st)
	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        domain.ReplayCap,
	})
	if err != nil {
		t.Fatalf("final dead err = %v", err)
	}

	err = st.Replay(t.Context(), replay.Request{ID: testJobID, By: "ops"})
	if !errors.Is(err, persist.ErrReplayCap) {
		t.Fatalf("Replay() at cap err = %v, want ErrReplayCap", err)
	}
}

func TestClaimValidationAndMissing(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	_, err := st.Claim(t.Context(), deliver.ClaimIn{WorkerID: testWorker})
	if !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("empty id err = %v", err)
	}

	_, err = st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: strings.Repeat("w", 129)})
	if !errors.Is(err, persist.ErrWorkerID) {
		t.Fatalf("long worker err = %v", err)
	}

	_, err = st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("missing claim err = %v", err)
	}

	_, err = st.Get(t.Context(), testJobID)
	if !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("missing get err = %v", err)
	}

	err = st.Replay(t.Context(), replay.Request{})
	if !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("empty replay id err = %v", err)
	}
}

func TestQueryJobTypeOmitsInternalFields(t *testing.T) {
	t.Parallel()

	rt := reflect.TypeFor[query.Job]()
	for field := range rt.Fields() {
		name := field.Name
		switch name {
		case "FenceToken", "Dispatch", "RequestHash", "ProducerKey", "Payload", "ClaimedBy", "ClaimExpiresAt":
			t.Fatalf("query.Job exposes %s (SEC-18)", name)
		}
	}
}

func TestCommitOutcomeWrongCycleAfterReplay(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	out := mustClaim(t, st)
	err := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatalf("dead outcome err = %v", err)
	}

	err = st.Replay(t.Context(), replay.Request{ID: testJobID, By: "ops"})
	if err != nil {
		t.Fatalf("Replay() err = %v", err)
	}

	out = mustClaim(t, st)
	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        0,
	})
	if !errors.Is(err, persist.ErrStaleFence) {
		t.Fatalf("omitted cycle after replay err = %v, want ErrStaleFence", err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatalf("matching cycle outcome err = %v", err)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Cycle != 1 || got.Status != domain.StatusDead {
		t.Fatalf("Get() after replay outcome = %+v %v", got, err)
	}
}

func TestCommitOutcomeRejectsRunning(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	err := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:         testJobID,
		FenceToken: "aa",
		Status:     domain.StatusRunning,
	})
	if !errors.Is(err, persist.ErrInvalidStatus) {
		t.Fatalf("running outcome err = %v", err)
	}
}

func TestStoreCloseNilSafe(t *testing.T) {
	t.Parallel()

	var st *persist.Store
	err := st.Close(t.Context())
	if err != nil {
		t.Fatalf("nil Close() err = %v", err)
	}

	err = newStore(t, nil, 0).Close(t.Context())
	if err != nil {
		t.Fatalf("memory Close() err = %v", err)
	}
}

func TestClaimEmptyWorker(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	_, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID})
	if !errors.Is(err, persist.ErrWorkerID) {
		t.Fatalf("empty worker err = %v", err)
	}

	err = st.Replay(t.Context(), replay.Request{ID: testJobID, By: strings.Repeat("b", 129)})
	if !errors.Is(err, persist.ErrWorkerID) {
		t.Fatalf("long replay by err = %v", err)
	}
}

func TestInsertMoreValidation(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(nil, 0)
	rec := testRecord(testJobID, testKey, 5)
	rec.RequestHash = "zz"
	err := st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrInvalidHash) {
		t.Fatalf("bad hash err = %v", err)
	}

	rec = testRecord(testJobID, "", 5)
	err = st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrInvalidKey) {
		t.Fatalf("empty key err = %v", err)
	}

	rec = testRecord(testJobID, strings.Repeat("k", 257), 5)
	err = st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrInvalidKey) {
		t.Fatalf("long key err = %v", err)
	}

	rec = testRecord(testJobID, testKey, 5)
	rec.Target = "not-a-url"
	err = st.Insert(t.Context(), rec)
	if err == nil {
		t.Fatal("invalid target inserted")
	}

	rec = testRecord(testJobID, testKey, 5)
	rec.Payload = nil
	err = st.Insert(t.Context(), rec)
	if err != nil {
		t.Fatalf("empty payload err = %v", err)
	}

	rec = testRecord("cccccccccccccccccccccccc", "idem-2", 5)
	rec.Payload = []byte(`{"x":"` + strings.Repeat("a", 262144) + `"}`)
	err = st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrPayload) {
		t.Fatalf("huge payload err = %v", err)
	}

	rec = testRecord("cccccccccccccccccccccccc", "idem-2", 5)
	rec.Payload = []byte(`{"n":`)
	err = st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrPayload) {
		t.Fatalf("truncated json err = %v", err)
	}
}

func TestClaimExpiredLeaseSkip(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	mustClaim(t, st)
	clk.add(2 * time.Second)

	_, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if !errors.Is(err, persist.ErrNotRunning) {
		t.Fatalf("expired running Claim() err = %v, want ErrNotRunning", err)
	}
}

func TestDuplicateJobID(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	err := st.Insert(t.Context(), testRecord(testJobID, "idem-other", 5))
	if !errors.Is(err, persist.ErrDuplicateKey) {
		t.Fatalf("dup id err = %v", err)
	}
}

func TestOutcomeRecordsAttempts(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	out := mustClaim(t, st)
	err := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		Attempts: []domain.Attempt{{
			At:         time.Now().UTC(),
			Outcome:    domain.OutcomeSuccess,
			Cycle:      0,
			Number:     1,
			DurationMS: 3,
			StatusCode: 200,
		}},
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusSucceeded,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatalf("CommitOutcome() err = %v", err)
	}
}

func TestCommitOutcomeEmptyFence(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	err := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:     testJobID,
		Status: domain.StatusSucceeded,
	})
	if !errors.Is(err, persist.ErrStaleFence) {
		t.Fatalf("empty fence err = %v", err)
	}
}

func TestHexRejectsUppercase(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, time.Second)
	rec := testRecord("AAAAAAAAAAAAAAAAaaaaaaaa", testKey, 5)
	err := st.Insert(t.Context(), rec)
	if !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("uppercase id err = %v", err)
	}
}
