package worker_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/worker"
)

type frozenClock struct {
	mu sync.Mutex
	ts time.Time
}

type memDelivery struct {
	body   []byte
	ackErr error
	acked  atomic.Bool
}

type stubHTTP struct {
	err  error
	code int
	n    atomic.Int32
}

type stubAux struct {
	err    error
	mu     sync.Mutex
	bodies [][]byte
}

type stubRelay struct {
	err error
	n   atomic.Int32
}

type capJobs struct{}

type chanSource struct {
	ch chan worker.Delivery
}

type failCommit struct {
	*persist.Store
}

type skipJobs struct {
	err error
}

const (
	testJobID  = "aaaaaaaaaaaaaaaaaaaaaaaa"
	testHash   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testTarget = "https://example.com/hook"
	testKey    = "idem-1"
)

var (
	_ deliver.HTTP        = (*stubHTTP)(nil)
	_ worker.AuxiliaryDLQ = (*stubAux)(nil)
	_ worker.Relayer      = (*stubRelay)(nil)
	_ worker.Delivery     = (*memDelivery)(nil)
	_ worker.Source       = (*chanSource)(nil)
	_ deliver.Jobs        = capJobs{}
	_ deliver.Jobs        = skipJobs{}
	_ dispatch.Jobs       = (*persist.Store)(nil)
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (c *frozenClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ts
}

func (d *memDelivery) Body() []byte {
	return d.body
}

func (d *memDelivery) Ack() error {
	if d.ackErr != nil {
		return d.ackErr
	}

	d.acked.Store(true)

	return nil
}

func (h *stubHTTP) Post(context.Context, deliver.HTTPRequest) (deliver.HTTPResult, error) {
	h.n.Add(1)
	if h.err != nil {
		return deliver.HTTPResult{}, h.err
	}

	return deliver.HTTPResult{StatusCode: h.code}, nil
}

func (a *stubAux) Publish(_ context.Context, body []byte) error {
	if a.err != nil {
		return a.err
	}

	a.mu.Lock()
	a.bodies = append(a.bodies, append([]byte(nil), body...))
	a.mu.Unlock()

	return nil
}

func (r *stubRelay) Tick(context.Context) error {
	r.n.Add(1)
	return r.err
}

func (capJobs) Claim(context.Context, deliver.ClaimIn) (deliver.ClaimOut, error) {
	return deliver.ClaimOut{}, domain.ErrDeliveryCap
}

func (capJobs) CommitOutcome(context.Context, deliver.OutcomeIn) error {
	return errors.New("must not commit")
}

func (s *chanSource) Next(ctx context.Context) (worker.Delivery, error) { //nolint:ireturn // test fake Source
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case d := <-s.ch:
		return d, nil
	}
}

func (f *failCommit) CommitOutcome(context.Context, deliver.OutcomeIn) error {
	return errors.New("mongo down")
}

func newClock() *frozenClock {
	return &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
}

func newStore(clk *frozenClock) *persist.Store {
	return persist.NewMemory(clk.now, 30*time.Second)
}

func mustInsert(t *testing.T, st *persist.Store, maxAttempts int) {
	t.Helper()

	err := st.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          testJobID,
		Target:      testTarget,
		ProducerKey: testKey,
		RequestHash: testHash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("Insert() err = %v", err)
	}
}

func jobBody(t *testing.T) []byte {
	t.Helper()

	body, err := broker.MarshalEnqueue(testJobID)
	if err != nil {
		t.Fatalf("MarshalEnqueue() err = %v", err)
	}

	return body
}

func newWorker(
	t *testing.T,
	jobs deliver.Jobs,
	client deliver.HTTP,
	aux worker.AuxiliaryDLQ,
	relay worker.Relayer,
	clk *frozenClock,
	id string,
) *worker.Worker {
	t.Helper()

	return worker.New(jobs, client, aux, relay, nil, worker.Config{
		Now:      clk.now,
		WorkerID: id,
	})
}

func getJob(t *testing.T, st *persist.Store) query.Job {
	t.Helper()

	got, err := st.Get(t.Context(), testJobID)
	if err != nil {
		t.Fatalf("Get() err = %v", err)
	}

	return got
}

func TestProcessSuccessAcksAfterMongo(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	httpStub := &stubHTTP{code: http.StatusOK}
	relay := &stubRelay{}
	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, st, httpStub, nil, relay, clk, "w1")

	err := wkr.Process(t.Context(), deliv)
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	if httpStub.n.Load() != 1 || !deliv.acked.Load() || relay.n.Load() != 1 {
		t.Fatalf("posts=%d acked=%v ticks=%d", httpStub.n.Load(), deliv.acked.Load(), relay.n.Load())
	}

	got := getJob(t, st)
	if got.Status != domain.StatusSucceeded || got.AttemptsDone != 1 {
		t.Fatalf("job = %+v", got)
	}
}

func TestProcessRetryableDelayNotSleep(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	httpStub := &stubHTTP{code: http.StatusServiceUnavailable}
	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, st, httpStub, nil, &stubRelay{}, clk, "w1")

	err := wkr.Process(t.Context(), deliv)
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	got := getJob(t, st)
	if got.Status != domain.StatusQueued || got.AttemptsDone != 1 {
		t.Fatalf("job = %+v", got)
	}

	pending, err := st.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Queue != "jobs.delay.1s" {
		t.Fatalf("pending = %+v err=%v", pending, err)
	}
}

func TestProcess408And429Retryable(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			t.Parallel()

			clk := newClock()
			st := newStore(clk)
			id := testJobID
			key := testKey + http.StatusText(code)
			err := st.Insert(t.Context(), enqueue.Record{
				Payload:     []byte(`{"n":1}`),
				ID:          id,
				Target:      testTarget,
				ProducerKey: key,
				RequestHash: testHash,
				Type:        domain.TypeHTTPPost,
				MaxAttempts: 5,
			})
			if err != nil {
				t.Fatalf("Insert() err = %v", err)
			}

			wkr := newWorker(t, st, &stubHTTP{code: code}, nil, &stubRelay{}, clk, "w1")
			deliv := &memDelivery{body: jobBody(t)}
			err = wkr.Process(t.Context(), deliv)
			if err != nil {
				t.Fatalf("Process() err = %v", err)
			}

			got := getJob(t, st)
			if got.Status != domain.StatusQueued {
				t.Fatalf("status = %s for %d", got.Status, code)
			}
		})
	}
}

func TestProcess404FailFastDead(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	wkr := newWorker(t, st, &stubHTTP{code: http.StatusNotFound}, nil, &stubRelay{}, clk, "w1")
	deliv := &memDelivery{body: jobBody(t)}

	err := wkr.Process(t.Context(), deliv)
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	got := getJob(t, st)
	if got.Status != domain.StatusDead || got.AttemptsDone != 1 {
		t.Fatalf("job = %+v", got)
	}
}

func TestProcessExhaustRetryableDead(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 1)
	wkr := newWorker(t, st, &stubHTTP{code: http.StatusBadGateway}, nil, &stubRelay{}, clk, "w1")
	deliv := &memDelivery{body: jobBody(t)}

	err := wkr.Process(t.Context(), deliv)
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	got := getJob(t, st)
	if got.Status != domain.StatusDead {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestProcessSSRFAndThreeXX(t *testing.T) {
	t.Parallel()

	t.Run("blocked", func(t *testing.T) {
		t.Parallel()

		clk := newClock()
		st := newStore(clk)
		mustInsert(t, st, 5)
		wkr := newWorker(t, st, &stubHTTP{err: deliver.ErrBlocked}, nil, &stubRelay{}, clk, "w1")
		err := wkr.Process(t.Context(), &memDelivery{body: jobBody(t)})
		if err != nil {
			t.Fatalf("Process() err = %v", err)
		}

		if getJob(t, st).Status != domain.StatusDead {
			t.Fatal("want dead")
		}
	})

	t.Run("301", func(t *testing.T) {
		t.Parallel()

		clk := newClock()
		st := newStore(clk)
		err := st.Insert(t.Context(), enqueue.Record{
			Payload:     []byte(`{"n":1}`),
			ID:          testJobID,
			Target:      testTarget,
			ProducerKey: "idem-301",
			RequestHash: testHash,
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 5,
		})
		if err != nil {
			t.Fatalf("Insert() err = %v", err)
		}

		wkr := newWorker(t, st, &stubHTTP{code: http.StatusMovedPermanently}, nil, &stubRelay{}, clk, "w1")
		err = wkr.Process(t.Context(), &memDelivery{body: jobBody(t)})
		if err != nil {
			t.Fatalf("Process() err = %v", err)
		}

		if getJob(t, st).Status != domain.StatusDead {
			t.Fatal("want dead")
		}
	})
}

func TestProcessGhostMalformedDLQ(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	aux := &stubAux{}
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := newWorker(t, st, httpStub, aux, &stubRelay{}, clk, "w1")

	ghost := &memDelivery{body: jobBody(t)}
	err := wkr.Process(t.Context(), ghost)
	if err != nil {
		t.Fatalf("ghost Process() err = %v", err)
	}

	if !ghost.acked.Load() || httpStub.n.Load() != 0 || len(aux.bodies) != 1 {
		t.Fatalf("ghost acked=%v posts=%d dlq=%d", ghost.acked.Load(), httpStub.n.Load(), len(aux.bodies))
	}

	parsed, err := broker.ParseDLQ(aux.bodies[0])
	if err != nil || parsed.Reason != "missing_document" || parsed.JobID != testJobID {
		t.Fatalf("ghost dlq = %+v err=%v", parsed, err)
	}

	raw := []byte(`{`)
	malformed := &memDelivery{body: raw}
	err = wkr.Process(t.Context(), malformed)
	if err != nil {
		t.Fatalf("malformed Process() err = %v", err)
	}

	if !malformed.acked.Load() || len(aux.bodies) != 2 {
		t.Fatal("malformed not acked")
	}

	parsed, err = broker.ParseDLQ(aux.bodies[1])
	sum := sha256.Sum256(raw)
	if err != nil || parsed.Reason != "malformed_message" || parsed.JobID != "" ||
		parsed.BodySHA256 != hex.EncodeToString(sum[:]) || parsed.BodySize == nil || *parsed.BodySize != 1 {
		t.Fatalf("malformed dlq = %+v err=%v", parsed, err)
	}
}

func TestProcessAuxFailLeavesUnacked(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	aux := &stubAux{err: errors.New("nack")}
	wkr := newWorker(t, st, &stubHTTP{code: http.StatusOK}, aux, &stubRelay{}, clk, "w1")
	deliv := &memDelivery{body: []byte(`not-json`)}

	err := wkr.Process(t.Context(), deliv)
	if err == nil {
		t.Fatal("Process() err = nil")
	}

	if deliv.acked.Load() {
		t.Fatal("acked after aux nack")
	}
}

func TestProcessSkipHTTPOnTerminalAndNotBefore(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := newWorker(t, st, httpStub, nil, &stubRelay{}, clk, "w1")

	err := wkr.Process(t.Context(), &memDelivery{body: jobBody(t)})
	if err != nil {
		t.Fatalf("success Process() err = %v", err)
	}

	second := &memDelivery{body: jobBody(t)}
	err = wkr.Process(t.Context(), second)
	if err != nil {
		t.Fatalf("terminal Process() err = %v", err)
	}

	if httpStub.n.Load() != 1 || !second.acked.Load() {
		t.Fatalf("posts=%d acked=%v", httpStub.n.Load(), second.acked.Load())
	}

	clk2 := newClock()
	st2 := newStore(clk2)
	err = st2.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          testJobID,
		Target:      testTarget,
		ProducerKey: "idem-nb",
		RequestHash: testHash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Insert() err = %v", err)
	}

	http2 := &stubHTTP{code: http.StatusServiceUnavailable}
	wkr2 := newWorker(t, st2, http2, nil, &stubRelay{}, clk2, "w1")
	err = wkr2.Process(t.Context(), &memDelivery{body: jobBody(t)})
	if err != nil {
		t.Fatalf("503 Process() err = %v", err)
	}

	early := &memDelivery{body: jobBody(t)}
	err = wkr2.Process(t.Context(), early)
	if err != nil {
		t.Fatalf("early Process() err = %v", err)
	}

	if http2.n.Load() != 1 || !early.acked.Load() {
		t.Fatalf("not_before posts=%d acked=%v", http2.n.Load(), early.acked.Load())
	}

	pending, err := st2.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Queue != "jobs.delay.1s" {
		t.Fatalf("early ack mutated pending = %+v err=%v", pending, err)
	}

	_, err = st2.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: "late"})
	if !errors.Is(err, persist.ErrNotDue) {
		t.Fatalf("Claim() after early ack err = %v, want ErrNotDue", err)
	}
}

func TestProcessTwoWorkersOneClaim(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	httpStub := &stubHTTP{code: http.StatusOK}
	w1 := newWorker(t, st, httpStub, nil, &stubRelay{}, clk, "alpha")
	w2 := newWorker(t, st, httpStub, nil, &stubRelay{}, clk, "beta")
	d1 := &memDelivery{body: jobBody(t)}
	d2 := &memDelivery{body: jobBody(t)}

	var wg sync.WaitGroup

	wg.Go(func() {
		err := w1.Process(t.Context(), d1)
		if err != nil {
			t.Errorf("w1 err = %v", err)
		}
	})
	wg.Go(func() {
		err := w2.Process(t.Context(), d2)
		if err != nil {
			t.Errorf("w2 err = %v", err)
		}
	})
	wg.Wait()

	if httpStub.n.Load() != 1 {
		t.Fatalf("posts = %d, want 1", httpStub.n.Load())
	}

	if !d1.acked.Load() || !d2.acked.Load() {
		t.Fatal("both deliveries must ack")
	}
}

func TestProcessAckOnSkipClaims(t *testing.T) {
	t.Parallel()

	httpStub := &stubHTTP{code: http.StatusOK}
	for _, err := range []error{
		deliver.ErrNotRunning,
		deliver.ErrClaimLost,
		deliver.ErrLeaseHeld,
		deliver.ErrNotDue,
		deliver.ErrTerminal,
	} {
		deliv := &memDelivery{body: jobBody(t)}
		wkr := newWorker(t, skipJobs{err: err}, httpStub, nil, &stubRelay{}, newClock(), "w1")
		procErr := wkr.Process(t.Context(), deliv)
		if procErr != nil || !deliv.acked.Load() || httpStub.n.Load() != 0 {
			t.Fatalf("%v: err=%v acked=%v posts=%d", err, procErr, deliv.acked.Load(), httpStub.n.Load())
		}
	}
}

func TestProcessClaimUnknownErrorDoesNotAck(t *testing.T) {
	t.Parallel()

	want := errors.New("mongo unavailable")
	httpStub := &stubHTTP{code: http.StatusOK}
	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, skipJobs{err: want}, httpStub, nil, &stubRelay{}, newClock(), "w1")

	err := wkr.Process(t.Context(), deliv)
	if !errors.Is(err, want) {
		t.Fatalf("Process() err = %v, want mongo unavailable", err)
	}

	if deliv.acked.Load() || httpStub.n.Load() != 0 {
		t.Fatalf("unknown claim err acked=%v posts=%d", deliv.acked.Load(), httpStub.n.Load())
	}
}

func (s skipJobs) Claim(context.Context, deliver.ClaimIn) (deliver.ClaimOut, error) {
	return deliver.ClaimOut{}, s.err
}

func (skipJobs) CommitOutcome(context.Context, deliver.OutcomeIn) error {
	return errors.New("must not commit")
}

func TestProcessDeliveryCap(t *testing.T) {
	t.Parallel()

	httpStub := &stubHTTP{code: http.StatusOK}
	relay := &stubRelay{}
	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, capJobs{}, httpStub, nil, relay, newClock(), "w1")

	err := wkr.Process(t.Context(), deliv)
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	if httpStub.n.Load() != 0 || !deliv.acked.Load() || relay.n.Load() != 1 {
		t.Fatal("cap path must skip HTTP, tick, ack")
	}
}

func TestProcessCommitFailDoesNotAck(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	deliv := &memDelivery{body: jobBody(t)}
	failHTTP := &stubHTTP{code: http.StatusOK}
	wkr := worker.New(&failCommit{Store: st}, failHTTP, nil, &stubRelay{}, nil, worker.Config{
		Now:      clk.now,
		WorkerID: "w1",
	})

	err := wkr.Process(t.Context(), deliv)
	if err == nil || deliv.acked.Load() {
		t.Fatalf("err=%v acked=%v", err, deliv.acked.Load())
	}
}

func TestProcessTickFailStillAcks(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, st, &stubHTTP{code: http.StatusOK}, nil, &stubRelay{err: errors.New("confirm")}, clk, "w1")

	err := wkr.Process(t.Context(), deliv)
	if err != nil || !deliv.acked.Load() {
		t.Fatalf("err=%v acked=%v", err, deliv.acked.Load())
	}

	if getJob(t, st).Status != domain.StatusSucceeded {
		t.Fatal("mongo must be succeeded")
	}
}

func TestProcessUnknownJSONIsMalformed(t *testing.T) {
	t.Parallel()

	aux := &stubAux{}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, aux, &stubRelay{}, newClock(), "w1")
	body, err := json.Marshal(map[string]string{"job_id": testJobID, "extra": "x"})
	if err != nil {
		t.Fatal(err)
	}

	deliv := &memDelivery{body: body}
	err = wkr.Process(t.Context(), deliv)
	if err != nil || !deliv.acked.Load() || len(aux.bodies) != 1 {
		t.Fatalf("extra field must be malformed dlq")
	}
}

func TestProcessClipsLocalError(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	wkr := newWorker(t, st, &stubHTTP{err: errors.New(strings.Repeat("e", 2000))}, nil, &stubRelay{}, clk, "w1")
	err := wkr.Process(t.Context(), &memDelivery{body: jobBody(t)})
	if err != nil {
		t.Fatalf("Process() err = %v", err)
	}

	got := getJob(t, st)
	if len(got.Attempts) != 1 || len([]rune(got.Attempts[0].Error)) != 1024 {
		t.Fatalf("error runes = %d want 1024", len([]rune(got.Attempts[0].Error)))
	}
}

func TestProcessGhostAuxFailLeavesUnacked(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	aux := &stubAux{err: errors.New("nack")}
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := newWorker(t, st, httpStub, aux, &stubRelay{}, clk, "w1")
	deliv := &memDelivery{body: jobBody(t)}

	err := wkr.Process(t.Context(), deliv)
	if err == nil {
		t.Fatal("Process() err = nil")
	}

	if deliv.acked.Load() || httpStub.n.Load() != 0 {
		t.Fatalf("ghost aux fail acked=%v posts=%d", deliv.acked.Load(), httpStub.n.Load())
	}
}

func TestProcessNilAuxiliary(t *testing.T) {
	t.Parallel()

	deliv := &memDelivery{body: jobBody(t)}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, nil, &stubRelay{}, newClock(), "w1")

	err := wkr.Process(t.Context(), deliv)
	if !errors.Is(err, worker.ErrAuxiliary) {
		t.Fatalf("Process() err = %v, want ErrAuxiliary", err)
	}

	if deliv.acked.Load() {
		t.Fatal("acked without auxiliary")
	}
}

func TestProcessAckFailAfterSuccessThenRedelivery(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := newStore(clk)
	mustInsert(t, st, 5)
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := newWorker(t, st, httpStub, nil, &stubRelay{}, clk, "w1")
	first := &memDelivery{body: jobBody(t), ackErr: errors.New("channel")}

	err := wkr.Process(t.Context(), first)
	if err == nil || first.acked.Load() {
		t.Fatalf("ack fail err=%v acked=%v", err, first.acked.Load())
	}

	if httpStub.n.Load() != 1 {
		t.Fatalf("posts = %d, want 1", httpStub.n.Load())
	}

	got := getJob(t, st)
	if got.Status != domain.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}

	second := &memDelivery{body: jobBody(t)}
	err = wkr.Process(t.Context(), second)
	if err != nil || !second.acked.Load() {
		t.Fatalf("redelivery err=%v acked=%v", err, second.acked.Load())
	}

	if httpStub.n.Load() != 1 {
		t.Fatalf("second POST after mongo success posts=%d (AT-CRASH-02)", httpStub.n.Load())
	}
}

func TestProcessMalformedBodies(t *testing.T) {
	t.Parallel()

	bodies := [][]byte{
		{},
		[]byte(`{"job_id":"aa"}`),
		[]byte(`{"job_id":"` + testJobID + `"}0`),
	}

	for _, body := range bodies {
		aux := &stubAux{}
		httpStub := &stubHTTP{code: http.StatusOK}
		wkr := newWorker(t, newStore(newClock()), httpStub, aux, &stubRelay{}, newClock(), "w1")
		deliv := &memDelivery{body: body}

		err := wkr.Process(t.Context(), deliv)
		if err != nil || !deliv.acked.Load() || httpStub.n.Load() != 0 || len(aux.bodies) != 1 {
			t.Fatalf("body=%q err=%v acked=%v posts=%d dlq=%d",
				body, err, deliv.acked.Load(), httpStub.n.Load(), len(aux.bodies))
		}

		parsed, parseErr := broker.ParseDLQ(aux.bodies[0])
		sum := sha256.Sum256(body)
		if parseErr != nil || parsed.Reason != "malformed_message" || parsed.JobID != "" ||
			parsed.BodySHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("dlq = %+v err=%v", parsed, parseErr)
		}
	}
}

func TestRunCancel(t *testing.T) {
	t.Parallel()

	src := &chanSource{ch: make(chan worker.Delivery)}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, nil, &stubRelay{}, newClock(), "w1")
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- wkr.Run(ctx, src)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return")
	}
}
