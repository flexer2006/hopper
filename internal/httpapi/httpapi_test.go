package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type recBroker struct {
	err error
	n   int
	mu  sync.Mutex
}

type frozenClock struct {
	mu sync.Mutex
	ts time.Time
}

type errBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
	ID    string `json:"id"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

type idBody struct {
	ID string `json:"id"`
}

type apiFixture struct {
	h   http.Handler
	st  *persist.Store
	pub *recBroker
	clk *frozenClock
}

type staticCheck struct {
	err  error
	name string
}

type errReplayStore struct {
	err error
}

const (
	testTarget = "https://example.invalid/webhook"
	createJSON = `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"n":1}}`
	idemKey    = "order-1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (p *recBroker) PublishJob(_ context.Context, _, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.n++

	return p.err
}

func (p *recBroker) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.n
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

func newFixture(t *testing.T) apiFixture {
	t.Helper()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	st := persist.NewMemory(clk.now, 30*time.Second)
	broker := new(recBroker)
	rel := dispatch.NewRelay(st, broker, dispatch.Config{
		Interval: time.Hour,
		Healing:  30 * time.Second,
		Limit:    8,
	}, nil)
	h := httpapi.New(httpapi.Options{
		Now:             clk.now,
		Enqueue:         enqueue.NewService(st, rel),
		Query:           query.NewService(st),
		Replay:          replay.NewService(st, rel),
		Checks:          []httpapi.Checker{staticCheck{name: "mongo"}, staticCheck{name: "amqp"}},
		Token:           platform.ValidToken(),
		MaxRequestBytes: 512,
		MaxPayloadBytes: 256,
		JSONMaxDepth:    8,
		RateLimitRPM:    1000,
		RateLimitBurst:  100,
		TrustXFFHops:    0,
	})

	return apiFixture{h: h, st: st, pub: broker, clk: clk}
}

func (s staticCheck) Name() string { return s.name }

func (s staticCheck) Check(context.Context) error { return s.err }

func (s errReplayStore) Replay(context.Context, replay.Request) (replay.Result, error) {
	return replay.Result{}, s.err
}

func doReq(t *testing.T, h http.Handler, method, path, token, key, body string) *http.Response {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	res := rec.Result()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	closeErr := res.Body.Close()
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	res.Body = io.NopCloser(bytes.NewReader(raw))

	return res
}

func decodeErr(t *testing.T, res *http.Response) errBody {
	t.Helper()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	closeErr := res.Body.Close()
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	var body errBody

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}

	if body.Type != "" || body.Title != "" {
		t.Fatalf("rfc7807 fields present: %+v", body)
	}

	return body
}

func TestCreateJobAccepted(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer closeBody(t, res)

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}

	var body idBody

	err := json.NewDecoder(res.Body).Decode(&body)
	if err != nil {
		t.Fatal(err)
	}

	if len(body.ID) != 24 {
		t.Fatalf("id = %q", body.ID)
	}

	got, err := fx.st.Get(t.Context(), body.ID)
	if err != nil || got.Target != testTarget {
		t.Fatalf("stored = %+v %v", got, err)
	}

	existing, err := fx.st.ByProducerKey(t.Context(), idemKey)
	if err != nil || existing.DispatchStatus != "published" || fx.pub.count() != 1 {
		t.Fatalf("confirm published=%+v n=%d err=%v", existing, fx.pub.count(), err)
	}
}

func TestCreateJobIdempotentRetry(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	first := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer closeBody(t, first)

	var a idBody

	err := json.NewDecoder(first.Body).Decode(&a)
	if err != nil {
		t.Fatal(err)
	}

	second := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer closeBody(t, second)

	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("retry status = %d", second.StatusCode)
	}

	var b idBody

	err = json.NewDecoder(second.Body).Decode(&b)
	if err != nil {
		t.Fatal(err)
	}

	if a.ID != b.ID {
		t.Fatalf("ids %s vs %s", a.ID, b.ID)
	}

	if fx.pub.count() != 1 {
		t.Fatalf("duplicate published n=%d", fx.pub.count())
	}
}

func TestCreateJobIdempotencyConflict(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	first := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer func() {
		err := first.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	other := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"n":2}}`
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, other)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusConflict || got.Code != "idempotency_conflict" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobConfirmFail503(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	fx.pub.err = errors.New("nack")

	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusServiceUnavailable || got.Code != "service_unavailable" || got.ID == "" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}

	existing, err := fx.st.ByProducerKey(t.Context(), idemKey)
	if err != nil || existing.DispatchStatus != "pending" {
		t.Fatalf("failed confirm still pending=%+v err=%v", existing, err)
	}
}

func TestCreateJobMissingAuth(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", "", idemKey, createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusUnauthorized || got.Code != "unauthorized" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobWrongToken(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", strings.Repeat("b", 32), idemKey, createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusUnauthorized || got.Code != "unauthorized" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobMissingIdempotencyKey(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), "", createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobIdempotencyKeyTooLong(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	key := strings.Repeat("k", 257)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), key, createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}

	if fx.pub.count() != 0 {
		t.Fatalf("rejected key published n=%d", fx.pub.count())
	}
}

func TestCreateJobPendingRetryDoesNotRepublishJobs(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	first := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer closeBody(t, first)

	var body idBody

	err := json.NewDecoder(first.Body).Decode(&body)
	if err != nil {
		t.Fatal(err)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs.delay.60s",
		Status:       domain.StatusQueued,
		DelaySeconds: 60,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	before := fx.pub.count()
	second := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	defer closeBody(t, second)

	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("retry status = %d", second.StatusCode)
	}

	if fx.pub.count() != before {
		t.Fatalf("pending retry published n=%d before=%d", fx.pub.count(), before)
	}

	existing, err := fx.st.ByProducerKey(t.Context(), idemKey)
	if err != nil || existing.Kind != dispatch.IntentRetry || existing.Queue != "jobs.delay.60s" {
		t.Fatalf("pending retry existing=%+v err=%v", existing, err)
	}
}

func TestCreateJobLiteralIPDenied(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	body := `{"type":"http_post","target":"http://127.0.0.1/hook","payload":{}}`
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, body)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobUnknownField(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	body := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{},"fence_token":"x"}`
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, body)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobTooLarge(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	body := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"n":"` +
		strings.Repeat("x", 600) + `"}}`
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, body)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusRequestEntityTooLarge || got.Code != "payload_too_large" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobTooDeep(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	nested := strings.Repeat(`{"k":`, 10) + "1" + strings.Repeat("}", 10)
	body := `{"type":"http_post","target":"https://example.invalid/webhook","payload":` + nested + `}`
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, body)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("status=%d body=%+v", res.StatusCode, got)
	}
}

func TestGetAndListDead(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var body idBody

	err := json.NewDecoder(created.Body).Decode(&body)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	got := doReq(t, fx.h, http.MethodGet, "/v1/jobs/"+body.ID, platform.ValidToken(), "", "")
	defer closeBody(t, got)

	if got.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d", got.StatusCode)
	}

	raw, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(raw, []byte("fence_token")) || bytes.Contains(raw, []byte("producer_idempotency_key")) ||
		bytes.Contains(raw, []byte("request_hash")) || bytes.Contains(raw, []byte("dispatch")) {
		t.Fatalf("leaked internals: %s", raw)
	}

	badStatus := doReq(t, fx.h, http.MethodGet, "/v1/jobs?status=queued", platform.ValidToken(), "", "")
	decoded := decodeErr(t, badStatus)
	if badStatus.StatusCode != http.StatusBadRequest || decoded.Code != "validation_failed" {
		t.Fatalf("list filter status=%d body=%+v", badStatus.StatusCode, decoded)
	}

	list := doReq(t, fx.h, http.MethodGet, "/v1/jobs?status=dead", platform.ValidToken(), "", "")
	defer closeBody(t, list)

	if list.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", list.StatusCode)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadList := doReq(t, fx.h, http.MethodGet, "/v1/jobs?status=dead", platform.ValidToken(), "", "")
	defer closeBody(t, deadList)

	rawList, err := io.ReadAll(deadList.Body)
	if err != nil {
		t.Fatal(err)
	}

	if deadList.StatusCode != http.StatusOK || !bytes.Contains(rawList, []byte(body.ID)) {
		t.Fatalf("dead list status=%d body=%s", deadList.StatusCode, rawList)
	}

	if bytes.Contains(rawList, []byte("payload")) || bytes.Contains(rawList, []byte("fence_token")) {
		t.Fatalf("list leaked fields: %s", rawList)
	}
}

func TestGetJobNotFoundAndBadID(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	missing := doReq(t, fx.h, http.MethodGet, "/v1/jobs/aaaaaaaaaaaaaaaaaaaaaaaa", platform.ValidToken(), "", "")
	got := decodeErr(t, missing)
	if missing.StatusCode != http.StatusNotFound || got.Code != "not_found" {
		t.Fatalf("missing status=%d body=%+v", missing.StatusCode, got)
	}

	bad := doReq(t, fx.h, http.MethodGet, "/v1/jobs/ZZZ", platform.ValidToken(), "", "")
	got = decodeErr(t, bad)
	if bad.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("bad id status=%d body=%+v", bad.StatusCode, got)
	}
}

func TestReplayDeadAndConflict(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var body idBody

	err := json.NewDecoder(created.Body).Decode(&body)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	queued := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+body.ID+"/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, queued)
	if queued.StatusCode != http.StatusConflict || got.Code != "conflict" {
		t.Fatalf("replay queued status=%d body=%+v", queued.StatusCode, got)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	replayed := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+body.ID+"/replay", platform.ValidToken(), "", "")
	defer closeBody(t, replayed)

	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status = %d", replayed.StatusCode)
	}
}

func TestReplayNotFound404(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs/aaaaaaaaaaaaaaaaaaaaaaaa/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusNotFound || got.Code != "not_found" {
		t.Fatalf("missing replay status=%d body=%+v", res.StatusCode, got)
	}
}

func TestReplayInvalidID400(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs/ZZZ/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusBadRequest || got.Code != "validation_failed" {
		t.Fatalf("bad replay id status=%d body=%+v", res.StatusCode, got)
	}
}

func TestReplayStoreError503(t *testing.T) {
	t.Parallel()

	h := httpapi.New(httpapi.Options{
		Replay:         replay.NewService(errReplayStore{err: errors.New("mongo")}, nil),
		Token:          platform.ValidToken(),
		RateLimitRPM:   10,
		RateLimitBurst: 10,
	})

	res := doReq(t, h, http.MethodPost, "/v1/jobs/aaaaaaaaaaaaaaaaaaaaaaaa/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusServiceUnavailable || got.Code != "service_unavailable" {
		t.Fatalf("replay store err status=%d body=%+v", res.StatusCode, got)
	}
}

func TestReplayConfirmFail503(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var body idBody

	err := json.NewDecoder(created.Body).Decode(&body)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	fx.pub.err = errors.New("nack")

	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+body.ID+"/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusServiceUnavailable || got.Code != "service_unavailable" || got.ID != body.ID {
		t.Fatalf("replay confirm fail status=%d body=%+v", res.StatusCode, got)
	}
}

func TestReplayCapConflict409(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var body idBody

	err := json.NewDecoder(created.Body).Decode(&body)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	for range domain.ReplayCap {
		out, claimErr := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
		if claimErr != nil {
			t.Fatalf("Claim() err = %v", claimErr)
		}

		deadErr := fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
			ID:           body.ID,
			FenceToken:   out.FenceToken,
			Status:       domain.StatusDead,
			AttemptsDone: 1,
			Cycle:        out.Cycle,
		})
		if deadErr != nil {
			t.Fatalf("dead err = %v", deadErr)
		}

		_, replayErr := fx.st.Replay(t.Context(), replay.Request{ID: body.ID, By: "ops"})
		if replayErr != nil {
			t.Fatalf("Replay() err = %v", replayErr)
		}
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        domain.ReplayCap,
	})
	if err != nil {
		t.Fatal(err)
	}

	res := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+body.ID+"/replay", platform.ValidToken(), "", "")
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusConflict || got.Code != "conflict" {
		t.Fatalf("replay cap status=%d body=%+v (AT-UC05-03)", res.StatusCode, got)
	}
}

func TestGetJobIncludesReplayHistory(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var body idBody

	err := json.NewDecoder(created.Body).Decode(&body)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: body.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           body.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	replayed := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+body.ID+"/replay", platform.ValidToken(), "", "")
	closeBody(t, replayed)
	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status = %d", replayed.StatusCode)
	}

	got := doReq(t, fx.h, http.MethodGet, "/v1/jobs/"+body.ID, platform.ValidToken(), "", "")
	defer closeBody(t, got)

	raw, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}

	if got.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d body=%s", got.StatusCode, raw)
	}

	if bytes.Contains(raw, []byte("fence_token")) || bytes.Contains(raw, []byte("dispatch")) {
		t.Fatalf("leaked internals: %s", raw)
	}

	var view struct {
		ReplayHistory []struct {
			FromCycle int `json:"from_cycle"`
			ToCycle   int `json:"to_cycle"`
		} `json:"replay_history"`
		Status string `json:"status"`
		Cycle  int    `json:"cycle"`
	}

	err = json.Unmarshal(raw, &view)
	if err != nil {
		t.Fatal(err)
	}

	if view.Status != string(domain.StatusQueued) || view.Cycle != 1 {
		t.Fatalf("view = %+v", view)
	}

	if len(view.ReplayHistory) != 1 || view.ReplayHistory[0].FromCycle != 0 || view.ReplayHistory[0].ToCycle != 1 {
		t.Fatalf("replay_history = %+v", view.ReplayHistory)
	}
}

func TestHealthzUnauthenticated(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodGet, "/healthz", "", "", "")
	defer closeBody(t, res)

	if res.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", res.StatusCode)
	}

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(raw, []byte(platform.ValidToken())) {
		t.Fatal("token leaked")
	}

	down := httpapi.New(httpapi.Options{
		Token:          platform.ValidToken(),
		RateLimitRPM:   10,
		RateLimitBurst: 10,
	})
	res2 := doReq(t, down, http.MethodGet, "/healthz", "", "", "")
	defer closeBody(t, res2)

	if res2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing checkers status = %d", res2.StatusCode)
	}
}

func TestHealthzPartialDown503(t *testing.T) {
	t.Parallel()

	h := httpapi.New(httpapi.Options{
		Token:          platform.ValidToken(),
		RateLimitRPM:   10,
		RateLimitBurst: 10,
		Checks: []httpapi.Checker{
			staticCheck{name: "mongo"},
			staticCheck{name: "amqp", err: errors.New("broker down")},
		},
	})

	res := doReq(t, h, http.MethodGet, "/healthz", "", "", "")
	defer closeBody(t, res)

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("partial down status = %d body=%s", res.StatusCode, raw)
	}

	var body struct {
		Checks map[string]string `json:"checks"`
		Status string            `json:"status"`
	}

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatal(err)
	}

	if body.Status == "ok" || body.Checks["amqp"] != "down" || body.Checks["mongo"] != "up" {
		t.Fatalf("health = %+v (AT-UC11-02)", body)
	}
}

func TestCreateJobPayloadTooLargeUnderBodyCap(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	st := persist.NewMemory(clk.now, 30*time.Second)
	rel := dispatch.NewRelay(st, new(recBroker), dispatch.Config{
		Interval: time.Hour,
		Healing:  30 * time.Second,
		Limit:    8,
	}, nil)
	h := httpapi.New(httpapi.Options{
		Now:             clk.now,
		Enqueue:         enqueue.NewService(st, rel),
		Query:           query.NewService(st),
		Token:           platform.ValidToken(),
		MaxRequestBytes: 4096,
		MaxPayloadBytes: 32,
		JSONMaxDepth:    8,
		RateLimitRPM:    1000,
		RateLimitBurst:  100,
	})

	body := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"n":"` +
		strings.Repeat("x", 40) + `"}}`
	res := doReq(t, h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, body)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusRequestEntityTooLarge || got.Code != "payload_too_large" {
		t.Fatalf("payload cap status=%d body=%+v", res.StatusCode, got)
	}
}

func TestCreateJobBodyTooLarge413(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	st := persist.NewMemory(clk.now, 30*time.Second)
	rel := dispatch.NewRelay(st, new(recBroker), dispatch.Config{
		Interval: time.Hour,
		Healing:  30 * time.Second,
		Limit:    8,
	}, nil)
	h := httpapi.New(httpapi.Options{
		Now:             clk.now,
		Enqueue:         enqueue.NewService(st, rel),
		Token:           platform.ValidToken(),
		MaxRequestBytes: 64,
		MaxPayloadBytes: 256,
		JSONMaxDepth:    8,
		RateLimitRPM:    1000,
		RateLimitBurst:  100,
	})

	res := doReq(t, h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusRequestEntityTooLarge || got.Code != "payload_too_large" {
		t.Fatalf("body cap status=%d body=%+v (AT-UC10-03)", res.StatusCode, got)
	}
}

func TestRateLimitBeforeAuth(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	h := httpapi.New(httpapi.Options{
		Now:            clk.now,
		Token:          platform.ValidToken(),
		RateLimitRPM:   1,
		RateLimitBurst: 1,
		TrustXFFHops:   0,
	})

	first := doReq(t, h, http.MethodPost, "/v1/jobs", "", idemKey, createJSON)
	got := decodeErr(t, first)
	if first.StatusCode != http.StatusUnauthorized {
		t.Fatalf("first = %d %+v", first.StatusCode, got)
	}

	second := doReq(t, h, http.MethodPost, "/v1/jobs", "", idemKey, createJSON)
	got = decodeErr(t, second)
	if second.StatusCode != http.StatusTooManyRequests || got.Code != "rate_limited" {
		t.Fatalf("second = %d %+v", second.StatusCode, got)
	}
}

func TestXFFIgnoredWhenHopsZero(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	h := httpapi.New(httpapi.Options{
		Now:            clk.now,
		Token:          platform.ValidToken(),
		RateLimitRPM:   1,
		RateLimitBurst: 1,
		TrustXFFHops:   0,
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/jobs", strings.NewReader(createJSON))
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	closeBody(t, rec.Result())

	req2 := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/jobs", strings.NewReader(createJSON))
	req2.Header.Set("X-Forwarded-For", "198.51.100.8")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	res := rec2.Result()
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusTooManyRequests || got.Code != "rate_limited" {
		t.Fatalf("xff spoof status=%d body=%+v", res.StatusCode, got)
	}
}

func TestBearerSchemeCaseInsensitive(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/jobs", strings.NewReader(createJSON))
	req.Header.Set("Authorization", "bearer "+platform.ValidToken())
	req.Header.Set("Idempotency-Key", idemKey)
	rec := httptest.NewRecorder()
	fx.h.ServeHTTP(rec, req)
	res := rec.Result()
	defer closeBody(t, res)

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func closeBody(t *testing.T, res *http.Response) {
	t.Helper()

	if res == nil || res.Body == nil {
		return
	}

	err := res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
}
