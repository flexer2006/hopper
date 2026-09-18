//go:build integration

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/worker"
)

func TestLiveATLEASE03ExpiredLeaseScanRecover(t *testing.T) {
	cfg := requireCompose(t)

	collection := fmt.Sprintf("jobs_hop13_%d", time.Now().UnixNano())
	store := openLeaseStore(t, cfg, collection, time.Millisecond)
	defer store.Close(t.Context())

	jobID := randomJobID(t)
	insertLeaseJob(t, store, jobID)

	_, err := store.Claim(t.Context(), deliver.ClaimIn{ID: jobID, WorkerID: "hop13-lease"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ids, err := waitExpiredLeases(ctx, store, jobID)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 1 || ids[0] != jobID {
		t.Fatalf("expired ids = %v, want [%s]", ids, jobID)
	}

	ok, err := store.RecoverExpiredLease(t.Context(), jobID)
	if err != nil || !ok {
		t.Fatalf("RecoverExpiredLease() ok=%v err=%v", ok, err)
	}

	job, err := store.Get(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	if job.Status != domain.StatusQueued {
		t.Fatalf("status = %s, want queued after recover", job.Status)
	}
}

func TestLiveDualScannerRecoverCAS(t *testing.T) {
	cfg := requireCompose(t)

	collection := fmt.Sprintf("jobs_hop13_%d", time.Now().UnixNano())
	store := openLeaseStore(t, cfg, collection, time.Millisecond)
	defer store.Close(t.Context())

	jobID := randomJobID(t)
	insertLeaseJob(t, store, jobID)

	_, err := store.Claim(t.Context(), deliver.ClaimIn{ID: jobID, WorkerID: "hop13-dual"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err = waitExpiredLeases(ctx, store, jobID)
	if err != nil {
		t.Fatal(err)
	}

	left := dispatch.NewRelay(store, nopPublisher{}, dispatch.Config{Limit: 8}, zap.NewNop())
	right := dispatch.NewRelay(store, nopPublisher{}, dispatch.Config{Limit: 8}, zap.NewNop())

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		tickErr := left.TickLeases(t.Context())
		if tickErr != nil {
			t.Error(tickErr)
		}
	}()

	go func() {
		defer wg.Done()

		tickErr := right.TickLeases(t.Context())
		if tickErr != nil {
			t.Error(tickErr)
		}
	}()

	wg.Wait()

	job, err := store.Get(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	if job.Status != domain.StatusQueued {
		t.Fatalf("status = %s, want queued after dual TickLeases", job.Status)
	}

	pending, err := store.ListPending(t.Context(), 8)
	if err != nil {
		t.Fatal(err)
	}

	found := 0

	for _, item := range pending {
		if item.ID == jobID {
			found++
		}
	}

	if found != 1 {
		t.Fatalf("pending intents for job = %d, want 1", found)
	}

	ok, err := store.RecoverExpiredLease(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal("loser CAS expected skip after dual TickLeases")
	}
}

func TestLiveRecoverPendingRelayPublish(t *testing.T) {
	cfg := requireCompose(t)

	collection := fmt.Sprintf("jobs_hop13_%d", time.Now().UnixNano())
	store := openLeaseStore(t, cfg, collection, time.Millisecond)
	defer store.Close(t.Context())

	jobID := randomJobID(t)
	insertLeaseJob(t, store, jobID)

	_, err := store.Claim(t.Context(), deliver.ClaimIn{ID: jobID, WorkerID: "hop13-relay"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err = waitExpiredLeases(ctx, store, jobID)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := store.RecoverExpiredLease(t.Context(), jobID)
	if err != nil || !ok {
		t.Fatalf("recover ok=%v err=%v", ok, err)
	}

	pending, err := store.ListPending(t.Context(), 8)
	if err != nil {
		t.Fatal(err)
	}

	found := false

	for _, item := range pending {
		if item.ID == jobID {
			found = true

			break
		}
	}

	if !found {
		t.Fatal("recovered job missing from ListPending")
	}

	conn, ch := openAMQP(t, cfg)
	defer conn.Close()
	defer ch.Close()

	pub := broker.PublisherFromChannel(ch, broker.DefaultConfirmTimeout)
	relay := dispatch.NewRelay(store, pub, dispatch.Config{Limit: 8}, zap.NewNop())

	err = relay.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	pending, err = store.ListPending(t.Context(), 8)
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range pending {
		if item.ID == jobID {
			t.Fatal("job still pending after relay tick")
		}
	}

	err = waitEnqueueOrGhost(ctx, ch, jobID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLiveATRELAY03OrphanHeal(t *testing.T) {
	cfg := requireCompose(t)

	collection := fmt.Sprintf("jobs_hop13_heal_%d", time.Now().UnixNano())
	store := openLeaseStore(t, cfg, collection, time.Millisecond)
	defer store.Close(t.Context())

	jobID := randomJobID(t)
	insertLeaseJob(t, store, jobID)

	err := store.MarkPublished(t.Context(), jobID, 1)
	if err != nil {
		t.Fatal(err)
	}

	conn, ch := openAMQP(t, cfg)
	defer conn.Close()
	defer ch.Close()

	pub := broker.PublisherFromChannel(ch, broker.DefaultConfirmTimeout)
	relay := dispatch.NewRelay(store, pub, dispatch.Config{
		Limit:   8,
		Healing: time.Millisecond,
	}, zap.NewNop())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err = waitDueHealing(ctx, store, jobID, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	err = relay.Tick(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = waitEnqueueOrGhost(ctx, ch, jobID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLiveATUC0801TerminalRedeliveryAck(t *testing.T) {
	cfg := requireCompose(t)

	collection := fmt.Sprintf("jobs_hop13_uc08_%d", time.Now().UnixNano())
	store := openLeaseStore(t, cfg, collection, 5*time.Second)
	defer store.Close(t.Context())

	jobID := randomJobID(t)
	insertLeaseJob(t, store, jobID)

	claimed, err := store.Claim(t.Context(), deliver.ClaimIn{ID: jobID, WorkerID: "hop13-uc08-setup"})
	if err != nil {
		t.Fatal(err)
	}

	err = store.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           jobID,
		FenceToken:   claimed.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := broker.MarshalEnqueue(jobID)
	if err != nil {
		t.Fatal(err)
	}

	http := &countHTTP{}
	msg := &ackDelivery{body: body}
	proc := worker.New(store, http, nopAux{}, nil, zap.NewNop(), worker.Config{WorkerID: "hop13-uc08"})

	err = proc.Process(t.Context(), msg)
	if err != nil {
		t.Fatal(err)
	}

	if http.n.Load() != 0 {
		t.Fatalf("HTTP posts = %d, want 0", http.n.Load())
	}

	if !msg.acked.Load() {
		t.Fatal("terminal redelivery was not acked")
	}

	after, err := store.Get(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	if after.Status != domain.StatusDead || after.AttemptsDone != 1 {
		t.Fatalf("status=%s attempts_done=%d, want dead/1", after.Status, after.AttemptsDone)
	}
}

type countHTTP struct {
	n atomic.Int32
}

func (c *countHTTP) Post(context.Context, deliver.HTTPRequest) (deliver.HTTPResult, error) {
	c.n.Add(1)

	return deliver.HTTPResult{StatusCode: 200}, nil
}

type ackDelivery struct {
	body  []byte
	acked atomic.Bool
}

func (d *ackDelivery) Body() []byte {
	return d.body
}

func (d *ackDelivery) Ack() error {
	d.acked.Store(true)

	return nil
}

type nopAux struct{}

func (nopAux) Publish(context.Context, []byte) error {
	return nil
}

func openLeaseStore(t *testing.T, cfg labConfig, collection string, lease time.Duration) *persist.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	store, err := persist.Open(ctx, persist.Options{
		URI:        cfg.MongoURI,
		Database:   cfg.MongoDB,
		Collection: collection,
		Lease:      lease,
	})
	if err != nil {
		t.Fatal("mongo unreachable (backend is internal; run the host operator integration script in the hopper-api netns): " + redactError(err).Error())
	}

	return store
}

func insertLeaseJob(t *testing.T, store *persist.Store, jobID string) {
	t.Helper()

	rec := enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          jobID,
		Target:      "https://example.com/",
		ProducerKey: fmt.Sprintf("hop13-%s-%d", jobID, time.Now().UnixNano()),
		RequestHash: strings.Repeat("a", 64),
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 3,
	}

	err := store.Insert(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
}

func waitNotPending(ctx context.Context, store *persist.Store, jobID string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		pending, err := store.ListPending(ctx, 256)
		if err != nil {
			return err
		}

		found := false

		for _, item := range pending {
			if item.ID == jobID {
				found = true

				break
			}
		}

		if !found {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitDueHealing(ctx context.Context, store *persist.Store, jobID string, age time.Duration) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		due, err := store.ListDueHealing(ctx, age, 8)
		if err != nil {
			return err
		}

		for _, item := range due {
			if item.ID == jobID {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitExpiredLeases(ctx context.Context, store *persist.Store, want string) ([]string, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		ids, err := store.ListExpiredLeases(ctx, 8)
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			if id == want {
				return ids, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func randomJobID(t *testing.T) string {
	t.Helper()

	buf := make([]byte, 12)

	_, err := rand.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	return hex.EncodeToString(buf)
}

func openAMQP(t *testing.T, cfg labConfig) (*amqp.Connection, *amqp.Channel) {
	t.Helper()

	conn, err := broker.Open(cfg.AMQPURI)
	if err != nil {
		t.Fatal("amqp unreachable (backend is internal; run the host operator integration script in the hopper-api netns): " + redactError(err).Error())
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}

	err = broker.PreparePublisher(ch)
	if err != nil {
		ch.Close()
		conn.Close()
		t.Fatal(err)
	}

	return conn, ch
}

type nopPublisher struct{}

func (nopPublisher) PublishJob(context.Context, string, string) error {
	return nil
}

func waitEnqueueOrGhost(ctx context.Context, ch *amqp.Channel, jobID string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	held := make([]uint64, 0, 8)

	defer func() { nackHeld(ch, held) }()

	jobMatch := func(msg amqp.Delivery) bool {
		parsed, err := broker.ParseEnqueue(msg.Body)

		return err == nil && parsed == jobID
	}
	ghostMatch := func(msg amqp.Delivery) bool {
		parsed, err := broker.ParseDLQ(msg.Body)

		return err == nil && parsed.JobID == jobID && parsed.Reason == "missing_document"
	}

	for {
		found, err := drainQueueOnce(ch, broker.QueueJobs, jobMatch, &held)
		if err != nil {
			return err
		}

		if found {
			return nil
		}

		found, err = drainQueueOnce(ch, domain.QueueDLQ, ghostMatch, &held)
		if err != nil {
			return err
		}

		if found {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func redactError(err error) error {
	if err == nil {
		return nil
	}

	msg := redactSecrets.ReplaceAllString(err.Error(), "[redacted]")

	return fmt.Errorf("%s", msg)
}
