package persist_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/persist"
)

func extJSON(t *testing.T, value any) string {
	t.Helper()

	raw, err := bson.MarshalExtJSON(value, false, false)
	if err != nil {
		t.Fatalf("MarshalExtJSON() err = %v", err)
	}

	return string(raw)
}

func pipelineJSON(t *testing.T, pipeline mongo.Pipeline) string {
	t.Helper()

	return extJSON(t, bson.D{{Key: "stages", Value: pipeline}})
}

func TestDurableWriteConcernMajorityJournal(t *testing.T) {
	t.Parallel()

	wc := persist.DurableWriteConcern()
	if wc == nil || wc.Journal == nil || !*wc.Journal {
		t.Fatal("Journal must be true")
	}

	w, ok := wc.W.(string)
	if !ok || w != writeconcern.WCMajority {
		t.Fatalf("W = %#v, want majority", wc.W)
	}

	if persist.ClientOptions("mongodb://localhost:27017/?replicaSet=rs0") == nil {
		t.Fatal("ClientOptions() nil")
	}
}

func TestURIHasReplicaSetAndHello(t *testing.T) {
	t.Parallel()

	if persist.URIHasReplicaSet("%") {
		t.Fatal("unparseable URI must fail closed")
	}

	if persist.MapDriverError("op", nil) != nil {
		t.Fatal("nil driver error")
	}

	if !errors.Is(persist.MapDriverError("op", mongo.ErrNoDocuments), persist.ErrNotFound) {
		t.Fatal("ErrNoDocuments map")
	}

	dup := mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000, Message: "E11000 duplicate"}}}
	if !errors.Is(persist.MapDriverError("op", dup), persist.ErrDuplicateKey) {
		t.Fatal("duplicate key map")
	}

	wrapped := persist.MapDriverError("ping", errors.New("network"))
	if wrapped == nil || !strings.Contains(wrapped.Error(), "ping") {
		t.Fatalf("wrap = %v", wrapped)
	}

	if persist.URIHasReplicaSet("mongodb://localhost:27017") {
		t.Fatal("URI without replicaSet")
	}

	if !persist.URIHasReplicaSet("mongodb://localhost:27017/?replicaSet=rs0") {
		t.Fatal("URI with replicaSet")
	}

	_, err := persist.SetNameFromHello(bson.M{})
	if !errors.Is(err, persist.ErrStandalone) {
		t.Fatalf("empty hello err = %v", err)
	}

	name, err := persist.SetNameFromHello(bson.M{"setName": "rs0"})
	if err != nil || name != "rs0" {
		t.Fatalf("setName = %q err = %v", name, err)
	}
}

func TestOpenRejectsStandaloneURI(t *testing.T) {
	t.Parallel()

	_, err := persist.Open(t.Context(), persist.Options{URI: "mongodb://localhost:27017"})
	if !errors.Is(err, persist.ErrStandalone) {
		t.Fatalf("Open() err = %v, want ErrStandalone", err)
	}
}

func TestCheckLeaseBudget(t *testing.T) {
	t.Parallel()

	httpTimeout := 10 * time.Second
	outcome := 5 * time.Second
	confirm := 5 * time.Second
	err := persist.CheckLeaseBudget(24*time.Second, httpTimeout, outcome, confirm)
	if !errors.Is(err, persist.ErrLeaseBudget) {
		t.Fatalf("short lease err = %v", err)
	}

	err = persist.CheckLeaseBudget(25*time.Second, httpTimeout, outcome, confirm)
	if err != nil {
		t.Fatalf("min lease err = %v", err)
	}

	err = persist.CheckLeaseBudget(30*time.Second, httpTimeout, outcome, confirm)
	if err != nil {
		t.Fatalf("default lease err = %v", err)
	}
}

func TestClaimDuePipelineUsesServerNow(t *testing.T) {
	t.Parallel()

	body := pipelineJSON(t, persist.ClaimDuePipeline("fence", "worker", 30000))
	if !strings.Contains(body, "$$NOW") {
		t.Fatalf("ClaimDuePipeline missing $$NOW: %s", body)
	}

	if !strings.Contains(body, "30000") {
		t.Fatalf("ClaimDuePipeline missing lease ms: %s", body)
	}

	filter := extJSON(t, persist.ClaimDueFilter(testJobID))
	if !strings.Contains(filter, "$$NOW") {
		t.Fatalf("ClaimDueFilter missing $$NOW: %s", filter)
	}
}

func TestMarkPublishedFilterAndPipeline(t *testing.T) {
	t.Parallel()

	filter := extJSON(t, persist.MarkPublishedFilter(testJobID, 3))
	if !strings.Contains(filter, "dispatch.generation") || !strings.Contains(filter, "pending") {
		t.Fatalf("MarkPublishedFilter = %s", filter)
	}

	body := pipelineJSON(t, persist.MarkPublishedPipeline())
	if !strings.Contains(body, "$$NOW") || !strings.Contains(body, "published") {
		t.Fatalf("MarkPublishedPipeline = %s", body)
	}
}

func TestLeaseAndNotDueFiltersUseNOW(t *testing.T) {
	t.Parallel()

	lease := extJSON(t, persist.LeaseFilter(testJobID))
	if !strings.Contains(lease, "$$NOW") || !strings.Contains(lease, "claim_expires_at") {
		t.Fatalf("LeaseFilter = %s", lease)
	}

	held := extJSON(t, persist.LeaseHeldFilter(testJobID))
	if !strings.Contains(held, "$$NOW") {
		t.Fatalf("LeaseHeldFilter = %s", held)
	}

	notDue := extJSON(t, persist.NotDueFilter(testJobID))
	if !strings.Contains(notDue, "$$NOW") {
		t.Fatalf("NotDueFilter = %s", notDue)
	}

	heal := extJSON(t, persist.HealingFilter(30000))
	if !strings.Contains(heal, "$$NOW") || !strings.Contains(heal, "published") {
		t.Fatalf("HealingFilter = %s", heal)
	}

	promote := extJSON(t, persist.PromoteDueRetryFilter(testJobID, 2))
	if !strings.Contains(promote, "retry") || !strings.Contains(promote, "pending") {
		t.Fatalf("PromoteDueRetryFilter = %s", promote)
	}

	pending := extJSON(t, persist.PendingFilter())
	if !strings.Contains(pending, "pending") {
		t.Fatalf("PendingFilter = %s", pending)
	}

	healID := extJSON(t, persist.HealPublishedFilter(testJobID, 4, 30000))
	if !strings.Contains(healID, testJobID) || !strings.Contains(healID, "dispatch.generation") {
		t.Fatalf("HealPublishedFilter = %s", healID)
	}

	enq := pipelineJSON(t, persist.EnqueuePendingPipeline())
	if !strings.Contains(enq, "$$NOW") || !strings.Contains(enq, "enqueue") {
		t.Fatalf("EnqueuePendingPipeline = %s", enq)
	}
}

func TestOutcomeAndCapPipelines(t *testing.T) {
	t.Parallel()

	success := pipelineJSON(t, persist.OutcomePipeline(deliver.OutcomeIn{
		Status: domain.StatusSucceeded,
	}))
	if strings.Contains(success, `"intent"`) {
		t.Fatalf("success pipeline rotated dispatch: %s", success)
	}

	if !strings.Contains(success, "$$NOW") {
		t.Fatalf("success pipeline missing $$NOW: %s", success)
	}

	if !strings.Contains(success, "$concatArrays") || !strings.Contains(success, "$attempts") {
		t.Fatalf("success pipeline missing append: %s", success)
	}

	if strings.Contains(success, `"cycle"`) {
		t.Fatalf("success pipeline must not $set cycle: %s", success)
	}

	retry := pipelineJSON(t, persist.OutcomePipeline(deliver.OutcomeIn{
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		Queue:        "jobs.delay.1s",
		AttemptsDone: 1,
	}))
	if !strings.Contains(retry, "$$NOW") || !strings.Contains(retry, "1000") {
		t.Fatalf("retry pipeline delay ms missing: %s", retry)
	}

	dead := pipelineJSON(t, persist.OutcomePipeline(deliver.OutcomeIn{
		Status: domain.StatusDead,
	}))
	if !strings.Contains(dead, domain.QueueDLQ) || !strings.Contains(dead, "$$NOW") {
		t.Fatalf("dead pipeline = %s", dead)
	}

	capBody := pipelineJSON(t, persist.CapDeadPipeline())
	if !strings.Contains(capBody, "$$NOW") || !strings.Contains(capBody, domain.QueueDLQ) {
		t.Fatalf("CapDeadPipeline = %s", capBody)
	}

	lease := pipelineJSON(t, persist.LeaseRecoverPipeline())
	if !strings.Contains(lease, "$$NOW") || !strings.Contains(lease, `"enqueue"`) {
		t.Fatalf("LeaseRecoverPipeline = %s", lease)
	}

	replayBody := pipelineJSON(t, persist.ReplayPipeline("ops"))
	if !strings.Contains(replayBody, "$$NOW") {
		t.Fatalf("ReplayPipeline = %s", replayBody)
	}
}

func TestIndexModelsUniqueAndPartial(t *testing.T) {
	t.Parallel()

	models := persist.IndexModels()
	if len(models) != 5 {
		t.Fatalf("IndexModels() len = %d", len(models))
	}

	raw := extJSON(t, models[0].Keys)
	if !strings.Contains(raw, "producer_idempotency_key") {
		t.Fatalf("unique keys = %s", raw)
	}

	partial := extJSON(t, models[2].Keys)
	if !strings.Contains(partial, "dispatch.status") {
		t.Fatalf("pending keys = %s", partial)
	}
}

func TestOutcomeFilterFenceCAS(t *testing.T) {
	t.Parallel()

	raw := extJSON(t, persist.OutcomeFilter(testJobID, "fence", 2))
	if !strings.Contains(raw, "fence_token") || !strings.Contains(raw, "running") || !strings.Contains(raw, "cycle") {
		t.Fatalf("OutcomeFilter = %s", raw)
	}

	capFilter := extJSON(t, persist.CapDeadFilter(testJobID))
	if !strings.Contains(capFilter, "queued") {
		t.Fatalf("CapDeadFilter = %s", capFilter)
	}

	replayFilter := extJSON(t, persist.ReplayFilter(testJobID))
	if !strings.Contains(replayFilter, "dead") {
		t.Fatalf("ReplayFilter = %s", replayFilter)
	}
}
