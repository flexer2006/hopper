package query_test

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/query"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestQueryGetAndListDead(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	svc := query.NewService(st)

	_, err := svc.Get(t.Context(), "aaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}

	err = st.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          "aaaaaaaaaaaaaaaaaaaaaaaa",
		Target:      "https://example.invalid/h",
		ProducerKey: "k",
		RequestHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(t.Context(), "aaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || got.Target == "" || len(got.Payload) == 0 {
		t.Fatalf("get = %+v %v", got, err)
	}

	items, err := svc.ListDead(t.Context())
	if err != nil || len(items) != 0 {
		t.Fatalf("list empty = %v %v", items, err)
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: "aaaaaaaaaaaaaaaaaaaaaaaa", WorkerID: "w"})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           "aaaaaaaaaaaaaaaaaaaaaaaa",
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	items, err = svc.ListDead(t.Context())
	if err != nil || len(items) != 1 || items[0].ID != "aaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("list dead = %+v %v", items, err)
	}
}
