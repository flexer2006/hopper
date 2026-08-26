package persist_test

import (
	"errors"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
)

func TestListPendingAndPromoteDueRetry(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	pending, err := st.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Generation != 1 {
		t.Fatalf("ListPending() = %+v err=%v", pending, err)
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
		t.Fatal(err)
	}

	clk.add(2 * time.Second)

	next, err := st.PromoteDueRetry(t.Context(), testJobID, 2)
	if err != nil || next.Kind != dispatch.IntentEnqueue || next.Queue != "jobs" || next.Generation != 3 {
		t.Fatalf("PromoteDueRetry() = %+v err=%v", next, err)
	}

	_, err = st.PromoteDueRetry(t.Context(), testJobID, 2)
	if !errors.Is(err, dispatch.ErrNotFound) {
		t.Fatalf("second promote err = %v", err)
	}
}

func TestListDueHealingAndStartHealing(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	err := st.MarkPublished(t.Context(), testJobID, 1)
	if err != nil {
		t.Fatal(err)
	}

	early, err := st.ListDueHealing(t.Context(), 30*time.Second, 8)
	if err != nil || len(early) != 0 {
		t.Fatalf("ListDueHealing() before age = %+v err=%v", early, err)
	}

	clk.add(30 * time.Second)

	due, err := st.ListDueHealing(t.Context(), 30*time.Second, 8)
	if err != nil || len(due) != 1 || due[0].Generation != 1 {
		t.Fatalf("ListDueHealing() = %+v err=%v", due, err)
	}

	next, err := st.StartHealing(t.Context(), testJobID, 1, 30*time.Second)
	if err != nil || next.Generation != 2 || next.Kind != dispatch.IntentEnqueue {
		t.Fatalf("StartHealing() = %+v err=%v", next, err)
	}

	_, err = st.StartHealing(t.Context(), testJobID, 1, 30*time.Second)
	if !errors.Is(err, dispatch.ErrNotFound) {
		t.Fatalf("second heal err = %v", err)
	}
}

func TestListPendingClampsLimit(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))
	mustInsert(t, st, testRecord("cccccccccccccccccccccccc", "idem-2", 5))

	got, err := st.ListPending(t.Context(), 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("ListPending(1) = %+v err=%v", got, err)
	}

	got, err = st.ListPending(t.Context(), 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListPending(0) = %+v err=%v", got, err)
	}

	got, err = st.ListPending(t.Context(), 4096)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListPending(4096) = %+v err=%v", got, err)
	}
}

func TestStartHealingSkipsFutureNotBefore(t *testing.T) {
	t.Parallel()

	clk := newClock(t)
	st := newStore(t, clk, 30*time.Second)
	mustInsert(t, st, testRecord(testJobID, testKey, 5))

	out := mustClaim(t, st)
	err := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
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

	_, err = st.StartHealing(t.Context(), testJobID, 2, 30*time.Second)
	if !errors.Is(err, dispatch.ErrNotFound) {
		t.Fatalf("heal future not_before err = %v", err)
	}
}

func TestMarkPublishedMissingMapsDispatchNotFound(t *testing.T) {
	t.Parallel()

	st := newStore(t, nil, 30*time.Second)
	err := st.MarkPublished(t.Context(), "dddddddddddddddddddddddd", 1)
	if !errors.Is(err, dispatch.ErrNotFound) {
		t.Fatalf("MarkPublished missing err = %v, want dispatch.ErrNotFound", err)
	}
}
