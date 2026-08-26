package dispatch_test

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/persist"
)

func TestTickLeasesExpiredRunningQueuedPending(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	_, err = st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	ids, err := st.ListExpiredLeases(t.Context(), 8)
	if err != nil || len(ids) != 0 {
		t.Fatalf("unexpired list = %v err=%v", ids, err)
	}

	clk.add(31 * time.Second)

	rel := newRelay(t, st, new(recPub))
	err = rel.TickLeases(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusQueued {
		t.Fatalf("after TickLeases Get = %+v err=%v", got, err)
	}

	pending, err := st.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Queue != "jobs" ||
		pending[0].Kind != dispatch.IntentEnqueue || pending[0].Generation != 2 {
		t.Fatalf("pending after recover = %+v err=%v", pending, err)
	}
}

func TestTickLeasesUnexpiredNoTransition(t *testing.T) {
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

	rel := newRelay(t, st, new(recPub))
	err = rel.TickLeases(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(t.Context(), testJobID)
	if err != nil || got.Status != domain.StatusRunning {
		t.Fatalf("unexpired Get = %+v err=%v", got, err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           testJobID,
		FenceToken:   out.FenceToken,
		Queue:        "jobs",
		Status:       domain.StatusSucceeded,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatalf("fence still valid after unexpired scan: %v", err)
	}
}

func TestTickLeasesListError(t *testing.T) {
	t.Parallel()

	want := errors.New("list")
	rel := dispatch.NewRelay(&stubJobs{leaseErr: want}, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.TickLeases(t.Context())
	if !errors.Is(err, want) {
		t.Fatalf("TickLeases() err = %v", err)
	}
}

func TestTickLeasesRecoverErrorContinues(t *testing.T) {
	t.Parallel()

	jobs := &stubJobs{
		expired:    []string{testJobID, "cccccccccccccccccccccccc"},
		recoverErr: errors.New("mongo"),
		recoverOK:  true,
	}
	rel := dispatch.NewRelay(jobs, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.TickLeases(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs.recovered) != 2 {
		t.Fatalf("recovered = %v, want both ids attempted", jobs.recovered)
	}
}

func TestTickLeasesRecoverSuccessLogsPath(t *testing.T) {
	t.Parallel()

	jobs := &stubJobs{expired: []string{testJobID}, recoverOK: true}
	rel := dispatch.NewRelay(jobs, new(recPub), dispatch.Config{Limit: 8}, zap.NewNop())

	err := rel.TickLeases(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs.recovered) != 1 || jobs.recovered[0] != testJobID {
		t.Fatalf("recovered = %v", jobs.recovered)
	}
}
