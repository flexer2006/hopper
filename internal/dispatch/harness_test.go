package dispatch_test

import (
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/persist"
)

func seededClock(t *testing.T) (*persist.Store, *frozenClock) {
	t.Helper()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	err := st.Insert(t.Context(), testRecord())
	if err != nil {
		t.Fatal(err)
	}

	return st, clk
}

func seededStore(t *testing.T) *persist.Store {
	t.Helper()

	st, _ := seededClock(t)

	return st
}

func claimedJob(t *testing.T) (*persist.Store, *frozenClock, deliver.ClaimOut) {
	t.Helper()

	st, clk := seededClock(t)
	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: testJobID, WorkerID: testWorker})
	if err != nil {
		t.Fatal(err)
	}

	return st, clk, out
}
