package persist //nolint:testpackage // unexported applyOutcome / jobDoc

import (
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
)

func TestApplyOutcomeAppendsAttemptsAndKeepsCycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	doc := jobDoc{
		Attempts: []attemptDoc{{Outcome: string(domain.OutcomeFailure), Cycle: 1, Number: 1}},
		Dispatch: dispatchDoc{Generation: 2, Cycle: 1, Status: dispatch.StatusPublished},
		Status:   string(domain.StatusRunning),
		Cycle:    1,
	}

	applyOutcome(&doc, now, deliver.OutcomeIn{
		Attempts: []domain.Attempt{{
			At:           now,
			Error:        "timeout",
			Outcome:      domain.OutcomeFailure,
			FailureClass: domain.ClassRetryable,
			Cycle:        1,
			Number:       2,
			DurationMS:   4,
		}},
		Status:       domain.StatusQueued,
		DelaySeconds: 1,
		AttemptsDone: 2,
		Cycle:        0,
	})

	if doc.Cycle != 1 {
		t.Fatalf("cycle = %d, want 1 (must not write OutcomeIn.Cycle)", doc.Cycle)
	}

	if len(doc.Attempts) != 2 || doc.Attempts[0].Number != 1 || doc.Attempts[1].Number != 2 {
		t.Fatalf("attempts = %+v, want append", doc.Attempts)
	}

	if doc.Dispatch.Cycle != 1 {
		t.Fatalf("dispatch.cycle = %d, want stored cycle", doc.Dispatch.Cycle)
	}

	applyOutcome(&doc, now, deliver.OutcomeIn{
		Status:       domain.StatusQueued,
		AttemptsDone: 2,
		Cycle:        99,
	})
	if len(doc.Attempts) != 2 {
		t.Fatalf("empty Attempts wiped journal: %+v", doc.Attempts)
	}
}

func TestHealingEligibleGates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	age := 30 * time.Second
	old := now.Add(-age)
	recent := now.Add(-time.Second)
	future := now.Add(time.Minute)

	queuedPublished := jobDoc{
		Dispatch: dispatchDoc{Status: dispatch.StatusPublished, PublishedAt: &old},
		Status:   string(domain.StatusQueued),
	}
	if !healingEligible(&queuedPublished, now, age) {
		t.Fatal("aged published queued should heal")
	}

	pending := queuedPublished
	pending.Dispatch.Status = dispatch.StatusPending
	if healingEligible(&pending, now, age) {
		t.Fatal("pending dispatch must not heal")
	}

	nilPublished := queuedPublished
	nilPublished.Dispatch.PublishedAt = nil
	if !healingEligible(&nilPublished, now, age) {
		t.Fatal("published with nil PublishedAt should heal")
	}

	fresh := queuedPublished
	fresh.Dispatch.PublishedAt = &recent
	if healingEligible(&fresh, now, age) {
		t.Fatal("fresh published must not heal")
	}

	running := queuedPublished
	running.Status = string(domain.StatusRunning)
	if healingEligible(&running, now, age) {
		t.Fatal("running must not heal")
	}

	notDue := queuedPublished
	notDue.NotBefore = &future
	if healingEligible(&notDue, now, age) {
		t.Fatal("future not_before must not heal")
	}
}
