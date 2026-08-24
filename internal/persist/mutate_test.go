package persist //nolint:testpackage // unexported applyOutcome / jobDoc

import (
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

func TestApplyOutcomeAppendsAttemptsAndKeepsCycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	doc := jobDoc{
		Attempts: []attemptDoc{{Outcome: string(domain.OutcomeFailure), Cycle: 1, Number: 1}},
		Dispatch: dispatchDoc{Generation: 2, Cycle: 1, Status: dispatchPublished},
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
