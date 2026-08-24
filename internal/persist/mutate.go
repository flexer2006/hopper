package persist

import (
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

func isDue(doc *jobDoc, now time.Time) bool {
	if doc.NotBefore == nil {
		return true
	}

	return !doc.NotBefore.After(now)
}

func underStartCap(doc *jobDoc) bool {
	return doc.DeliveryStarts < doc.MaxAttempts+domain.DeliveryStartsSlack
}

func leaseExpired(doc *jobDoc, now time.Time) bool {
	if doc.Status != string(domain.StatusRunning) || doc.ClaimExpiresAt == nil {
		return false
	}

	return doc.ClaimExpiresAt.Before(now)
}

//nolint:gocritic // hugeParam: dispatch intent value
func rotateDispatch(doc *jobDoc, next dispatchDoc) {
	prev := doc.Dispatch
	hist := make([]dispatchDoc, 0, len(doc.DispatchHistory)+1)
	hist = append(hist, prev)
	hist = append(hist, doc.DispatchHistory...)

	if len(hist) > dispatchHistoryCap {
		hist = hist[:dispatchHistoryCap]
	}

	doc.DispatchHistory = hist
	doc.Dispatch = next
}

func applyClaim(doc *jobDoc, now time.Time, lease time.Duration, fence, worker string) {
	expires := now.Add(lease)
	doc.Status = string(domain.StatusRunning)
	doc.FenceToken = fence
	doc.ClaimedBy = worker
	doc.ClaimExpiresAt = &expires
	doc.DeliveryStarts++
	doc.UpdatedAt = now
}

func applyCapDead(doc *jobDoc, now time.Time) {
	next := dispatchDoc{
		CreatedAt:   now,
		PublishedAt: nil,
		NotBefore:   nil,
		Intent:      intentDLQ,
		Queue:       domain.QueueDLQ,
		Status:      dispatchPending,
		Generation:  doc.Dispatch.Generation + 1,
		Cycle:       doc.Cycle,
		Attempt:     0,
	}
	rotateDispatch(doc, next)
	doc.Status = string(domain.StatusDead)
	doc.FenceToken = ""
	doc.ClaimedBy = ""
	doc.ClaimExpiresAt = nil
	doc.UpdatedAt = now
}

//nolint:gocritic // hugeParam: deliver.OutcomeIn
func applyOutcome(doc *jobDoc, now time.Time, in deliver.OutcomeIn) {
	switch in.Status {
	case domain.StatusSucceeded, domain.StatusQueued, domain.StatusDead:
	default:
		return
	}

	doc.Attempts = append(doc.Attempts, mapAttempts(in.Attempts)...)
	doc.AttemptsDone = in.AttemptsDone
	doc.Status = string(in.Status)
	doc.FenceToken = ""
	doc.ClaimedBy = ""
	doc.ClaimExpiresAt = nil
	doc.UpdatedAt = now

	if in.Status == domain.StatusSucceeded {
		doc.NotBefore = nil

		return
	}

	intent := intentRetry
	queue := in.Queue

	if in.Status == domain.StatusDead {
		intent = intentDLQ
		queue = domain.QueueDLQ
	}

	if queue == "" {
		queue = queueJobs
	}

	next := dispatchDoc{
		CreatedAt:   now,
		PublishedAt: nil,
		NotBefore:   nil,
		Intent:      intent,
		Queue:       queue,
		Status:      dispatchPending,
		Generation:  doc.Dispatch.Generation + 1,
		Cycle:       doc.Cycle,
		Attempt:     in.AttemptsDone,
	}
	if in.DelaySeconds > 0 {
		due := now.Add(time.Duration(in.DelaySeconds) * time.Second)
		doc.NotBefore = &due
		next.NotBefore = &due
	} else {
		doc.NotBefore = nil
	}

	rotateDispatch(doc, next)
}

func applyLeaseRecover(doc *jobDoc, now time.Time) {
	next := dispatchDoc{
		CreatedAt:   now,
		PublishedAt: nil,
		NotBefore:   nil,
		Intent:      intentEnqueue,
		Queue:       queueJobs,
		Status:      dispatchPending,
		Generation:  doc.Dispatch.Generation + 1,
		Cycle:       doc.Cycle,
		Attempt:     0,
	}
	rotateDispatch(doc, next)
	doc.Status = string(domain.StatusQueued)
	doc.FenceToken = ""
	doc.ClaimedBy = ""
	doc.ClaimExpiresAt = nil
	doc.NotBefore = nil
	doc.UpdatedAt = now
}

func applyReplay(doc *jobDoc, now time.Time, by string) {
	from := doc.Cycle
	next := dispatchDoc{
		CreatedAt:   now,
		PublishedAt: nil,
		NotBefore:   nil,
		Intent:      intentEnqueue,
		Queue:       queueJobs,
		Status:      dispatchPending,
		Generation:  doc.Dispatch.Generation + 1,
		Cycle:       from + 1,
		Attempt:     0,
	}
	rotateDispatch(doc, next)
	hist := make([]replayDoc, 0, len(doc.ReplayHistory)+1)
	hist = append(hist, replayDoc{At: now, By: by, FromCycle: from, ToCycle: from + 1})
	hist = append(hist, doc.ReplayHistory...)

	if len(hist) > replayHistoryCap {
		hist = hist[:replayHistoryCap]
	}

	doc.ReplayHistory = hist
	doc.Cycle = from + 1
	doc.AttemptsDone = 0
	doc.DeliveryStarts = 0
	doc.ReplayCount++
	doc.Status = string(domain.StatusQueued)
	doc.NotBefore = nil
	doc.FenceToken = ""
	doc.ClaimedBy = ""
	doc.ClaimExpiresAt = nil
	doc.UpdatedAt = now
}

func applyMarkPublished(doc *jobDoc, now time.Time) {
	published := now
	doc.Dispatch.Status = dispatchPublished
	doc.Dispatch.PublishedAt = &published
	doc.UpdatedAt = now
}

func classifySkip(doc *jobDoc, now time.Time) error {
	switch domain.Status(doc.Status) {
	case domain.StatusSucceeded, domain.StatusDead:
		return ErrTerminal
	case domain.StatusRunning:
		if doc.ClaimExpiresAt != nil && !doc.ClaimExpiresAt.Before(now) {
			return ErrLeaseHeld
		}

		return ErrNotRunning
	case domain.StatusQueued:
		if !isDue(doc, now) {
			return ErrNotDue
		}

		return ErrClaimConflict
	default:
		return ErrNotFound
	}
}
