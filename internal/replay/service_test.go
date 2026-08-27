package replay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/replay"
)

type stubPub struct {
	err error
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (s stubPub) Publish(context.Context, dispatch.Intent) error {
	return s.err
}

func TestReplayDeadConfirm(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	svc := replay.NewService(st, stubPub{})

	err := st.Insert(t.Context(), enqueue.Record{
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

	_, err = svc.Replay(t.Context(), replay.Request{ID: "aaaaaaaaaaaaaaaaaaaaaaaa", By: "ops"})
	if !errors.Is(err, domain.ErrReplayNotDead) {
		t.Fatalf("queued err = %v", err)
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

	got, err := svc.Replay(t.Context(), replay.Request{ID: "aaaaaaaaaaaaaaaaaaaaaaaa", By: "ops"})
	if err != nil || !got.Accepted || got.Status != domain.StatusQueued {
		t.Fatalf("replay = %+v %v", got, err)
	}
}

func TestReplayCap(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	svc := replay.NewService(st, stubPub{})
	const id = "aaaaaaaaaaaaaaaaaaaaaaaa"

	err := st.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          id,
		Target:      "https://example.invalid/h",
		ProducerKey: "k",
		RequestHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	for range domain.ReplayCap {
		out, claimErr := st.Claim(t.Context(), deliver.ClaimIn{ID: id, WorkerID: "w"})
		if claimErr != nil {
			t.Fatal(claimErr)
		}

		deadErr := st.CommitOutcome(t.Context(), deliver.OutcomeIn{
			ID:           id,
			FenceToken:   out.FenceToken,
			Status:       domain.StatusDead,
			AttemptsDone: 1,
			Cycle:        out.Cycle,
		})
		if deadErr != nil {
			t.Fatal(deadErr)
		}

		_, replayErr := svc.Replay(t.Context(), replay.Request{ID: id, By: "ops"})
		if replayErr != nil {
			t.Fatalf("Replay() err = %v", replayErr)
		}
	}

	out, err := st.Claim(t.Context(), deliver.ClaimIn{ID: id, WorkerID: "w"})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           id,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        domain.ReplayCap,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.Replay(t.Context(), replay.Request{ID: id, By: "ops"})
	if !errors.Is(err, domain.ErrReplayCap) {
		t.Fatalf("cap err = %v, want ErrReplayCap", err)
	}
}

func TestReplayConfirmFailAndInvalid(t *testing.T) {
	t.Parallel()

	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}, 30*time.Second)
	svc := replay.NewService(st, stubPub{err: errors.New("nack")})

	_, err := svc.Replay(t.Context(), replay.Request{})
	if !errors.Is(err, replay.ErrInvalid) {
		t.Fatalf("empty id err = %v", err)
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

	claimed, err := st.Claim(t.Context(), deliver.ClaimIn{ID: "aaaaaaaaaaaaaaaaaaaaaaaa", WorkerID: "w"})
	if err != nil {
		t.Fatal(err)
	}

	err = st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           "aaaaaaaaaaaaaaaaaaaaaaaa",
		FenceToken:   claimed.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        claimed.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := svc.Replay(t.Context(), replay.Request{ID: "aaaaaaaaaaaaaaaaaaaaaaaa"})
	if err != nil || got.Accepted {
		t.Fatalf("nack replay = %+v %v", got, err)
	}
}
