package broker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/domain"
)

type stubWaiter struct {
	err   error
	acked bool
}

type publishCall struct {
	exchange string
	key      string
	msg      amqp.Publishing
}

func (stub stubWaiter) WaitContext(ctx context.Context) (bool, error) {
	if stub.err != nil {
		return stub.acked, stub.err
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return stub.acked, nil
	}
}

func TestPublishAckNackTimeout(t *testing.T) {
	t.Parallel()

	t.Run("ack", func(t *testing.T) {
		t.Parallel()

		var got publishCall
		pub := broker.NewPublisher(func(
			_ context.Context,
			exchange, key string,
			msg amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			got = publishCall{exchange: exchange, key: key, msg: msg}

			return stubWaiter{acked: true}, nil
		}, broker.DefaultConfirmTimeout)

		err := pub.PublishJob(t.Context(), broker.QueueJobs, testJobID)
		if err != nil {
			t.Fatalf("PublishJob: %v", err)
		}

		if got.exchange != "" || got.key != broker.QueueJobs {
			t.Fatalf("routing = %q %q", got.exchange, got.key)
		}

		if got.msg.DeliveryMode != amqp.Persistent || got.msg.ContentType != "application/json" {
			t.Fatalf("publishing = %+v", got.msg)
		}

		if got.msg.Expiration != "" {
			t.Fatal("per-message expiration must not mix with queue TTL")
		}

		id, err := broker.ParseEnqueue(got.msg.Body)
		if err != nil || id != testJobID {
			t.Fatalf("body = %s err=%v", got.msg.Body, err)
		}
	})

	t.Run("nack AT-DLQ-01", func(t *testing.T) {
		t.Parallel()

		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return stubWaiter{acked: false}, nil
		}, broker.DefaultConfirmTimeout)

		err := pub.Publish(t.Context(), broker.QueueJobs, []byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`))
		if !errors.Is(err, broker.ErrNack) {
			t.Fatalf("err = %v, want ErrNack", err)
		}

		if errors.Is(err, broker.ErrConfirmTimeout) {
			t.Fatal("nack must not be timeout")
		}
	})

	t.Run("timeout not nack", func(t *testing.T) {
		t.Parallel()

		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return stubWaiter{err: context.DeadlineExceeded}, nil
		}, broker.DefaultConfirmTimeout)

		err := pub.Publish(t.Context(), broker.QueueJobs, []byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`))
		if !errors.Is(err, broker.ErrConfirmTimeout) {
			t.Fatalf("err = %v, want ErrConfirmTimeout", err)
		}

		if errors.Is(err, broker.ErrNack) {
			t.Fatal("timeout must not be nack")
		}
	})

	t.Run("canceled not nack", func(t *testing.T) {
		t.Parallel()

		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return stubWaiter{err: context.Canceled}, nil
		}, broker.DefaultConfirmTimeout)

		err := pub.Publish(t.Context(), broker.QueueJobs, []byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`))
		if !errors.Is(err, broker.ErrConfirmTimeout) {
			t.Fatalf("err = %v, want ErrConfirmTimeout", err)
		}
	})
}

func TestPublishErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("nil publisher", func(t *testing.T) {
		t.Parallel()

		var pub *broker.Publisher

		err := pub.Publish(t.Context(), broker.QueueJobs, nil)
		if !errors.Is(err, broker.ErrNoConfirm) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("nil waiter", func(t *testing.T) {
		t.Parallel()

		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return nil, nil //nolint:nilnil // AMQP returns a nil waiter when confirm mode is off.
		}, 0)

		err := pub.Publish(t.Context(), broker.QueueJobs, nil)
		if !errors.Is(err, broker.ErrNoConfirm) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("publish io", func(t *testing.T) {
		t.Parallel()

		want := errors.New("io")
		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return nil, want
		}, broker.DefaultConfirmTimeout)

		err := pub.Publish(t.Context(), broker.QueueJobs, nil)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("wait other", func(t *testing.T) {
		t.Parallel()

		want := errors.New("confirm closed")
		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			return stubWaiter{err: want}, nil
		}, broker.DefaultConfirmTimeout)

		err := pub.Publish(t.Context(), broker.QueueJobs, nil)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}

		if errors.Is(err, broker.ErrNack) || errors.Is(err, broker.ErrConfirmTimeout) {
			t.Fatal("closed confirm is not nack/timeout")
		}
	})

	t.Run("invalid job", func(t *testing.T) {
		t.Parallel()

		pub := broker.NewPublisher(func(
			context.Context, string, string, amqp.Publishing,
		) (broker.ConfirmWaiter, error) {
			t.Fatal("must not publish")

			return nil, broker.ErrNoConfirm
		}, broker.DefaultConfirmTimeout)

		err := pub.PublishJob(t.Context(), broker.QueueJobs, "nope")
		if !errors.Is(err, broker.ErrInvalidJobID) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestPublishDLQValidatesBody(t *testing.T) {
	t.Parallel()

	var key string
	pub := broker.NewPublisher(func(
		_ context.Context,
		_, routing string,
		_ amqp.Publishing,
	) (broker.ConfirmWaiter, error) {
		key = routing

		return stubWaiter{acked: true}, nil
	}, broker.DefaultConfirmTimeout)

	body, err := broker.MarshalGhostDLQ(testJobID)
	if err != nil {
		t.Fatalf("ghost: %v", err)
	}

	err = pub.PublishDLQ(t.Context(), body)
	if err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	if key != domain.QueueDLQ {
		t.Fatalf("queue = %s", key)
	}

	err = pub.PublishDLQ(t.Context(), []byte(`{"reason":"nope"}`))
	if err == nil {
		t.Fatal("invalid dlq must not publish")
	}
}

func TestPublishMutexSerializesWait(t *testing.T) {
	t.Parallel()

	var inflight atomic.Int32
	var peak atomic.Int32

	pub := broker.NewPublisher(func(
		ctx context.Context,
		_, _ string,
		_ amqp.Publishing,
	) (broker.ConfirmWaiter, error) {
		current := inflight.Add(1)
		defer inflight.Add(-1)

		for {
			seen := peak.Load()
			if current <= seen || peak.CompareAndSwap(seen, current) {
				break
			}
		}

		return stubWaiter{acked: true}, nil
	}, broker.DefaultConfirmTimeout)

	const workers = 32

	var waitGroup sync.WaitGroup

	waitGroup.Add(workers)

	for range workers {
		go func() {
			defer waitGroup.Done()

			err := pub.PublishJob(t.Context(), broker.QueueJobs, testJobID)
			if err != nil {
				t.Errorf("PublishJob: %v", err)
			}
		}()
	}

	waitGroup.Wait()

	if peak.Load() != 1 {
		t.Fatalf("peak inflight = %d, want 1 (publish mutex)", peak.Load())
	}
}

func TestPublisherFromChannelConstructs(t *testing.T) {
	t.Parallel()

	_ = broker.PublisherFromChannel(nil, 0)
}
