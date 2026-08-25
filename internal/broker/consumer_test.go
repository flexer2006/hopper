package broker_test

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/broker"
)

type fakeAck struct {
	err         error
	acks        int
	nacks       int
	lastRequeue bool
}

func (fake *fakeAck) Ack(uint64, bool) error {
	fake.acks++

	return fake.err
}

func (fake *fakeAck) Nack(_ uint64, _, requeue bool) error {
	fake.nacks++
	fake.lastRequeue = requeue

	return fake.err
}

func (fake *fakeAck) Reject(uint64, bool) error {
	return nil
}

func TestAckAndNackDrop(t *testing.T) {
	t.Parallel()

	t.Run("ack", func(t *testing.T) {
		t.Parallel()

		ack := &fakeAck{}
		delivery := amqp.Delivery{Acknowledger: ack}

		err := broker.Ack(&delivery)
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}

		if ack.acks != 1 || ack.nacks != 0 {
			t.Fatalf("ack=%d nack=%d", ack.acks, ack.nacks)
		}
	})

	t.Run("nack drop no requeue", func(t *testing.T) {
		t.Parallel()

		ack := &fakeAck{}
		delivery := amqp.Delivery{Acknowledger: ack}

		err := broker.NackDrop(&delivery)
		if err != nil {
			t.Fatalf("NackDrop: %v", err)
		}

		if ack.nacks != 1 || ack.lastRequeue {
			t.Fatalf("nack requeue=%v (must not bypass delay buckets)", ack.lastRequeue)
		}
	})

	t.Run("missing channel", func(t *testing.T) {
		t.Parallel()

		err := broker.Ack(nil)
		if !errors.Is(err, broker.ErrAckChannel) {
			t.Fatalf("err = %v", err)
		}

		err = broker.NackDrop(&amqp.Delivery{})
		if !errors.Is(err, broker.ErrAckChannel) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("ack error", func(t *testing.T) {
		t.Parallel()

		want := errors.New("ack")
		ack := &fakeAck{err: want}
		delivery := amqp.Delivery{Acknowledger: ack}

		err := broker.Ack(&delivery)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}

		err = broker.NackDrop(&delivery)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})
}
