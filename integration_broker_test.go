//go:build integration

package hopper_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/domain"
)

func TestLiveATINT01TopologyPassiveInspect(t *testing.T) {
	cfg := requireCompose(t)

	conn, err := broker.Open(cfg.AMQPURI)
	if err != nil {
		t.Fatal("amqp unreachable (backend is internal; run HOP-13-operator-inttest.py): " + redactError(err).Error())
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	defer ch.Close()

	err = ch.ExchangeDeclarePassive(broker.ExchangeDelayDLX, "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("delay dlx exchange: %v", err)
	}

	_, err = ch.QueueDeclarePassive(broker.QueueJobs, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("jobs queue: %v", err)
	}

	err = ch.ExchangeDeclarePassive(broker.QueueJobs, "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("jobs per-queue exchange (W1): %v", err)
	}

	_, err = ch.QueueDeclarePassive(domain.QueueDLQ, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("dlq queue: %v", err)
	}

	delayQueues, err := broker.DelayQueues()
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range delayQueues {
		_, qErr := ch.QueueDeclarePassive(name, true, false, false, false, nil)
		if qErr != nil {
			t.Fatalf("delay queue %s: %v", name, qErr)
		}

		xErr := ch.ExchangeDeclarePassive(name, "direct", true, false, false, false, nil)
		if xErr != nil {
			t.Fatalf("delay exchange %s: %v", name, xErr)
		}
	}
}

func TestLiveATGHOST01MissingDocumentDLQ(t *testing.T) {
	cfg := requireCompose(t)
	conn, ch := openAMQP(t, cfg)
	defer conn.Close()
	defer ch.Close()

	jobID := randomJobID(t)
	pub := broker.PublisherFromChannel(ch, broker.DefaultConfirmTimeout)

	err := pub.PublishJob(t.Context(), broker.QueueJobs, jobID)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err = waitDLQReason(ctx, ch, func(msg broker.DLQMessage) bool {
		return msg.JobID == jobID && msg.Reason == "missing_document"
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLiveATMALF01MalformedDLQ(t *testing.T) {
	cfg := requireCompose(t)
	conn, ch := openAMQP(t, cfg)
	defer conn.Close()
	defer ch.Close()

	raw := []byte(`{"job_id":"NOT-A-HEX-ID"}`)
	sum := sha256.Sum256(raw)
	want := hex.EncodeToString(sum[:])
	pub := broker.PublisherFromChannel(ch, broker.DefaultConfirmTimeout)

	err := pub.Publish(t.Context(), broker.QueueJobs, raw)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err = waitDLQReason(ctx, ch, func(msg broker.DLQMessage) bool {
		return msg.Reason == "malformed_message" && msg.BodySHA256 == want
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitDLQReason(ctx context.Context, ch *amqp.Channel, match func(broker.DLQMessage) bool) error {
	return drainGetMatch(ctx, ch, domain.QueueDLQ, func(msg amqp.Delivery) bool {
		parsed, err := broker.ParseDLQ(msg.Body)

		return err == nil && match(parsed)
	})
}
