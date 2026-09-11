package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/domain"
)

type ConfirmWaiter interface {
	WaitContext(ctx context.Context) (bool, error)
}

type confirmChannel interface {
	PublishWithDeferredConfirmWithContext(
		ctx context.Context,
		exchange, key string,
		mandatory, immediate bool,
		msg amqp.Publishing,
	) (*amqp.DeferredConfirmation, error)
}

type Publisher struct {
	publish func(ctx context.Context, exchange, key string, msg amqp.Publishing) (ConfirmWaiter, error)
	timeout time.Duration
	mu      sync.Mutex
}

func NewPublisher(
	publish func(ctx context.Context, exchange, key string, msg amqp.Publishing) (ConfirmWaiter, error),
	timeout time.Duration,
) *Publisher {
	if timeout <= 0 {
		timeout = DefaultConfirmTimeout
	}

	return &Publisher{publish: publish, timeout: timeout, mu: sync.Mutex{}}
}

func PublisherFromChannel(channel *amqp.Channel, timeout time.Duration) *Publisher {
	return NewPublisher(func(
		ctx context.Context,
		exchange, key string,
		msg amqp.Publishing,
	) (ConfirmWaiter, error) {
		return PublishConfirmed(ctx, channel, exchange, key, msg)
	}, timeout)
}

func LazyPublisher(channel func() *amqp.Channel, timeout time.Duration) *Publisher {
	return NewPublisher(func(
		ctx context.Context,
		exchange, key string,
		msg amqp.Publishing,
	) (ConfirmWaiter, error) {
		current := channel()
		if current == nil {
			return nil, ErrUnavailable
		}

		return PublishConfirmed(ctx, current, exchange, key, msg)
	}, timeout)
}

//nolint:ireturn // ConfirmWaiter is the publisher's confirm contract.
func PublishConfirmed(
	ctx context.Context,
	channel confirmChannel,
	exchange, key string,
	msg amqp.Publishing, //nolint:gocritic // hugeParam: amqp091-go takes Publishing by value.
) (ConfirmWaiter, error) {
	deferred, err := channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		key,
		publishMandatory,
		publishImmediate,
		msg,
	)
	if err != nil {
		return nil, fmt.Errorf("publish confirm: %w", err)
	}

	if deferred == nil {
		return nil, ErrNoConfirm
	}

	return deferred, nil
}

func (pub *Publisher) Publish(ctx context.Context, queue string, body []byte) error {
	if pub == nil || pub.publish == nil {
		return ErrNoConfirm
	}

	msg := amqp.Publishing{ //nolint:exhaustruct_v5 // AMQP zeros are protocol defaults.
		DeliveryMode: amqp.Persistent,
		ContentType:  contentTypeJSON,
		Body:         body,
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()

	waitCtx, cancel := context.WithTimeout(ctx, pub.timeout)
	defer cancel()

	waiter, err := pub.publish(waitCtx, queue, queue, msg)
	if err != nil {
		return err
	}

	if waiter == nil {
		return ErrNoConfirm
	}

	acked, waitErr := waiter.WaitContext(waitCtx)
	if waitErr != nil {
		if errors.Is(waitErr, context.DeadlineExceeded) || errors.Is(waitErr, context.Canceled) {
			return fmt.Errorf("%w: %w", ErrConfirmTimeout, waitErr)
		}

		return fmt.Errorf("confirm wait: %w", waitErr)
	}

	if !acked {
		return ErrNack
	}

	return nil
}

func (pub *Publisher) PublishJob(ctx context.Context, queue, jobID string) error {
	body, err := MarshalEnqueue(jobID)
	if err != nil {
		return err
	}

	return pub.Publish(ctx, queue, body)
}

func (pub *Publisher) PublishDLQ(ctx context.Context, body []byte) error {
	_, err := ParseDLQ(body)
	if err != nil {
		return err
	}

	return pub.Publish(ctx, domain.QueueDLQ, body)
}
