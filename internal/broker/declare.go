package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/domain"
)

type topologyChannel interface {
	Confirm(noWait bool) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

type qosChannel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
}

func SetPrefetchCount(channel qosChannel, count int) error {
	if count < 1 {
		return fmt.Errorf("invalid prefetch: %w", ErrInvalidPrefetch)
	}

	err := channel.Qos(count, prefetchSizeBytes, false)
	if err != nil {
		return err
	}

	return nil
}

func Declare(channel topologyChannel) error {
	err := channel.ExchangeDeclare(ExchangeDelayDLX, exchangeKindDirect, true, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(QueueJobs, true, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(domain.QueueDLQ, true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = channel.QueueBind(QueueJobs, RoutingKeyJobs, ExchangeDelayDLX, false, nil)
	if err != nil {
		return err
	}

	buckets, err := delayBuckets()
	if err != nil {
		return err
	}

	for _, seconds := range buckets {
		name, delayErr := domain.DelayQueue(seconds)
		if delayErr != nil {
			return delayErr
		}

		_, err = channel.QueueDeclare(name, true, false, false, false, DelayQueueArgs(seconds))
		if err != nil {
			return err
		}
	}

	return nil
}

func Prepare(pub topologyChannel, sub qosChannel) error {
	return PrepareWithPrefetch(pub, sub, PrefetchCount)
}

func PrepareWithPrefetch(pub topologyChannel, sub qosChannel, prefetch int) error {
	if prefetch < 1 {
		return fmt.Errorf("invalid prefetch: %w", ErrInvalidPrefetch)
	}

	err := PreparePublisher(pub)
	if err != nil {
		return err
	}

	return SetPrefetchCount(sub, prefetch)
}

func PreparePublisher(pub topologyChannel) error {
	err := pub.Confirm(false)
	if err != nil {
		return err
	}

	return Declare(pub)
}
