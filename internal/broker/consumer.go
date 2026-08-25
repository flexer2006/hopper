package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Ack(delivery *amqp.Delivery) error {
	if delivery == nil || delivery.Acknowledger == nil {
		return ErrAckChannel
	}

	err := delivery.Ack(ackMultiple)
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}

	return nil
}

func NackDrop(delivery *amqp.Delivery) error {
	if delivery == nil || delivery.Acknowledger == nil {
		return ErrAckChannel
	}

	err := delivery.Nack(ackMultiple, nackRequeue)
	if err != nil {
		return fmt.Errorf("nack: %w", err)
	}

	return nil
}
