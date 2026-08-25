package broker

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ValidateURI(uri string) error {
	if strings.TrimSpace(uri) == "" {
		return ErrURI
	}

	_, err := amqp.ParseURI(uri)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrURI, err)
	}

	return nil
}

func Open(uri string) (*amqp.Connection, error) {
	err := ValidateURI(uri)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}

	return conn, nil
}
