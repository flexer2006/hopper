package broker_test

import (
	"errors"
	"testing"

	"github.com/flexer2006/hopper/internal/broker"
)

func TestValidateURIAndOpen(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		err := broker.ValidateURI("")
		if !errors.Is(err, broker.ErrURI) {
			t.Fatalf("err = %v", err)
		}

		_, err = broker.Open("")
		if !errors.Is(err, broker.ErrURI) {
			t.Fatalf("Open = %v", err)
		}

		err = broker.ValidateURI("   ")
		if !errors.Is(err, broker.ErrURI) {
			t.Fatalf("whitespace = %v", err)
		}
	})

	t.Run("invalid scheme", func(t *testing.T) {
		t.Parallel()

		err := broker.ValidateURI("http://localhost:5672/")
		if !errors.Is(err, broker.ErrURI) {
			t.Fatalf("err = %v", err)
		}

		_, err = broker.Open("not-a-uri")
		if !errors.Is(err, broker.ErrURI) {
			t.Fatalf("Open = %v", err)
		}
	})

	t.Run("amqp parses", func(t *testing.T) {
		t.Parallel()

		err := broker.ValidateURI("amqp://guest:guest@127.0.0.1:5672/")
		if err != nil {
			t.Fatalf("ValidateURI: %v", err)
		}
	})
}
