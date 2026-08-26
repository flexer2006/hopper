package enqueue

import "errors"

var (
	ErrDuplicateKey        = errors.New("duplicate producer idempotency key")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different body")
	ErrInvalid             = errors.New("invalid enqueue request")
	ErrTooLarge            = errors.New("enqueue document too large")
)
