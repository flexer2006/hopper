package broker

import "errors"

var (
	ErrURI            = errors.New("amqp uri required")
	ErrConfirmTimeout = errors.New("publish confirm timeout")
	ErrNack           = errors.New("publish nack")
	ErrNoConfirm      = errors.New("confirm mode required")
	ErrInvalidEnqueue = errors.New("invalid enqueue body")
	ErrInvalidDLQ     = errors.New("invalid dlq body")
	ErrInvalidJobID   = errors.New("invalid job id")
	ErrAckChannel     = errors.New("ack on missing delivery channel")
	ErrDelayBuckets   = errors.New("delay bucket table")
)
