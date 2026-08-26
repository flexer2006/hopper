package worker

import "errors"

var (
	ErrMalformed = errors.New("malformed enqueue body")
	ErrAuxiliary = errors.New("auxiliary dlq missing")
)
