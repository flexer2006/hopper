package domain

import "errors"

var (
	ErrIllegalTransition  = errors.New("illegal job transition")
	ErrInvalidTarget      = errors.New("invalid target url")
	ErrInvalidType        = errors.New("invalid job type")
	ErrInvalidMaxAttempts = errors.New("invalid max_attempts")
	ErrInvalidJobID       = errors.New("invalid job id")
	ErrReplayNotDead      = errors.New("replay requires dead status")
	ErrReplayCap          = errors.New("replay cap reached")
	ErrDeliveryCap        = errors.New("delivery_starts cap")
	ErrHTTPForbidden      = errors.New("http forbidden")
	ErrSkipHTTP           = errors.New("skip http; success recorded")
	ErrInvalidAttempt     = errors.New("invalid attempt record")
	ErrInvalidHTTPStatus  = errors.New("invalid http status")
	ErrInvalidBackoff     = errors.New("invalid backoff")
	ErrInvalidLocalKind   = errors.New("invalid local error kind")
)
