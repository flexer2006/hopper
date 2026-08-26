package persist

import (
	"errors"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
)

var (
	ErrNotFound        = domain.ErrNotFound
	ErrClaimConflict   = deliver.ErrClaimLost
	ErrDuplicateKey    = enqueue.ErrDuplicateKey
	ErrInvalidStatus   = errors.New("invalid outcome status")
	ErrStandalone      = errors.New("mongodb replica set required")
	ErrStaleFence      = errors.New("stale fence token")
	ErrStaleGeneration = dispatch.ErrStaleGeneration
	ErrNotDue          = deliver.ErrNotDue
	ErrLeaseHeld       = deliver.ErrLeaseHeld
	ErrTerminal        = deliver.ErrTerminal
	ErrNotRunning      = deliver.ErrNotRunning
	ErrInvalidID       = errors.New("invalid job id")
	ErrInvalidHash     = errors.New("invalid request hash")
	ErrInvalidKey      = errors.New("invalid producer idempotency key")
	ErrPayload         = enqueue.ErrInvalid
	ErrTooLarge        = enqueue.ErrTooLarge
	ErrLeaseBudget     = errors.New("claim lease shorter than persist budget")
	ErrReplayNotDead   = domain.ErrReplayNotDead
	ErrReplayCap       = domain.ErrReplayCap
	ErrWorkerID        = errors.New("invalid claimed_by")
)
