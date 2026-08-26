package persist

import (
	"errors"

	"github.com/flexer2006/hopper/internal/dispatch"
)

var (
	ErrNotFound        = errors.New("job not found")
	ErrClaimConflict   = errors.New("claim lost to concurrent update")
	ErrDuplicateKey    = errors.New("duplicate producer idempotency key")
	ErrInvalidStatus   = errors.New("invalid outcome status")
	ErrStandalone      = errors.New("mongodb replica set required")
	ErrStaleFence      = errors.New("stale fence token")
	ErrStaleGeneration = dispatch.ErrStaleGeneration
	ErrNotDue          = errors.New("job not_before is future")
	ErrLeaseHeld       = errors.New("job running with unexpired lease")
	ErrTerminal        = errors.New("job is terminal")
	ErrDeliveryCap     = errors.New("delivery_starts cap")
	ErrInvalidID       = errors.New("invalid job id")
	ErrInvalidHash     = errors.New("invalid request hash")
	ErrInvalidKey      = errors.New("invalid producer idempotency key")
	ErrPayload         = errors.New("invalid payload")
	ErrLeaseBudget     = errors.New("claim lease shorter than persist budget")
	ErrNotRunning      = errors.New("job is not running")
	ErrReplayNotDead   = errors.New("replay requires dead status")
	ErrReplayCap       = errors.New("replay cap reached")
	ErrWorkerID        = errors.New("invalid claimed_by")
)
