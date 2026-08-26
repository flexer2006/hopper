package persist

import "time"

const (
	defaultLease       = 30 * time.Second
	leaseMargin        = 5 * time.Second
	maxPayloadBytes    = 262144
	maxProducerKey     = 256
	maxClaimedBy       = 128
	maxReplayBy        = 128
	hexIDLen           = 24
	hexHashLen         = 64
	fenceBytes         = 16
	dispatchHistoryCap = 50
	replayHistoryCap   = 20
	msPerSecond        = 1000
	firstGeneration    = 1
	defaultScanLimit   = 64
	maxScanLimit       = 256
	serverNow          = "$$NOW"
	fID                = "_id"
	fStatus            = "status"
	fNotBefore         = "not_before"
	fFenceToken        = "fence_token"
	fClaimExpiresAt    = "claim_expires_at"
	fUpdatedAt         = "updated_at"
	fDispatchStatus    = "dispatch.status"
	fDispatchHistory   = "dispatch_history"
	fDispatch          = "dispatch"
	fDispatchGen       = "dispatch.generation"
	fDispatchIntent    = "dispatch.intent"
	fPublishedAt       = "dispatch.published_at"
	fCycle             = "cycle"
	fAttempts          = "attempts"
	pathCycle          = "$cycle"
	pathAttempts       = "$attempts"
	pathClaimExpiresAt = "$claim_expires_at"
	opExpr             = "$expr"
	opAdd              = "$add"
	opSet              = "$set"
	opUnset            = "$unset"
	opLt               = "$lt"
	opAnd              = "$and"
	opExists           = "$exists"
	opConcatArrays     = "$concatArrays"
	opIfNull           = "$ifNull"
)

func validHex(value string, n int) bool {
	if len(value) != n {
		return false
	}

	for i := range n {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}
