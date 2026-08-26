package persist

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
)

func dueClause() bson.D {
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: fNotBefore, Value: bson.D{{Key: opExists, Value: false}}}},
		bson.D{{Key: opExpr, Value: bson.D{{Key: "$lte", Value: bson.A{"$not_before", serverNow}}}}},
	}}}
}

func startCapExpr(lt bool) bson.D {
	cmp := opLt
	if !lt {
		cmp = "$gte"
	}

	return bson.D{{Key: opExpr, Value: bson.D{
		{Key: cmp, Value: bson.A{
			"$delivery_starts",
			bson.D{{Key: opAdd, Value: bson.A{"$max_attempts", domain.DeliveryStartsSlack}}},
		}},
	}}}
}

func ClaimDueFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusQueued)},
		{Key: opAnd, Value: bson.A{dueClause(), startCapExpr(true)}},
	}
}

func CapDeadFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusQueued)},
		{Key: opAnd, Value: bson.A{dueClause(), startCapExpr(false)}},
	}
}

func ClaimDuePipeline(fence, worker string, leaseMs int64) mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fStatus, Value: string(domain.StatusRunning)},
			{Key: fFenceToken, Value: fence},
			{Key: "claimed_by", Value: worker},
			{Key: fClaimExpiresAt, Value: bson.D{{Key: opAdd, Value: bson.A{serverNow, leaseMs}}}},
			{Key: "delivery_starts", Value: bson.D{{Key: opAdd, Value: bson.A{"$delivery_starts", 1}}}},
			{Key: fUpdatedAt, Value: serverNow},
		}}},
	}
}

func MarkPublishedFilter(id string, generation int) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: "dispatch.generation", Value: generation},
		{Key: fDispatchStatus, Value: dispatch.StatusPending},
	}
}

func MarkPublishedPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fDispatchStatus, Value: dispatch.StatusPublished},
			{Key: "dispatch.published_at", Value: serverNow},
			{Key: fUpdatedAt, Value: serverNow},
		}}},
	}
}

func OutcomeFilter(id, fence string, cycle int) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fFenceToken, Value: fence},
		{Key: fStatus, Value: string(domain.StatusRunning)},
		{Key: fCycle, Value: cycle},
	}
}

func LeaseScanFilter() bson.D {
	return bson.D{
		{Key: fStatus, Value: string(domain.StatusRunning)},
		{Key: opExpr, Value: bson.D{{Key: opLt, Value: bson.A{pathClaimExpiresAt, serverNow}}}},
	}
}

func LeaseFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusRunning)},
		{Key: opExpr, Value: bson.D{{Key: opLt, Value: bson.A{pathClaimExpiresAt, serverNow}}}},
	}
}

func ReplayFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusDead)},
		{Key: opExpr, Value: bson.D{{Key: opLt, Value: bson.A{"$replay_count", domain.ReplayCap}}}},
	}
}

func PendingFilter() bson.D {
	return bson.D{{Key: fDispatchStatus, Value: dispatch.StatusPending}}
}

func healingAgeClause(ageMs int64) bson.D {
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: fPublishedAt, Value: bson.D{{Key: opExists, Value: false}}}},
		bson.D{{Key: opExpr, Value: bson.D{{Key: "$lte", Value: bson.A{
			bson.D{{Key: opAdd, Value: bson.A{"$" + fPublishedAt, ageMs}}},
			serverNow,
		}}}}},
	}}}
}

func HealingFilter(ageMs int64) bson.D {
	return bson.D{
		{Key: fStatus, Value: string(domain.StatusQueued)},
		{Key: fDispatchStatus, Value: dispatch.StatusPublished},
		{Key: opAnd, Value: bson.A{dueClause(), healingAgeClause(ageMs)}},
	}
}

func PromoteDueRetryFilter(id string, generation int) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusQueued)},
		{Key: fDispatchGen, Value: generation},
		{Key: fDispatchStatus, Value: dispatch.StatusPending},
		{Key: fDispatchIntent, Value: dispatch.IntentRetry},
		{Key: opAnd, Value: bson.A{dueClause()}},
	}
}

func HealPublishedFilter(id string, generation int, ageMs int64) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fDispatchGen, Value: generation},
		{Key: opAnd, Value: bson.A{HealingFilter(ageMs)}},
	}
}

func LeaseHeldFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusRunning)},
		{Key: opExpr, Value: bson.D{{Key: "$gte", Value: bson.A{pathClaimExpiresAt, serverNow}}}},
	}
}

func NotDueFilter(id string) bson.D {
	return bson.D{
		{Key: fID, Value: id},
		{Key: fStatus, Value: string(domain.StatusQueued)},
		{Key: fNotBefore, Value: bson.D{{Key: opExists, Value: true}}},
		{Key: opExpr, Value: bson.D{{Key: "$gt", Value: bson.A{"$not_before", serverNow}}}},
	}
}

func IndexModels() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "producer_idempotency_key", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("producer_idempotency_key_unique"),
		},
		{
			Keys:    bson.D{{Key: fStatus, Value: 1}, {Key: fUpdatedAt, Value: 1}},
			Options: options.Index().SetName("status_updated_at"),
		},
		{
			Keys: bson.D{{Key: fDispatchStatus, Value: 1}},
			Options: options.Index().SetName("dispatch_pending").SetPartialFilterExpression(bson.D{
				{Key: fDispatchStatus, Value: dispatch.StatusPending},
			}),
		},
		{
			Keys:    bson.D{{Key: fStatus, Value: 1}, {Key: fClaimExpiresAt, Value: 1}},
			Options: options.Index().SetName("status_claim_expires_at"),
		},
		{
			Keys:    bson.D{{Key: fStatus, Value: 1}, {Key: fNotBefore, Value: 1}},
			Options: options.Index().SetName("status_not_before"),
		},
	}
}
