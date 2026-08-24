package persist

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

func historyConcat() bson.D {
	return bson.D{{Key: "$slice", Value: bson.A{
		bson.D{{Key: opConcatArrays, Value: bson.A{
			bson.A{"$dispatch"},
			bson.D{{Key: opIfNull, Value: bson.A{"$dispatch_history", bson.A{}}}},
		}}},
		dispatchHistoryCap,
	}}}
}

func appendAttempts(rows []domain.Attempt) bson.D {
	return bson.D{{Key: opConcatArrays, Value: bson.A{
		bson.D{{Key: opIfNull, Value: bson.A{pathAttempts, bson.A{}}}},
		mapAttempts(rows),
	}}}
}

func pendingDispatch(intent, queue string, cycle any) bson.D {
	return bson.D{
		{Key: "generation", Value: bson.D{{Key: opAdd, Value: bson.A{"$dispatch.generation", 1}}}},
		{Key: "intent", Value: intent},
		{Key: "queue", Value: queue},
		{Key: fStatus, Value: dispatchPending},
		{Key: "created_at", Value: serverNow},
		{Key: fCycle, Value: cycle},
	}
}

func unsetFence() bson.D {
	return bson.D{{Key: opUnset, Value: bson.A{fFenceToken, "claimed_by", fClaimExpiresAt}}}
}

func CapDeadPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fStatus, Value: string(domain.StatusDead)},
			{Key: fUpdatedAt, Value: serverNow},
			{Key: fDispatchHistory, Value: historyConcat()},
			{Key: fDispatch, Value: pendingDispatch(intentDLQ, domain.QueueDLQ, pathCycle)},
		}}},
		unsetFence(),
		bson.D{{Key: opUnset, Value: bson.A{fNotBefore}}},
	}
}

func LeaseRecoverPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fStatus, Value: string(domain.StatusQueued)},
			{Key: fUpdatedAt, Value: serverNow},
			{Key: fDispatchHistory, Value: historyConcat()},
			{Key: fDispatch, Value: pendingDispatch(intentEnqueue, queueJobs, pathCycle)},
		}}},
		unsetFence(),
		bson.D{{Key: opUnset, Value: bson.A{fNotBefore}}},
	}
}

func ReplayPipeline(by string) mongo.Pipeline {
	entry := bson.D{
		{Key: "at", Value: serverNow},
		{Key: "by", Value: by},
		{Key: "from_cycle", Value: pathCycle},
		{Key: "to_cycle", Value: bson.D{{Key: opAdd, Value: bson.A{pathCycle, 1}}}},
	}

	return mongo.Pipeline{
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fStatus, Value: string(domain.StatusQueued)},
			{Key: fCycle, Value: bson.D{{Key: opAdd, Value: bson.A{pathCycle, 1}}}},
			{Key: "attempts_done", Value: 0},
			{Key: "delivery_starts", Value: 0},
			{Key: "replay_count", Value: bson.D{{Key: opAdd, Value: bson.A{"$replay_count", 1}}}},
			{Key: fUpdatedAt, Value: serverNow},
			{Key: "replay_history", Value: bson.D{{Key: "$slice", Value: bson.A{
				bson.D{{Key: opConcatArrays, Value: bson.A{
					bson.A{entry},
					bson.D{{Key: opIfNull, Value: bson.A{"$replay_history", bson.A{}}}},
				}}},
				replayHistoryCap,
			}}}},
			{Key: fDispatchHistory, Value: historyConcat()},
			{Key: fDispatch, Value: pendingDispatch(
				intentEnqueue,
				queueJobs,
				bson.D{{Key: opAdd, Value: bson.A{pathCycle, 1}}},
			)},
		}}},
		unsetFence(),
		bson.D{{Key: opUnset, Value: bson.A{fNotBefore}}},
	}
}

//nolint:gocritic // hugeParam: matches deliver.OutcomeIn
func OutcomePipeline(in deliver.OutcomeIn) mongo.Pipeline {
	set := bson.D{
		{Key: fAttempts, Value: appendAttempts(in.Attempts)},
		{Key: "attempts_done", Value: in.AttemptsDone},
		{Key: fStatus, Value: string(in.Status)},
		{Key: fUpdatedAt, Value: serverNow},
	}
	stages := mongo.Pipeline{
		bson.D{{Key: opSet, Value: set}},
		unsetFence(),
	}

	if in.Status == domain.StatusSucceeded {
		return append(stages, bson.D{{Key: opUnset, Value: bson.A{fNotBefore}}})
	}

	return append(stages, retryOutcomeStages(in)...)
}

//nolint:gocritic // hugeParam: matches deliver.OutcomeIn
func retryOutcomeStages(in deliver.OutcomeIn) mongo.Pipeline {
	intent := intentRetry
	queue := in.Queue

	if in.Status == domain.StatusDead {
		intent = intentDLQ
		queue = domain.QueueDLQ
	}

	if queue == "" {
		queue = queueJobs
	}

	dispatchSet := pendingDispatch(intent, queue, pathCycle)
	dispatchSet = append(dispatchSet, bson.E{Key: "attempt", Value: in.AttemptsDone})
	setFields := bson.D{
		{Key: fDispatchHistory, Value: historyConcat()},
		{Key: fDispatch, Value: dispatchSet},
	}

	extra := make(mongo.Pipeline, 0, 1)

	if in.DelaySeconds > 0 {
		due := bson.D{{Key: opAdd, Value: bson.A{serverNow, int64(in.DelaySeconds) * msPerSecond}}}
		dispatchSet = append(dispatchSet, bson.E{Key: fNotBefore, Value: due})
		setFields[1].Value = dispatchSet
		setFields = append(setFields, bson.E{Key: fNotBefore, Value: due})
	} else {
		extra = mongo.Pipeline{bson.D{{Key: opUnset, Value: bson.A{fNotBefore}}}}
	}

	return append(extra, bson.D{{Key: opSet, Value: setFields}})
}
