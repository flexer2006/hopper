package persist

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

type mongoColl struct {
	coll *mongo.Collection
}

func MapDriverError(op string, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return ErrNotFound
	}

	if mongo.IsDuplicateKeyError(err) {
		return ErrDuplicateKey
	}

	return fmt.Errorf("%s: %w", op, err)
}

func (c *mongoColl) insert(ctx context.Context, doc *jobDoc) error {
	_, err := c.coll.InsertOne(ctx, doc)

	return MapDriverError("insert", err)
}

func (c *mongoColl) byID(ctx context.Context, id string) (jobDoc, error) {
	return c.findOne(ctx, bson.D{{Key: fID, Value: id}})
}

func (c *mongoColl) byProducerKey(ctx context.Context, key string) (jobDoc, error) {
	return c.findOne(ctx, bson.D{{Key: "producer_idempotency_key", Value: key}})
}

func (c *mongoColl) findOne(ctx context.Context, filter bson.D) (jobDoc, error) {
	var doc jobDoc

	err := c.coll.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		return jobDoc{}, MapDriverError("find", err)
	}

	return doc, nil
}

func (c *mongoColl) findAndUpdate(ctx context.Context, filter bson.D, pipeline mongo.Pipeline) (jobDoc, error) {
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var doc jobDoc

	err := c.coll.FindOneAndUpdate(ctx, filter, pipeline, opts).Decode(&doc)
	if err != nil {
		return jobDoc{}, MapDriverError("findAndUpdate", err)
	}

	return doc, nil
}

func (c *mongoColl) claimDue(ctx context.Context, id, worker, fence string, lease time.Duration) (jobDoc, error) {
	return c.findAndUpdate(ctx, ClaimDueFilter(id), ClaimDuePipeline(fence, worker, lease.Milliseconds()))
}

func (c *mongoColl) capDead(ctx context.Context, id string) (jobDoc, error) {
	return c.findAndUpdate(ctx, CapDeadFilter(id), CapDeadPipeline())
}

//nolint:gocritic // hugeParam: deliver.OutcomeIn
func (c *mongoColl) outcome(ctx context.Context, in deliver.OutcomeIn) (jobDoc, error) {
	doc, err := c.findAndUpdate(ctx, OutcomeFilter(in.ID, in.FenceToken, in.Cycle), OutcomePipeline(in))
	if errors.Is(err, ErrNotFound) {
		return jobDoc{}, ErrStaleFence
	}

	return doc, err
}

func (c *mongoColl) markPublished(ctx context.Context, id string, generation int) (jobDoc, error) {
	doc, err := c.findAndUpdate(ctx, MarkPublishedFilter(id, generation), MarkPublishedPipeline())
	if errors.Is(err, ErrNotFound) {
		return jobDoc{}, ErrStaleGeneration
	}

	return doc, err
}

func (c *mongoColl) recoverLease(ctx context.Context, id string) (jobDoc, error) {
	return c.findAndUpdate(ctx, LeaseFilter(id), LeaseRecoverPipeline())
}

func (c *mongoColl) replay(ctx context.Context, id, by string) (jobDoc, error) {
	doc, err := c.findAndUpdate(ctx, ReplayFilter(id), ReplayPipeline(by))
	if !errors.Is(err, ErrNotFound) {
		return doc, err
	}

	existing, findErr := c.byID(ctx, id)
	if findErr != nil {
		return jobDoc{}, findErr
	}

	if existing.Status != string(domain.StatusDead) {
		return jobDoc{}, ErrReplayNotDead
	}

	return jobDoc{}, ErrReplayCap
}

func (c *mongoColl) skipReason(ctx context.Context, id string) error {
	doc, err := c.byID(ctx, id)
	if err != nil {
		return err
	}

	switch domain.Status(doc.Status) {
	case domain.StatusSucceeded, domain.StatusDead:
		return ErrTerminal
	case domain.StatusRunning:
		return c.runningSkip(ctx, id)
	case domain.StatusQueued:
		return c.queuedSkip(ctx, id)
	default:
		return ErrNotFound
	}
}

func (c *mongoColl) runningSkip(ctx context.Context, id string) error {
	_, err := c.findOne(ctx, LeaseHeldFilter(id))
	if err == nil {
		return ErrLeaseHeld
	}

	if errors.Is(err, ErrNotFound) {
		return ErrNotRunning
	}

	return err
}

func (c *mongoColl) queuedSkip(ctx context.Context, id string) error {
	_, err := c.findOne(ctx, NotDueFilter(id))
	if err == nil {
		return ErrNotDue
	}

	if errors.Is(err, ErrNotFound) {
		return ErrClaimConflict
	}

	return err
}
