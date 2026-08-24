package persist

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Options struct {
	URI        string
	Database   string
	Collection string
	Lease      time.Duration
}

func Open(ctx context.Context, opts Options) (*Store, error) {
	opts = withOpenDefaults(opts)
	if !URIHasReplicaSet(opts.URI) {
		return nil, fmt.Errorf("%w: uri missing replicaSet", ErrStandalone)
	}

	client, err := mongo.Connect(ClientOptions(opts.URI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	store, readyErr := ready(ctx, client, opts)
	if readyErr == nil {
		return store, nil
	}

	disErr := client.Disconnect(ctx)
	if disErr != nil {
		return nil, fmt.Errorf("mongo ready: %w", errors.Join(readyErr, disErr))
	}

	return nil, fmt.Errorf("mongo ready: %w", readyErr)
}

func withOpenDefaults(opts Options) Options {
	if opts.Database == "" {
		opts.Database = "hopper"
	}

	if opts.Collection == "" {
		opts.Collection = "jobs"
	}

	if opts.Lease <= 0 {
		opts.Lease = defaultLease
	}

	return opts
}

func ready(ctx context.Context, client *mongo.Client, opts Options) (*Store, error) {
	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	var hello bson.M

	err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	if err != nil {
		return nil, fmt.Errorf("mongo hello: %w", err)
	}

	_, err = SetNameFromHello(hello)
	if err != nil {
		return nil, err
	}

	coll := client.Database(opts.Database).Collection(opts.Collection)

	_, err = coll.Indexes().CreateMany(ctx, IndexModels())
	if err != nil {
		return nil, fmt.Errorf("mongo indexes: %w", err)
	}

	return &Store{
		coll:     &mongoColl{coll: coll},
		now:      func() time.Time { return time.Now().UTC() },
		newFence: randomFence,
		lease:    opts.Lease,
		client:   client,
	}, nil
}
