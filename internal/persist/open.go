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

type OpenClose struct {
	Start func(context.Context) error
	Stop  func(context.Context) error
}

const DefaultCloseTimeout = 5 * time.Second

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

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultCloseTimeout)
	defer cancel()

	disErr := client.Disconnect(cleanupCtx)
	if disErr != nil {
		return nil, fmt.Errorf("mongo ready: %w", errors.Join(readyErr, disErr))
	}

	return nil, fmt.Errorf("mongo ready: %w", readyErr)
}

func (s *Store) Open(ctx context.Context, opts Options) error {
	if s == nil {
		return ErrNotOpen
	}

	if s.client != nil {
		return ErrAlreadyOpen
	}

	opened, err := Open(ctx, opts)
	if err != nil {
		return err
	}

	s.adopt(opened)

	return nil
}

func (s *Store) BindOpenClose(opts Options, closeTimeout time.Duration) OpenClose {
	if closeTimeout <= 0 {
		closeTimeout = DefaultCloseTimeout
	}

	return OpenClose{
		Start: func(ctx context.Context) error {
			err := s.Open(ctx, opts)
			if err != nil {
				return fmt.Errorf("mongo open: %w", err)
			}

			return nil
		},
		Stop: func(ctx context.Context) error {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
			defer cancel()

			return s.Close(stopCtx)
		},
	}
}

func (s *Store) adopt(opened *Store) {
	s.coll = opened.coll
	s.now = opened.now
	s.newFence = opened.newFence
	s.client = opened.client
	s.lease = opened.lease
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

	jobs := new(mongoColl)
	jobs.coll = coll

	store := new(Store)
	store.coll = jobs
	store.now = func() time.Time { return time.Now().UTC() }
	store.newFence = randomFence
	store.lease = opts.Lease
	store.client = client

	return store, nil
}
