package fxworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/fx"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/worker"
)

type brokerChannels struct {
	pub        *amqp.Channel
	sub        *amqp.Channel
	deliveries <-chan amqp.Delivery
}

type workerResources struct {
	conn     *amqp.Connection
	pub      *broker.Publisher
	channels brokerChannels
	mu       sync.RWMutex
}

type amqpSource struct {
	resources *workerResources
}

type amqpDelivery struct {
	delivery amqp.Delivery
}

type mongoChecker struct {
	store *persist.Store
}

type amqpChecker struct {
	resources *workerResources
}

const (
	productionConfirmTimeout = 5 * time.Second
	storeCloseTimeout        = 5 * time.Second
)

var errDeliveriesClosed = errors.New("amqp source: deliveries closed")

func (s *amqpSource) Next(ctx context.Context) (worker.Delivery, error) { //nolint:ireturn // worker.Source contract.
	if s == nil || s.resources == nil {
		return nil, fmt.Errorf("amqp source: %w", broker.ErrUnavailable)
	}

	deliveries := s.resources.next()
	if deliveries == nil {
		return nil, fmt.Errorf("amqp source: %w", broker.ErrUnavailable)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-deliveries:
		if !ok {
			return nil, errDeliveriesClosed
		}

		return &amqpDelivery{delivery: delivery}, nil
	}
}

func (d *amqpDelivery) Body() []byte { return d.delivery.Body }

func (d *amqpDelivery) Ack() error { return broker.Ack(&d.delivery) }

func Production() fx.Option { //nolint:ireturn // Fx composition contract.
	return fx.Options(
		fx.Invoke(requireInfrastructure),
		fx.Provide(openStore),
		fx.Provide(openBroker),
		bindPorts(),
	)
}

func bindPorts() fx.Option { //nolint:ireturn // Fx composition contract.
	return fx.Options(
		fx.Provide(func(store *persist.Store) deliver.Jobs { return store }),
		fx.Provide(func(store *persist.Store) dispatch.Jobs { return store }),
		fx.Provide(func(res *workerResources) *broker.Publisher { return res.pub }),
		fx.Provide(func(res *workerResources) dispatch.Publisher { return res.pub }),
		fx.Provide(func(res *workerResources) worker.Source {
			return &amqpSource{resources: res}
		}),
		fx.Provide(fx.Annotate(mongoHealth, fx.ResultTags(`group:"health"`))),
		fx.Provide(fx.Annotate(amqpHealth, fx.ResultTags(`group:"health"`))),
	)
}

func mongoOptions(cfg *platform.Config) persist.Options {
	opts := persist.Options{
		URI:        cfg.MongoURI,
		Database:   cfg.MongoDatabase,
		Collection: cfg.MongoJobsCollection,
		Lease:      claimLease,
	}

	return opts
}

func requireInfrastructure(cfg *platform.Config) error {
	err := cfg.ValidateInfrastructure()
	if err != nil {
		return err
	}

	if !persist.URIHasReplicaSet(cfg.MongoURI) {
		return fmt.Errorf("mongo_uri: %w", persist.ErrStandalone)
	}

	err = broker.ValidateURI(cfg.AMQPURI)
	if err != nil {
		return fmt.Errorf("amqp_uri: %w", err)
	}

	return nil
}

func openStore(lc fx.Lifecycle, cfg *platform.Config) *persist.Store {
	store := new(persist.Store)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			err := store.Open(ctx, mongoOptions(cfg))
			if err != nil {
				return fmt.Errorf("mongo open: %w", err)
			}

			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeCloseTimeout)
			defer cancel()

			return store.Close(stopCtx)
		},
	})

	return store
}

func openBroker(lc fx.Lifecycle, cfg *platform.Config) *workerResources {
	resources := newWorkerResources()

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			conn, err := broker.Open(cfg.AMQPURI)
			if err != nil {
				return fmt.Errorf("amqp open: %w", err)
			}

			channels, err := prepareBroker(conn, cfg.Prefetch)
			if err != nil {
				return errors.Join(err, conn.Close())
			}

			resources.set(conn, channels)

			return nil
		},
		OnStop: func(context.Context) error {
			conn, channels := resources.release()

			return closeBroker(conn, channels)
		},
	})

	return resources
}

func newWorkerResources() *workerResources {
	resources := new(workerResources)
	resources.pub = broker.LazyPublisher(resources.channel, productionConfirmTimeout)

	return resources
}

func prepareBroker(conn *amqp.Connection, prefetch int) (brokerChannels, error) {
	pubCh, err := conn.Channel()
	if err != nil {
		return brokerChannels{}, fmt.Errorf("amqp publisher channel: %w", err)
	}

	subCh, err := conn.Channel()
	if err != nil {
		return brokerChannels{}, errors.Join(fmt.Errorf("amqp consumer channel: %w", err), pubCh.Close())
	}

	err = broker.PrepareWithPrefetch(pubCh, subCh, prefetch)
	if err != nil {
		return brokerChannels{}, errors.Join(fmt.Errorf("amqp prepare: %w", err), subCh.Close(), pubCh.Close())
	}

	deliveries, err := subCh.Consume(broker.QueueJobs, "", false, false, false, false, nil)
	if err != nil {
		return brokerChannels{}, errors.Join(fmt.Errorf("amqp consume: %w", err), subCh.Close(), pubCh.Close())
	}

	return brokerChannels{pub: pubCh, sub: subCh, deliveries: deliveries}, nil
}

func closeBroker(conn *amqp.Connection, channels brokerChannels) error {
	var errs []error

	if channels.sub != nil {
		errs = append(errs, channels.sub.Close())
	}

	if channels.pub != nil {
		errs = append(errs, channels.pub.Close())
	}

	if conn != nil {
		errs = append(errs, conn.Close())
	}

	return errors.Join(errs...)
}

func (r *workerResources) set(conn *amqp.Connection, channels brokerChannels) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.conn = conn
	r.channels = channels
}

func (r *workerResources) release() (*amqp.Connection, brokerChannels) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var cleared brokerChannels

	conn, channels := r.conn, r.channels
	r.conn = nil
	r.channels = cleared

	return conn, channels
}

func (r *workerResources) channel() *amqp.Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.channels.pub
}

func (r *workerResources) next() <-chan amqp.Delivery {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.channels.deliveries
}

func (r *workerResources) alive() bool {
	if r == nil {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.conn != nil && !r.conn.IsClosed() &&
		r.channels.pub != nil && !r.channels.pub.IsClosed() &&
		r.channels.sub != nil && !r.channels.sub.IsClosed() &&
		r.channels.deliveries != nil
}

func (c *mongoChecker) Name() string { return "mongo" }

func (c *mongoChecker) Check(ctx context.Context) error {
	if c == nil || c.store == nil {
		return persist.ErrNotOpen
	}

	return c.store.Ping(ctx)
}

func (c *amqpChecker) Name() string { return "amqp" }

func (c *amqpChecker) Check(context.Context) error {
	if c == nil || !c.resources.alive() {
		return broker.ErrUnavailable
	}

	return nil
}

func mongoHealth(store *persist.Store) httpapi.Checker { //nolint:ireturn // health group contract.
	return &mongoChecker{store: store}
}

func amqpHealth(res *workerResources) httpapi.Checker { //nolint:ireturn // health group contract.
	return &amqpChecker{resources: res}
}
