package fxapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/fx"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type (
	mongoChecker struct{ store *persist.Store }
	amqpChecker  struct{ resources *brokerResources }
)

type brokerResources struct {
	conn  *amqp.Connection
	pubCh *amqp.Channel
	pub   *broker.Publisher
	mu    sync.RWMutex
}

const (
	publishConfirmTimeout = 5 * time.Second
	storeCloseTimeout     = 5 * time.Second
)

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
		fx.Provide(func(s *persist.Store) dispatch.Jobs { return s }),
		fx.Provide(func(s *persist.Store) enqueue.Store { return s }),
		fx.Provide(func(s *persist.Store) query.Store { return s }),
		fx.Provide(func(s *persist.Store) replay.Store { return s }),
		fx.Provide(func(r *brokerResources) dispatch.Publisher { return r.pub }),
		fx.Provide(fx.Annotate(mongoHealth, fx.ResultTags(`group:"health"`))),
		fx.Provide(fx.Annotate(amqpHealth, fx.ResultTags(`group:"health"`))),
	)
}

func mongoOptions(cfg *platform.Config) persist.Options {
	if cfg == nil {
		return persist.Options{
			URI:        "",
			Database:   "",
			Collection: "",
			Lease:      platform.DefaultClaimLease,
		}
	}

	lease := platform.DefaultClaimLease
	if cfg.ClaimLease > 0 {
		lease = cfg.ClaimLease
	}

	return persist.Options{
		URI:        cfg.MongoURI,
		Database:   cfg.MongoDatabase,
		Collection: cfg.MongoJobsCollection,
		Lease:      lease,
	}
}

func requireInfrastructure(cfg *platform.Config) error {
	if cfg == nil {
		return fmt.Errorf("%w: missing config", platform.ErrConfig)
	}

	cfg.FillRuntimeDefaults()

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

	if cfg.ExchangeDelay != "" && cfg.ExchangeDelay != broker.ExchangeDelayDLX {
		return fmt.Errorf("exchange_delay must be %s: %w", broker.ExchangeDelayDLX, platform.ErrConfig)
	}

	err = persist.CheckLeaseBudget(cfg.ClaimLease, cfg.HTTPTimeout, cfg.MongoOutcomeTimeout, cfg.PublishConfirmTimeout)
	if err != nil {
		return fmt.Errorf("fr-46 lease budget: %w", err)
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

func openBroker(lc fx.Lifecycle, cfg *platform.Config) *brokerResources {
	resources := newBrokerResources()
	if cfg != nil && cfg.PublishConfirmTimeout > 0 {
		resources.pub = broker.LazyPublisher(resources.channel, cfg.PublishConfirmTimeout)
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			conn, err := broker.Open(cfg.AMQPURI)
			if err != nil {
				return fmt.Errorf("amqp open: %w", err)
			}

			pubCh, err := openPublisherChannel(conn)
			if err != nil {
				return errors.Join(err, conn.Close())
			}

			resources.set(conn, pubCh)

			return nil
		},
		OnStop: func(context.Context) error {
			conn, pubCh := resources.release()

			return closeBroker(conn, pubCh)
		},
	})

	return resources
}

func newBrokerResources() *brokerResources {
	resources := new(brokerResources)
	resources.pub = broker.LazyPublisher(resources.channel, publishConfirmTimeout)

	return resources
}

func openPublisherChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	pubCh, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp publisher channel: %w", err)
	}

	err = broker.PreparePublisher(pubCh)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("amqp prepare: %w", err), pubCh.Close())
	}

	return pubCh, nil
}

func closeBroker(conn *amqp.Connection, pubCh *amqp.Channel) error {
	var errs []error

	if pubCh != nil {
		errs = append(errs, pubCh.Close())
	}

	if conn != nil {
		errs = append(errs, conn.Close())
	}

	return errors.Join(errs...)
}

func (r *brokerResources) set(conn *amqp.Connection, pubCh *amqp.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.conn = conn
	r.pubCh = pubCh
}

func (r *brokerResources) release() (*amqp.Connection, *amqp.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, pubCh := r.conn, r.pubCh
	r.conn = nil
	r.pubCh = nil

	return conn, pubCh
}

func (r *brokerResources) channel() *amqp.Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.pubCh
}

func (r *brokerResources) alive() bool {
	if r == nil {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.conn != nil && !r.conn.IsClosed() && r.pubCh != nil && !r.pubCh.IsClosed()
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

func mongoHealth(s *persist.Store) httpapi.Checker { //nolint:ireturn // health group contract.
	return &mongoChecker{store: s}
}

func amqpHealth(r *brokerResources) httpapi.Checker { //nolint:ireturn // health group contract.
	return &amqpChecker{resources: r}
}
