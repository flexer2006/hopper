package fboot

import (
	"fmt"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
)

func RequireInfrastructure(cfg *platform.Config) error {
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
