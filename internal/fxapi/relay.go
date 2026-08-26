package fxapi

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/platform"
)

type relayIn struct {
	fx.In

	LC        fx.Lifecycle
	Log       *zap.Logger
	Cfg       *platform.Config
	Jobs      dispatch.Jobs      `optional:"true"`
	Publisher dispatch.Publisher `optional:"true"`
}

func startRelay(in relayIn) {
	if in.Jobs == nil || in.Publisher == nil || in.Cfg == nil {
		return
	}

	relay := dispatch.NewRelay(in.Jobs, in.Publisher, dispatch.Config{
		Interval: in.Cfg.RelayInterval,
		Healing:  in.Cfg.HealingInterval,
		Limit:    dispatch.DefaultLimit,
	}, in.Log)

	var stop func()

	in.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			stop = relay.Start(ctx)

			return nil
		},
		OnStop: func(context.Context) error {
			if stop != nil {
				stop()
			}

			return nil
		},
	})
}
