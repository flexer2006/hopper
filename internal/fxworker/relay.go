package fxworker

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/platform"
)

type relayHolder struct {
	relay *dispatch.Relay
}

type relayIn struct {
	fx.In

	Log       *zap.Logger
	Cfg       *platform.Config
	Jobs      dispatch.Jobs      `optional:"true"`
	Publisher dispatch.Publisher `optional:"true"`
}

type relayLife struct {
	fx.In

	LC     fx.Lifecycle
	Holder *relayHolder
}

func newRelayHolder(in relayIn) *relayHolder {
	if in.Jobs == nil || in.Publisher == nil || in.Cfg == nil {
		return new(relayHolder)
	}

	cfg := new(dispatch.Config)
	cfg.Interval = in.Cfg.RelayInterval
	cfg.Healing = in.Cfg.HealingInterval
	cfg.Lease = in.Cfg.LeaseScanInterval

	holder := new(relayHolder)
	holder.relay = dispatch.NewRelay(in.Jobs, in.Publisher, *cfg, in.Log)

	return holder
}

func startHeldRelay(in relayLife) {
	if in.Holder == nil || in.Holder.relay == nil {
		return
	}

	var stop func(context.Context) error

	in.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			stop = in.Holder.relay.Start(ctx)

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if stop != nil {
				return stop(ctx)
			}

			return nil
		},
	})
}
