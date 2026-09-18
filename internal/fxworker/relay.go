package fxworker

import (
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
	holder := new(relayHolder)
	holder.relay = dispatch.NewRelayFrom(in.Jobs, in.Publisher, in.Cfg, in.Log)

	return holder
}

func startHeldRelay(in relayLife) {
	if in.Holder == nil || in.Holder.relay == nil {
		return
	}

	hooks := in.Holder.relay.BindStart()
	in.LC.Append(fx.Hook{OnStart: hooks.Start, OnStop: hooks.Stop})
}
