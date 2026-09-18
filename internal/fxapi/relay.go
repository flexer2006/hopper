package fxapi

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
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

type enqueueIn struct {
	fx.In

	Holder *relayHolder
	Store  enqueue.Store `optional:"true"`
}

type queryIn struct {
	fx.In

	Store query.Store `optional:"true"`
}

type replayIn struct {
	fx.In

	Holder *relayHolder
	Store  replay.Store `optional:"true"`
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

func newEnqueue(in enqueueIn) *enqueue.Service {
	if in.Store == nil {
		return nil
	}

	var pub enqueue.Publisher
	if in.Holder != nil && in.Holder.relay != nil {
		pub = in.Holder.relay
	}

	return enqueue.NewService(in.Store, pub)
}

func newQuery(in queryIn) *query.Service {
	if in.Store == nil {
		return nil
	}

	return query.NewService(in.Store)
}

func newReplay(in replayIn) *replay.Service {
	if in.Store == nil {
		return nil
	}

	var pub replay.Publisher
	if in.Holder != nil && in.Holder.relay != nil {
		pub = in.Holder.relay
	}

	return replay.NewService(in.Store, pub)
}
