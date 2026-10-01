// Package dispatch publishes job intents and recovers expired leases.
//
// [Relay.Publish] and [Relay.Tick] publish an intent and, after that succeeds,
// mark its generation published. A retry intent is promoted first. Promote and
// healing skip a stale or missing generation. Tick heals a due published intent
// and publishes the generation [Jobs.StartHealing] returns. [Relay.TickLeases]
// recovers expired leases and does not publish.
package dispatch
