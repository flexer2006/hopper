// Package query reads jobs for the public API.
//
// [Job] does not carry a fence, a dispatch intent, or a producer idempotency
// key. [Service.Get] returns one job. [Service.ListDead] requests at most
// [DefaultListLimit] jobs in status dead. The package does not change a job.
package query
