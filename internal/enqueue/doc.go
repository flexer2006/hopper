// Package enqueue inserts a job and publishes its first intent.
//
// [Service.Enqueue] requires a producer key and a request hash.
// [domain.AdmitTarget] checks the target. The first insert publishes generation
// 1 on [domain.QueueJobs]. The same key and the same hash return that job's id.
// A different hash is [ErrIdempotencyConflict]. An already published intent, or
// a retry intent, is accepted without another publish. If publish fails, or the
// publisher is nil, [Result.Accepted] is false and Enqueue returns a nil error.
// Any other store error is returned unchanged.
package enqueue
