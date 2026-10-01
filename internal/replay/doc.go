// Package replay asks the store to reopen a dead job, then publishes that intent.
//
// [Service.Replay] rejects an empty id. The store performs the transition.
// Publish uses the returned generation on [domain.QueueJobs]. If publish fails,
// or the publisher is nil, [Result.Accepted] is false and Replay returns a nil
// error. An error from the store is returned unchanged.
package replay
