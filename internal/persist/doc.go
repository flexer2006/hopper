// Package persist stores the Job document.
//
// [Open] requires a replica set. Mongo writes use majority concern with
// journaling. [Store.Claim] and [Store.CommitOutcome] update a document only
// while its filter still matches. [Store.MarkPublished] marks that generation
// and does not clear a newer one. Mongo due time and leases use server time.
// [NewMemory] applies the same transitions in memory. The package does not
// publish to the broker.
package persist
