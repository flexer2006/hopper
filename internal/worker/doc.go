// Package worker consumes job messages one at a time.
//
// [Worker.Process] claims the job and posts only when HTTP is allowed. Ack
// follows a committed outcome. A job that is not due, still leased, or already
// terminal is acknowledged without HTTP, as is the delivery-start cap. A
// malformed body or a missing job is published to the auxiliary DLQ before
// ack. If that publish or the outcome commit fails, the delivery stays
// unacked. [Worker.Run] does not cancel an attempt already taken when the
// consume context ends.
package worker
