// Package httpapi is the public HTTP API.
//
// [New] serves unauthenticated GET /healthz. It returns 200 only when the
// mongo and amqp checks are up. Routes under /v1 are rate limited before
// bearer authentication. The limiter uses the connection address, or one
// X-Forwarded-For hop when TrustXFFHops is positive and that hop is present.
// POST /v1/jobs requires Idempotency-Key and hashes the raw body. Create and
// replay return 202 only when publish is accepted, and 503 with the job id
// when Accepted is false. GET /v1/jobs lists only status dead.
package httpapi
