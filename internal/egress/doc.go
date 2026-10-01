// Package egress performs one pinned HTTP POST.
//
// [Client.Post] resolves the target and refuses the attempt when any address
// is denylisted. It dials only the chosen IP. The Host header and TLS server
// name stay the target name, not that IP. Redirects are not followed. Each
// attempt uses a fresh HTTP/1 transport. The default deadline is 10s and the
// default body limit is 1 MiB. Idempotency-Key comes from
// [domain.OutboundIdempotencyKey].
package egress
