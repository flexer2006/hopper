package deliver

import "errors"

var (
	ErrLeaseHeld   = errors.New("job running with unexpired lease")
	ErrNotDue      = errors.New("job not_before is future")
	ErrTerminal    = errors.New("job is terminal")
	ErrNotRunning  = errors.New("job is not running")
	ErrClaimLost   = errors.New("claim lost to concurrent update")
	ErrBlocked     = errors.New("ssrf destination blocked")
	ErrEmptyDNS    = errors.New("dns returned no addresses")
	ErrBodyLimit   = errors.New("response exceeds 1 MiB")
	ErrInvalidHTTP = errors.New("invalid egress request")
)
