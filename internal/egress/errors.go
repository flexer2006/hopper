package egress

import "errors"

var (
	ErrBlocked         = errors.New("ssrf destination blocked")
	ErrBodyLimit       = errors.New("response exceeds 1 MiB")
	ErrInvalidRequest  = errors.New("invalid egress request")
	ErrEmptyResolution = errors.New("dns returned no addresses")
)
