package egress

import "github.com/flexer2006/hopper/internal/deliver"

var (
	ErrBlocked         = deliver.ErrBlocked
	ErrBodyLimit       = deliver.ErrBodyLimit
	ErrInvalidRequest  = deliver.ErrInvalidHTTP
	ErrEmptyResolution = deliver.ErrEmptyDNS
)
