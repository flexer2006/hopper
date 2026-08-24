package platform

import "errors"

var (
	ErrHelp        = errors.New("help requested")
	ErrInvalidMode = errors.New("invalid process mode")
	ErrConfig      = errors.New("configuration")
	ErrAPIToken    = errors.New("api token shorter than 32 bytes")
)
