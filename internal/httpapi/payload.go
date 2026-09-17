package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
)

var (
	errJSONDepth       = errors.New("json nesting too deep")
	errJSONSyntax      = errors.New("invalid json")
	errPayloadTooLarge = errors.New("payload too large")
)

func boundPayload(raw json.RawMessage, limit int) ([]byte, error) {
	trim := bytes.TrimSpace(raw)
	if len(trim) == 0 || trim[0] != '{' {
		return nil, errJSONSyntax
	}

	if len(trim) > limit {
		return nil, errPayloadTooLarge
	}

	if !json.Valid(trim) {
		return nil, errJSONSyntax
	}

	return trim, nil
}
