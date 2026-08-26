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

	var obj map[string]any

	err := json.Unmarshal(trim, &obj)
	if err != nil {
		return nil, errJSONSyntax
	}

	encoded, err := json.Marshal(obj)
	if err != nil {
		return nil, errJSONSyntax
	}

	if len(encoded) > limit {
		return nil, errPayloadTooLarge
	}

	return trim, nil
}
