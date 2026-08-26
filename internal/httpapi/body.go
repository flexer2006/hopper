package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	defaultMaxRequestBytes = 524288
	defaultMaxPayloadBytes = 262144
	defaultJSONMaxDepth    = 64
	bodyCapSlack           = 1
)

var errBodyTooLarge = errors.New("request body too large")

func readCapped(body io.Reader, limit int) ([]byte, error) {
	limited := io.LimitReader(body, int64(limit)+bodyCapSlack)

	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if len(raw) > limit {
		return nil, errBodyTooLarge
	}

	return raw, nil
}

func requestHash(raw []byte) string {
	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])
}

func jsonTooDeep(raw []byte, maxDepth int) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	depth := 0

	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return depth != 0
			}

			return true
		}

		delim, ok := tok.(json.Delim)
		if !ok {
			continue
		}

		switch delim {
		case '{', '[':
			depth++
			if depth > maxDepth {
				return true
			}
		case '}', ']':
			depth--
		default:
			continue
		}
	}
}
