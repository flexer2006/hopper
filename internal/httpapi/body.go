package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
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
	depth := 0

	i := 0

	for i < len(raw) {
		switch raw[i] {
		case '"':
			next := skipJSONString(raw, i+1)
			if next < 0 {
				return true
			}

			i = next
		case '{', '[':
			depth++
			if depth > maxDepth {
				return true
			}

			i++
		case '}', ']':
			depth--
			if depth < 0 {
				return true
			}

			i++
		default:
			i++
		}
	}

	return depth != 0
}

func skipJSONString(raw []byte, i int) int {
	for i < len(raw) {
		if raw[i] == '\\' {
			if i+1 >= len(raw) {
				return -1
			}

			i += 2

			continue
		}

		if raw[i] == '"' {
			return i + 1
		}

		i++
	}

	return -1
}
