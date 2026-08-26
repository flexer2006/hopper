package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type enqueueBody struct {
	JobID string `json:"job_id"`
}

const (
	jobIDLen        = 24
	reasonMalformed = "malformed_message"
	reasonMissing   = "missing_document"
	maxErrorRunes   = 1024
)

func parseJobID(body []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var msg enqueueBody

	err := decoder.Decode(&msg)
	if err != nil {
		return "", ErrMalformed
	}

	if decoder.More() {
		return "", ErrMalformed
	}

	if len(msg.JobID) != jobIDLen || !validHex(msg.JobID) {
		return "", ErrMalformed
	}

	return msg.JobID, nil
}

func malformedDLQ(body []byte) ([]byte, error) {
	sum := sha256.Sum256(body)
	size := len(body)

	out, err := json.Marshal(struct {
		Reason string `json:"reason"`
		SHA    string `json:"body_sha256"`
		Size   int    `json:"body_size"`
	}{
		Reason: reasonMalformed,
		SHA:    hex.EncodeToString(sum[:]),
		Size:   size,
	})
	if err != nil {
		return nil, fmt.Errorf("malformed dlq: %w", err)
	}

	return out, nil
}

func ghostDLQ(id string) ([]byte, error) {
	out, err := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}{JobID: id, Reason: reasonMissing})
	if err != nil {
		return nil, fmt.Errorf("ghost dlq: %w", err)
	}

	return out, nil
}

func validHex(value string) bool {
	for i := range value {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

func clipRunes(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}

	runes := []rune(s)

	return string(runes[:limit])
}
