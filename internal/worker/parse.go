package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
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
	var msg enqueueBody

	err := jsonv2.Unmarshal(
		body,
		&msg,
		json.DefaultOptionsV1(),
		jsonv2.RejectUnknownMembers(true),
		jsontext.AllowDuplicateNames(false),
		jsontext.AllowInvalidUTF8(false),
	)
	if err != nil {
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

	var hexBuf [sha256.Size * 2]byte

	hex.Encode(hexBuf[:], sum[:])

	out, err := json.Marshal(struct {
		Reason string `json:"reason"`
		SHA    string `json:"body_sha256"`
		Size   int    `json:"body_size"`
	}{
		Reason: reasonMalformed,
		SHA:    string(hexBuf[:]),
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
	for i := range len(value) {
		ch := value[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}

	return true
}

func clipRunes(s string, limit int) string {
	if len(s) <= limit || utf8.RuneCountInString(s) <= limit {
		return s
	}

	runes := []rune(s)

	return string(runes[:limit])
}
