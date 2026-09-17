package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
)

type EnqueueMessage struct {
	JobID string `json:"job_id"`
}

type DLQMessage struct {
	At         string `json:"at,omitempty"`
	BodySHA256 string `json:"body_sha256,omitempty"`
	JobID      string `json:"job_id,omitempty"`
	Reason     string `json:"reason"`
	BodySize   *int   `json:"body_size,omitempty"`
	Cycle      *int   `json:"cycle,omitempty"`
}

func MarshalEnqueue(jobID string) ([]byte, error) {
	if !validHex(jobID, hexIDLen) {
		return nil, ErrInvalidJobID
	}

	body, err := json.Marshal(EnqueueMessage{JobID: jobID})
	if err != nil {
		return nil, fmt.Errorf("marshal enqueue: %w", err)
	}

	return body, nil
}

func ParseEnqueue(body []byte) (string, error) {
	var msg EnqueueMessage

	err := jsonv2.Unmarshal(body, &msg, json.DefaultOptionsV1(), jsonv2.RejectUnknownMembers(true))
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidEnqueue, err)
	}

	if !validHex(msg.JobID, hexIDLen) {
		return "", ErrInvalidJobID
	}

	return msg.JobID, nil
}

func MarshalKnownDLQ(jobID string, cycle int, reason string) ([]byte, error) {
	if !validHex(jobID, hexIDLen) {
		return nil, ErrInvalidJobID
	}

	if cycle < 0 || !knownDLQReason(reason) {
		return nil, ErrInvalidDLQ
	}

	cycleVal := cycle

	body, err := json.Marshal(DLQMessage{
		At:         "",
		BodySHA256: "",
		JobID:      jobID,
		Reason:     reason,
		BodySize:   nil,
		Cycle:      &cycleVal,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal known dlq: %w", err)
	}

	return body, nil
}

func MarshalGhostDLQ(jobID string) ([]byte, error) {
	if !validHex(jobID, hexIDLen) {
		return nil, ErrInvalidJobID
	}

	body, err := json.Marshal(DLQMessage{
		At:         "",
		BodySHA256: "",
		JobID:      jobID,
		Reason:     reasonMissing,
		BodySize:   nil,
		Cycle:      nil,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal ghost dlq: %w", err)
	}

	return body, nil
}

func MarshalMalformedDLQ(raw []byte) ([]byte, error) {
	sum := sha256.Sum256(raw)
	size := len(raw)

	body, err := json.Marshal(DLQMessage{
		At:         "",
		BodySHA256: hex.EncodeToString(sum[:]),
		JobID:      "",
		Reason:     reasonMalformed,
		BodySize:   &size,
		Cycle:      nil,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal malformed dlq: %w", err)
	}

	return body, nil
}

func ParseDLQ(body []byte) (DLQMessage, error) {
	var msg DLQMessage

	err := jsonv2.Unmarshal(body, &msg, json.DefaultOptionsV1(), jsonv2.RejectUnknownMembers(true))
	if err != nil {
		return DLQMessage{}, fmt.Errorf("%w: %w", ErrInvalidDLQ, err)
	}

	err = validateDLQ(&msg)
	if err != nil {
		return DLQMessage{}, err
	}

	return msg, nil
}

func validateDLQ(msg *DLQMessage) error {
	var err error

	switch msg.Reason {
	case reasonMalformed:
		err = validateMalformedDLQ(msg)
	case reasonMissing:
		err = validateGhostDLQ(msg)
	default:
		err = validateKnownDLQ(msg)
	}

	if err != nil {
		return err
	}

	return nil
}

func validateMalformedDLQ(msg *DLQMessage) error {
	if msg.JobID != "" || msg.Cycle != nil || !validHex(msg.BodySHA256, hexHashLen) {
		return ErrInvalidDLQ
	}

	if msg.BodySize == nil || *msg.BodySize < 0 {
		return ErrInvalidDLQ
	}

	return nil
}

func validateGhostDLQ(msg *DLQMessage) error {
	if !validHex(msg.JobID, hexIDLen) || msg.Cycle != nil || msg.BodySHA256 != "" || msg.BodySize != nil {
		return ErrInvalidDLQ
	}

	return nil
}

func validateKnownDLQ(msg *DLQMessage) error {
	if !knownDLQReason(msg.Reason) || !validHex(msg.JobID, hexIDLen) {
		return ErrInvalidDLQ
	}

	if msg.Cycle == nil || *msg.Cycle < 0 || msg.BodySHA256 != "" || msg.BodySize != nil {
		return ErrInvalidDLQ
	}

	return nil
}
