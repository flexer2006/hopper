package broker_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/flexer2006/hopper/internal/broker"
)

const (
	testJobID = "aaaaaaaaaaaaaaaaaaaaaaaa"
	testHash  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestMarshalParseEnqueueContract(t *testing.T) {
	t.Parallel()

	body, err := broker.MarshalEnqueue(testJobID)
	if err != nil {
		t.Fatalf("MarshalEnqueue: %v", err)
	}

	var raw map[string]any

	err = json.Unmarshal(body, &raw)
	if err != nil {
		t.Fatalf("json: %v", err)
	}

	if len(raw) != 1 || raw["job_id"] != testJobID {
		t.Fatalf("enqueue body = %s", body)
	}

	got, err := broker.ParseEnqueue(body)
	if err != nil {
		t.Fatalf("ParseEnqueue: %v", err)
	}

	if got != testJobID {
		t.Fatalf("job id = %s", got)
	}
}

func TestParseEnqueueRejectsUnknownAndInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want error
	}{
		{
			name: "unknown field AT-CONTRACT-04",
			body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","cycle":0}`,
			want: broker.ErrInvalidEnqueue,
		},
		{name: "empty", body: ``, want: broker.ErrInvalidEnqueue},
		{name: "array", body: `[]`, want: broker.ErrInvalidEnqueue},
		{name: "trailing", body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}{}`, want: broker.ErrInvalidEnqueue},
		{name: "uppercase", body: `{"job_id":"AAAAAAAAAAAAAAAAaaaaaaaa"}`, want: broker.ErrInvalidJobID},
		{name: "short", body: `{"job_id":"abc"}`, want: broker.ErrInvalidJobID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := broker.ParseEnqueue([]byte(tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	_, err := broker.MarshalEnqueue("not-a-job-id")
	if !errors.Is(err, broker.ErrInvalidJobID) {
		t.Fatalf("MarshalEnqueue invalid = %v", err)
	}
}

func TestDLQKnownGhostMalformed(t *testing.T) {
	t.Parallel()

	known, err := broker.MarshalKnownDLQ(testJobID, 0, "attempts_exhausted")
	if err != nil {
		t.Fatalf("known: %v", err)
	}

	if !bytes.Contains(known, []byte(`"cycle":0`)) {
		t.Fatalf("cycle 0 omitted: %s", known)
	}

	parsed, err := broker.ParseDLQ(known)
	if err != nil {
		t.Fatalf("ParseDLQ known: %v", err)
	}

	if parsed.JobID != testJobID || parsed.Reason != "attempts_exhausted" || parsed.Cycle == nil || *parsed.Cycle != 0 {
		t.Fatalf("known = %+v", parsed)
	}

	ghost, err := broker.MarshalGhostDLQ(testJobID)
	if err != nil {
		t.Fatalf("ghost: %v", err)
	}

	if bytes.Contains(ghost, []byte("cycle")) {
		t.Fatalf("ghost must omit cycle: %s", ghost)
	}

	gparsed, err := broker.ParseDLQ(ghost)
	if err != nil {
		t.Fatalf("ParseDLQ ghost: %v", err)
	}

	if gparsed.Reason != "missing_document" || gparsed.JobID != testJobID {
		t.Fatalf("ghost = %+v", gparsed)
	}

	secret := []byte(`{"password":"hunter2","job_id":"should-not-leak"}`)
	malformed, err := broker.MarshalMalformedDLQ(secret)
	if err != nil {
		t.Fatalf("malformed: %v", err)
	}

	if bytes.Contains(malformed, secret) || strings.Contains(string(malformed), "hunter2") ||
		strings.Contains(string(malformed), "password") {
		t.Fatalf("malformed leaked raw body: %s", malformed)
	}

	sum := sha256.Sum256(secret)
	wantHash := hex.EncodeToString(sum[:])

	mparsed, err := broker.ParseDLQ(malformed)
	if err != nil {
		t.Fatalf("ParseDLQ malformed: %v", err)
	}

	if mparsed.Reason != "malformed_message" || mparsed.BodySHA256 != wantHash || mparsed.BodySize == nil ||
		*mparsed.BodySize != len(secret) {
		t.Fatalf("malformed = %+v", mparsed)
	}

	if mparsed.JobID != "" {
		t.Fatalf("malformed must not carry job_id")
	}
}

func TestParseDLQRejectsCrossShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "known missing cycle", body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","reason":"terminal_http"}`},
		{name: "ghost with cycle", body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","reason":"missing_document","cycle":1}`},
		{
			name: "malformed with job_id",
			body: `{"reason":"malformed_message","body_sha256":"` + testHash + `","body_size":1,"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`,
		},
		{name: "unknown reason", body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","cycle":1,"reason":"nope"}`},
		{
			name: "unknown field",
			body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","cycle":1,"reason":"terminal_http","payload":"x"}`,
		},
		{name: "negative cycle", body: `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","cycle":-1,"reason":"operator_manual"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := broker.ParseDLQ([]byte(tc.body))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}

	_, err := broker.MarshalKnownDLQ(testJobID, -1, "terminal_http")
	if !errors.Is(err, broker.ErrInvalidDLQ) {
		t.Fatalf("negative cycle marshal = %v", err)
	}

	_, err = broker.MarshalKnownDLQ(testJobID, 0, "missing_document")
	if !errors.Is(err, broker.ErrInvalidDLQ) {
		t.Fatalf("ghost reason on known marshal = %v", err)
	}

	_, err = broker.MarshalGhostDLQ("short")
	if !errors.Is(err, broker.ErrInvalidJobID) {
		t.Fatalf("ghost id = %v", err)
	}
}

func FuzzParseEnqueue(f *testing.F) {
	f.Add([]byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa","x":1}`))
	f.Add([]byte(`null`))
	f.Add([]byte{0xff, 0xfe})

	f.Fuzz(func(t *testing.T, body []byte) {
		id, err := broker.ParseEnqueue(body)
		if err != nil {
			return
		}

		if len(id) != 24 {
			t.Fatalf("id len = %d", len(id))
		}

		for i := range id {
			char := id[i]
			if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
				t.Fatalf("non-hex id %q", id)
			}
		}
	})
}
