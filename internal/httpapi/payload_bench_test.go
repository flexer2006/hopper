package httpapi //nolint:testpackage // boundPayload, jsonTooDeep, requestHash

import (
	"encoding/json"
	"strings"
	"testing"
)

func BenchmarkBoundPayload512(b *testing.B) {
	raw := json.RawMessage(`{"pad":"` + strings.Repeat("a", 503) + `"}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		_, err := boundPayload(raw, defaultMaxPayloadBytes)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestHash(b *testing.B) {
	raw := []byte(`{"type":"http_post","target":"https://example.invalid/hop16","payload":{"pad":"` +
		strings.Repeat("a", 400) + `"}}`)
	b.ReportAllocs()
	for b.Loop() {
		if requestHash(raw) == "" {
			b.Fatal("empty hash")
		}
	}
}

func BenchmarkJSONTooDeep(b *testing.B) {
	raw := []byte(`{"type":"http_post","target":"https://example.invalid/hop16","payload":{"pad":"` +
		strings.Repeat("a", 400) + `"}}`)
	b.ReportAllocs()
	for b.Loop() {
		if jsonTooDeep(raw, defaultJSONMaxDepth) {
			b.Fatal("depth")
		}
	}
}
