package persist //nolint:testpackage // payloadRaw, payloadJSON, insertDoc

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
)

const sizeB = 512

func BenchmarkPayloadRaw512(b *testing.B) {
	raw := benchJSONObject(sizeB)

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		_, err := payloadRaw(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPayloadJSON512(b *testing.B) {
	encoded, err := payloadRaw(benchJSONObject(sizeB))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		out := payloadJSON(encoded)
		if len(out) == 0 {
			b.Fatal("empty payload json")
		}
	}
}

func BenchmarkInsertDoc512(b *testing.B) {
	payload := benchJSONObject(sizeB)

	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	b.ReportAllocs()

	i := 0
	for b.Loop() {
		i++
		_, err := insertDoc(enqueue.Record{
			Payload:     payload,
			ID:          fmt.Sprintf("%024x", i),
			Target:      "https://example.invalid/hop16",
			ProducerKey: fmt.Sprintf("k-%d", i),
			RequestHash: fmt.Sprintf("%064x", i),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		}, now)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchJSONObject(size int) []byte {
	const wrap = `{"pad":""}`

	inner := max(size-len(wrap), 1)

	return []byte(`{"pad":"` + strings.Repeat("a", inner) + `"}`)
}
