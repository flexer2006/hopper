package enqueue_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
)

func BenchmarkEnqueueMemory(b *testing.B) {
	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	svc := enqueue.NewService(st, stubPub{})
	payload := []byte(`{"pad":"` + strings.Repeat("a", 503) + `"}`)
	ctx := b.Context()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		_, err := svc.Enqueue(ctx, enqueue.Record{
			Payload:     payload,
			ID:          fmt.Sprintf("%024x", i),
			Target:      "https://example.invalid/hop16",
			ProducerKey: fmt.Sprintf("k-%d", i),
			RequestHash: fmt.Sprintf("%064x", i),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
