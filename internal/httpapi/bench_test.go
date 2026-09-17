package httpapi_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

func BenchmarkCreateJob(b *testing.B) {
	clk := &frozenClock{ts: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
	st := persist.NewMemory(clk.now, 30*time.Second)
	broker := new(recBroker)
	rel := dispatch.NewRelay(st, broker, dispatch.Config{
		Interval: time.Hour,
		Healing:  30 * time.Second,
		Limit:    8,
	}, nil)
	handler := httpapi.New(httpapi.Options{
		Now:             clk.now,
		Enqueue:         enqueue.NewService(st, rel),
		Query:           query.NewService(st),
		Replay:          replay.NewService(st, rel),
		Token:           platform.ValidToken(),
		MaxRequestBytes: 1 << 20,
		MaxPayloadBytes: 1 << 18,
		RateLimitRPM:    10_000_000,
		RateLimitBurst:  10_000_000,
	})
	body := `{"type":"http_post","target":"https://example.invalid/hop16","payload":{"pad":"` +
		strings.Repeat("a", 400) + `"},"max_attempts":1}`
	token := platform.ValidToken()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		req := httptest.NewRequestWithContext(
			b.Context(),
			http.MethodPost,
			"/v1/jobs",
			strings.NewReader(body),
		)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("hop16-%d", i))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		res := rec.Result()
		_, err := io.Copy(io.Discard, res.Body)
		closeErr := res.Body.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		if res.StatusCode != http.StatusAccepted {
			b.Fatalf("status = %d", res.StatusCode)
		}
	}
}
