package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/httpapi"
)

func TestLimiterEvictsAtCap(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	if httpapi.LimiterCapAllows(clk.now) != 2 {
		t.Fatal("cap did not stay bounded")
	}
}

func TestLimiterIdleTTL(t *testing.T) {
	t.Parallel()

	clk := &frozenClock{ts: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	n := httpapi.LimiterIdleEvicts(clk.now, clk.add)
	if n != 1 {
		t.Fatalf("idle evict size = %d", n)
	}
}

func TestJSONTooDeep(t *testing.T) {
	t.Parallel()

	shallow := []byte(`{"a":{"b":1}}`)
	if httpapi.JSONTooDeep(shallow, 8) {
		t.Fatal("shallow rejected")
	}

	deep := []byte(`{"a":{"b":{"c":{"d":1}}}}`)
	if !httpapi.JSONTooDeep(deep, 3) {
		t.Fatal("deep accepted")
	}

	if !httpapi.JSONTooDeep([]byte(`{`), 8) {
		t.Fatal("truncated accepted")
	}
}

func TestXFFHopFromRight(t *testing.T) {
	t.Parallel()

	got := httpapi.XFFHop("1.1.1.1, 2.2.2.2, 3.3.3.3", 1)
	if got != "3.3.3.3" {
		t.Fatalf("hops=1 got %q", got)
	}

	got = httpapi.XFFHop("1.1.1.1, 2.2.2.2, 3.3.3.3", 2)
	if got != "2.2.2.2" {
		t.Fatalf("hops=2 got %q", got)
	}

	if httpapi.XFFHop("1.1.1.1", 3) != "" {
		t.Fatal("over-long hops should fall back")
	}

	if httpapi.XFFHop("", 1) != "" {
		t.Fatal("empty header")
	}
}

func TestRecoverer(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic", http.NoBody)
	rec := httptest.NewRecorder()
	httpapi.RecovererForTest().ServeHTTP(rec, req)

	res := rec.Result()
	got := decodeErr(t, res)
	if res.StatusCode != http.StatusInternalServerError || got.Code != "service_unavailable" {
		t.Fatalf("panic status=%d body=%+v", res.StatusCode, got)
	}
}
