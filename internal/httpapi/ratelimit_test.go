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

	quoted := []byte(`{"a":"{b{c{d{e"}`)
	if httpapi.JSONTooDeep(quoted, 2) {
		t.Fatal("braces inside strings counted as depth")
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

	got = httpapi.XFFHop(" , 198.51.100.7, , 10.0.0.1 , ", 2)
	if got != "198.51.100.7" {
		t.Fatalf("empty segments hops=2 got %q, want 198.51.100.7", got)
	}

	got = httpapi.XFFHop("2001:DB8:0:0::1", 1)
	if got != "2001:db8::1" {
		t.Fatalf("ipv6 canonical hop got %q, want 2001:db8::1", got)
	}

	if httpapi.XFFHop("2001:0db8::0001", 1) != got {
		t.Fatal("ipv6 variants did not canonicalize to identical key")
	}

	if httpapi.XFFHop("::ffff:198.51.100.7", 1) != "198.51.100.7" {
		t.Fatal("ipv4-mapped ipv6 hop did not unmap to ipv4")
	}

	if httpapi.XFFHop("198.51.100.7, not-an-ip", 1) != "" || httpapi.XFFHop("2001:db8::1%eth0", 1) != "" {
		t.Fatal("malformed or zoned hop should fall back to empty string")
	}

	if httpapi.XFFHop("1.1.1.1", 3) != "" {
		t.Fatal("over-long hops should fall back")
	}

	if httpapi.XFFHop("", 1) != "" {
		t.Fatal("empty header")
	}
}

func TestXFFHeader(t *testing.T) {
	t.Parallel()

	if got := httpapi.XFFHeader(http.Header{}); got != "" {
		t.Fatalf("empty header map got %q", got)
	}

	singleBlank := http.Header{}
	singleBlank.Add("X-Forwarded-For", "   ")
	if got := httpapi.XFFHeader(singleBlank); got != "" {
		t.Fatalf("single whitespace line got %q", got)
	}

	multi := http.Header{}
	multi.Add("X-Forwarded-For", "")
	multi.Add("X-Forwarded-For", " 203.0.113.10 ")
	multi.Add("X-Forwarded-For", "  \t ")
	multi.Add("X-Forwarded-For", "198.51.100.42")
	if got := httpapi.XFFHeader(multi); got != "203.0.113.10, 198.51.100.42" {
		t.Fatalf("multi header coalescing got %q", got)
	}

	allBlank := http.Header{}
	allBlank.Add("X-Forwarded-For", "")
	allBlank.Add("X-Forwarded-For", "   ")
	if got := httpapi.XFFHeader(allBlank); got != "" {
		t.Fatalf("all-blank multi header got %q", got)
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
