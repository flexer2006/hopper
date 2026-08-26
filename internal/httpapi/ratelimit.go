package httpapi

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type limiter struct {
	now     func() time.Time
	items   map[string]*bucket
	mu      sync.Mutex
	rpm     int
	burst   int
	maxKeys int
}

type bucket struct {
	last     time.Time
	lastSeen time.Time
	tokens   float64
}

const (
	defaultRateRPM   = 100
	defaultRateBurst = 20
	rateMaxKeys      = 4096
	rateIdleTTL      = 10 * time.Minute
	secondsPerMinute = 60
)

func newLimiter(rpm, burst int, now func() time.Time) *limiter {
	lim := new(limiter)
	lim.now = now
	lim.items = make(map[string]*bucket)
	lim.rpm = rpm
	lim.burst = burst
	lim.maxKeys = rateMaxKeys

	return lim
}

func (h *Handler) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := clientIP(r, h.xffHops)
		if !h.limit.allow(key) {
			writeErr(w, http.StatusTooManyRequests, "rate limited", codeRateLimited)

			return
		}

		next.ServeHTTP(w, r)
	})
}

func (rl *limiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.now()

	item, ok := rl.items[key]
	if !ok {
		rl.evict(now)

		item = new(bucket)
		item.last = now
		item.lastSeen = now
		item.tokens = float64(rl.burst)
		rl.items[key] = item
	}

	elapsed := now.Sub(item.last).Seconds()
	if elapsed > 0 {
		item.tokens += elapsed * (float64(rl.rpm) / secondsPerMinute)
		if item.tokens > float64(rl.burst) {
			item.tokens = float64(rl.burst)
		}

		item.last = now
	}

	item.lastSeen = now
	if item.tokens < 1 {
		return false
	}

	item.tokens--

	return true
}

func (rl *limiter) evict(now time.Time) {
	if len(rl.items) < rl.maxKeys {
		return
	}

	for key, item := range rl.items {
		if now.Sub(item.lastSeen) >= rateIdleTTL {
			delete(rl.items, key)
		}
	}

	if len(rl.items) < rl.maxKeys {
		return
	}

	var oldestKey string

	var oldest time.Time

	first := true
	for key, item := range rl.items {
		if first || item.lastSeen.Before(oldest) {
			oldest = item.lastSeen
			oldestKey = key
			first = false
		}
	}

	if oldestKey != "" {
		delete(rl.items, oldestKey)
	}
}

func clientIP(r *http.Request, hops int) string {
	if hops > 0 {
		if ip := xffHop(r.Header.Get("X-Forwarded-For"), hops); ip != "" {
			return ip
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func xffHop(header string, hops int) string {
	if header == "" || hops < 1 {
		return ""
	}

	parts := strings.Split(header, ",")

	idx := len(parts) - hops
	if idx < 0 || idx >= len(parts) {
		return ""
	}

	return strings.TrimSpace(parts[idx])
}
