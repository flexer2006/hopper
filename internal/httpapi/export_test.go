package httpapi

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

func LimiterCapAllows(now func() time.Time) int {
	rl := newLimiter(100, 1, now)
	rl.maxKeys = 2
	rl.allow("a")
	rl.allow("b")
	rl.allow("c")

	return len(rl.items)
}

func LimiterIdleEvicts(now func() time.Time, advance func(time.Duration)) int {
	rl := newLimiter(100, 1, now)
	rl.maxKeys = 2
	rl.allow("old")
	rl.allow("held")
	advance(rateIdleTTL)
	rl.allow("new")

	rl.mu.Lock()
	defer rl.mu.Unlock()

	_, old := rl.items["old"]
	_, fresh := rl.items["new"]
	if old || !fresh {
		return -1
	}

	return len(rl.items)
}

func JSONTooDeep(raw []byte, maxDepth int) bool {
	return jsonTooDeep(raw, maxDepth)
}

func XFFHop(header string, hops int) string {
	return xffHop(header, hops)
}

func RecovererForTest() http.Handler {
	return RecovererWithLog(nil)
}

func RecovererWithLog(log *zap.Logger) http.Handler {
	h := new(Handler)
	if log == nil {
		log = zap.NewNop()
	}

	h.log = log

	return h.recoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("test")
	}))
}
