package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type Handler struct {
	checks                                 []Checker
	token                                  []byte
	log                                    *zap.Logger
	enqueue                                *enqueue.Service
	query                                  *query.Service
	replay                                 *replay.Service
	limit                                  *limiter
	maxBody, maxPayload, maxDepth, xffHops int
}

type Options struct {
	Checks                                                                                     []Checker
	Token                                                                                      string
	Log                                                                                        *zap.Logger
	Now                                                                                        func() time.Time
	Enqueue                                                                                    *enqueue.Service
	Query                                                                                      *query.Service
	Replay                                                                                     *replay.Service
	MaxRequestBytes, MaxPayloadBytes, JSONMaxDepth, RateLimitRPM, RateLimitBurst, TrustXFFHops int
}

const (
	jobIDLen          = 24
	maxIdempotencyKey = 256
	statusDeadQ       = "dead"
	listStatusKey     = "status"
)

func New(opts Options) http.Handler { //nolint:gocritic // hugeParam: Options is the constructor bag
	log := opts.Log
	if log == nil {
		log = zap.NewNop()
	}

	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	maxBody := opts.MaxRequestBytes
	if maxBody < 1 {
		maxBody = defaultMaxRequestBytes
	}

	maxPayload := opts.MaxPayloadBytes
	if maxPayload < 1 {
		maxPayload = defaultMaxPayloadBytes
	}

	maxDepth := opts.JSONMaxDepth
	if maxDepth < 1 {
		maxDepth = defaultJSONMaxDepth
	}

	rpm := opts.RateLimitRPM
	if rpm < 1 {
		rpm = defaultRateRPM
	}

	burst := opts.RateLimitBurst
	if burst < 1 {
		burst = defaultRateBurst
	}

	handler := new(Handler{
		log:        log,
		enqueue:    opts.Enqueue,
		query:      opts.Query,
		replay:     opts.Replay,
		limit:      newLimiter(rpm, burst, now),
		checks:     opts.Checks,
		token:      []byte(opts.Token),
		maxBody:    maxBody,
		maxPayload: maxPayload,
		maxDepth:   maxDepth,
		xffHops:    opts.TrustXFFHops,
	})

	mux := chi.NewRouter()
	mux.Use(handler.recoverer)
	mux.Get("/healthz", handler.health)
	mux.Route("/v1", func(r chi.Router) {
		r.Use(handler.rateLimit)
		r.Use(handler.bearer)
		r.Post("/jobs", handler.createJob)
		r.Get("/jobs", handler.listJobs)
		r.Get("/jobs/{id}", handler.getJob)
		r.Post("/jobs/{id}/replay", handler.replayJob)
	})

	return mux
}
