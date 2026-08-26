package fxapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type httpIn struct {
	fx.In

	LC      fx.Lifecycle
	Log     *zap.Logger
	Cfg     *platform.Config
	Enqueue *enqueue.Service  `optional:"true"`
	Query   *query.Service    `optional:"true"`
	Replay  *replay.Service   `optional:"true"`
	Checks  []httpapi.Checker `group:"health"`
}

const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	maxHeaderBytes    = 64 << 10
	maxHeaderValues   = 64
)

func startHTTP(in httpIn) { //nolint:gocritic // hugeParam: fx.In composition
	if in.Cfg == nil {
		return
	}

	handler := httpapi.New(httpOptions(in))

	srv := new(http.Server)
	srv.Addr = in.Cfg.HTTPAddr
	srv.Handler = handler
	srv.ReadTimeout = readTimeout
	srv.ReadHeaderTimeout = readHeaderTimeout
	srv.WriteTimeout = writeTimeout
	srv.IdleTimeout = idleTimeout
	srv.MaxHeaderBytes = maxHeaderBytes
	srv.MaxHeaderValueCount = maxHeaderValues
	srv.ErrorLog = log.New(io.Discard, "", 0)

	var stopServe context.CancelFunc

	in.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			serveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			stopServe = cancel
			srv.BaseContext = func(net.Listener) context.Context {
				return serveCtx
			}

			var lc net.ListenConfig

			ln, err := lc.Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				cancel()

				return fmt.Errorf("http listen: %w", err)
			}

			go func() {
				serveErr := srv.Serve(ln)
				if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
					in.Log.Error("http server", zap.Error(serveErr))
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), in.Cfg.APIShutdownTimeout)
			defer cancel()

			err := srv.Shutdown(stopCtx)

			if stopServe != nil {
				stopServe()
			}

			if err != nil {
				return fmt.Errorf("http shutdown: %w", err)
			}

			return nil
		},
	})
}

func httpOptions(in httpIn) httpapi.Options { //nolint:gocritic // hugeParam: fx.In composition
	opts := new(httpapi.Options)
	opts.Log = in.Log
	opts.Enqueue = in.Enqueue
	opts.Query = in.Query
	opts.Replay = in.Replay
	opts.Checks = in.Checks
	opts.Token = in.Cfg.APIToken
	opts.MaxRequestBytes = in.Cfg.MaxRequestBytes
	opts.MaxPayloadBytes = in.Cfg.MaxPayloadBytes
	opts.JSONMaxDepth = in.Cfg.JSONMaxDepth
	opts.RateLimitRPM = in.Cfg.RateLimitRPM
	opts.RateLimitBurst = in.Cfg.RateLimitBurst
	opts.TrustXFFHops = in.Cfg.TrustXFFHops

	return *opts
}
