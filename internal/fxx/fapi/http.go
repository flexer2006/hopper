package fapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
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

	Checks  []httpapi.Checker `group:"health"`
	LC      fx.Lifecycle
	Log     *zap.Logger
	Cfg     *platform.Config
	Enqueue *enqueue.Service `optional:"true"`
	Query   *query.Service   `optional:"true"`
	Replay  *replay.Service  `optional:"true"`
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

	var (
		srv       *http.Server
		stopServe context.CancelFunc
		wg        sync.WaitGroup
	)

	in.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			serveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			stopServe = cancel
			srv = newHTTPServer(serveCtx, in.Cfg.HTTPAddr, handler)

			var lc net.ListenConfig

			ln, err := lc.Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				cancel()

				return fmt.Errorf("http listen: %w", err)
			}

			activeSrv := srv

			wg.Go(func() {
				serveErr := activeSrv.Serve(ln)
				if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
					in.Log.Error("http server", zap.Error(serveErr))
				}
			})

			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), in.Cfg.APIShutdownTimeout)
			defer cancel()

			var err error

			if srv != nil {
				activeSrv := srv
				srv = nil
				err = activeSrv.Shutdown(stopCtx)
			}

			if stopServe != nil {
				stopFn := stopServe
				stopServe = nil

				stopFn()
			}

			waitErr := waitDone(stopCtx, &wg)

			joinErr := errors.Join(err, waitErr)
			if joinErr != nil {
				return fmt.Errorf("http shutdown: %w", joinErr)
			}

			return nil
		},
	})
}

func newHTTPServer(baseCtx context.Context, addr string, handler http.Handler) *http.Server {
	srv := new(http.Server)
	srv.Addr = addr
	srv.Handler = handler
	srv.ReadTimeout = readTimeout
	srv.ReadHeaderTimeout = readHeaderTimeout
	srv.WriteTimeout = writeTimeout
	srv.IdleTimeout = idleTimeout
	srv.MaxHeaderBytes = maxHeaderBytes
	srv.MaxHeaderValueCount = maxHeaderValues
	srv.ErrorLog = log.New(io.Discard, "", 0)
	srv.BaseContext = func(net.Listener) context.Context {
		return baseCtx
	}

	return srv
}

func waitDone(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
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
