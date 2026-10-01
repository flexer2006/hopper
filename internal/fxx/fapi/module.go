package fapi

import (
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/flexer2006/hopper/internal/fxx/fboot"
	"github.com/flexer2006/hopper/internal/platform"
)

func Module() fx.Option { //nolint:ireturn // fx.Option is the composition contract.
	return fx.Module("api",
		fx.Provide(platform.NewLogger),
		fx.Provide(newRelayHolder),
		fx.Provide(newEnqueue),
		fx.Provide(newQuery),
		fx.Provide(newReplay),
		fx.Invoke(startHeldRelay),
		fx.Invoke(startHTTP),
		fx.WithLogger(func(log *zap.Logger) fxevent.Logger {
			zl := new(fxevent.ZapLogger{Logger: log})
			zl.UseLogLevel(zapcore.DebugLevel)

			return zl
		}),
	)
}

func NewApp(opts ...fx.Option) *fx.App {
	return fboot.NewApp(platform.DefaultAPIShutdownTimeout, platform.APIStopTimeout, Module(), opts...)
}

func Run() error {
	return platform.RunProcess("api", NewApp(Production()))
}
