package fxapi

import (
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/flexer2006/hopper/internal/fxboot"
	"github.com/flexer2006/hopper/internal/platform"
)

func Module() fx.Option { //nolint:ireturn // fx.Option is the composition contract.
	return fx.Module("api",
		fx.Provide(platform.NewLogger),
		fx.Invoke(startRelay),
		fx.WithLogger(func(log *zap.Logger) fxevent.Logger {
			zl := new(fxevent.ZapLogger{Logger: log})
			zl.UseLogLevel(zapcore.DebugLevel)

			return zl
		}),
	)
}

func NewApp(opts ...fx.Option) *fx.App {
	return fxboot.NewApp(platform.DefaultAPIShutdownTimeout, platform.APIStopTimeout, Module(), opts...)
}

func Run() error {
	return platform.RunProcess("api", NewApp())
}
