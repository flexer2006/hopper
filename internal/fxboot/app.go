package fxboot

import (
	"time"

	"go.uber.org/fx"

	"github.com/flexer2006/hopper/internal/platform"
)

const baseOptionCount = 3

func NewApp(
	fallback time.Duration,
	pick func(*platform.Config) time.Duration,
	module fx.Option,
	opts ...fx.Option,
) *fx.App {
	cfg, loadErr := platform.Load()
	timeout := fallback

	if loadErr == nil {
		timeout = pick(new(cfg))
	}

	base := make([]fx.Option, 0, baseOptionCount+len(opts))
	base = append(base,
		fx.StopTimeout(timeout),
		fx.Provide(func() (*platform.Config, error) {
			if loadErr != nil {
				return nil, loadErr
			}

			return new(cfg), nil
		}),
		module,
	)
	base = append(base, opts...)

	return fx.New(base...)
}
