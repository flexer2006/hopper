package platform

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(cfg *Config) (*zap.Logger, error) {
	level := zapcore.InfoLevel

	if cfg.LogLevel != "" {
		parsed, err := zapcore.ParseLevel(cfg.LogLevel)
		if err != nil {
			return nil, fmt.Errorf("parse log_level: %w", err)
		}

		level = parsed
	}

	zcfg := zap.NewProductionConfig()
	zcfg.Level = zap.NewAtomicLevelAt(level)
	zcfg.Encoding = "json"
	zcfg.DisableStacktrace = !cfg.LogStackTraces
	zcfg.Sampling = nil
	zcfg.OutputPaths = []string{"stdout"}
	zcfg.ErrorOutputPaths = []string{"stderr"}

	log, err := zcfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build logger: %w", err)
	}

	return log, nil
}
