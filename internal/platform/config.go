package platform

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/config"
)

type Config struct {
	APIToken                  string        `yaml:"api_token"`
	LogLevel                  string        `yaml:"log_level"`
	APIShutdownTimeoutYAML    string        `yaml:"api_shutdown_timeout"`
	WorkerShutdownTimeoutYAML string        `yaml:"worker_shutdown_timeout"`
	APIShutdownTimeout        time.Duration `yaml:"-"`
	WorkerShutdownTimeout     time.Duration `yaml:"-"`
	LogStackTraces            bool          `yaml:"log_stack_traces"`
}

const (
	ConfigFileEnv                = "HOPPER_CONFIG_FILE"
	APITokenEnv                  = "HOPPER_API_TOKEN"
	APIShutdownTimeoutEnv        = "HOPPER_API_SHUTDOWN_TIMEOUT"
	WorkerShutdownTimeoutEnv     = "HOPPER_WORKER_SHUTDOWN_TIMEOUT"
	MinAPITokenBytes             = 32
	DefaultAPIShutdownTimeout    = 10 * time.Second
	DefaultWorkerShutdownTimeout = 30 * time.Second
)

func Load() (Config, error) {
	path := os.Getenv(ConfigFileEnv)
	if path == "" {
		return Config{}, fmt.Errorf("%w: %s is unset", ErrConfig, ConfigFileEnv)
	}

	return LoadFile(path)
}

func LoadFile(path string) (Config, error) {
	provider, err := config.NewYAML(config.File(path))
	if err != nil {
		return Config{}, fmt.Errorf("%w: read %s: %w", ErrConfig, path, err)
	}

	var cfg Config

	popErr := provider.Get("").Populate(&cfg)
	if popErr != nil {
		return Config{}, fmt.Errorf("%w: populate %s: %w", ErrConfig, path, popErr)
	}

	if token := os.Getenv(APITokenEnv); token != "" {
		cfg.APIToken = token
	}

	applyErr := cfg.applyTimeouts()
	if applyErr != nil {
		return Config{}, applyErr
	}

	valErr := cfg.validate()
	if valErr != nil {
		return Config{}, valErr
	}

	return cfg, nil
}

func (cfg *Config) applyTimeouts() error {
	apiTimeout, apiErr := resolveDuration(
		cfg.APIShutdownTimeoutYAML,
		"api_shutdown_timeout",
		APIShutdownTimeoutEnv,
		DefaultAPIShutdownTimeout,
	)
	if apiErr != nil {
		return apiErr
	}

	workerTimeout, workerErr := resolveDuration(
		cfg.WorkerShutdownTimeoutYAML,
		"worker_shutdown_timeout",
		WorkerShutdownTimeoutEnv,
		DefaultWorkerShutdownTimeout,
	)
	if workerErr != nil {
		return workerErr
	}

	cfg.APIShutdownTimeout = apiTimeout
	cfg.WorkerShutdownTimeout = workerTimeout

	return nil
}

func resolveDuration(yamlRaw, yamlKey, envName string, fallback time.Duration) (time.Duration, error) {
	raw := yamlRaw
	source := yamlKey

	if env := os.Getenv(envName); env != "" {
		raw = env
		source = envName
	}

	if raw == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: parse %s %q: %w", ErrConfig, source, raw, err)
	}

	if parsed <= 0 {
		return 0, fmt.Errorf("%w: %s must be positive", ErrConfig, source)
	}

	return parsed, nil
}

func (cfg *Config) validate() error {
	if len(cfg.APIToken) < MinAPITokenBytes {
		return ErrAPIToken
	}

	return nil
}

func APIStopTimeout(cfg *Config) time.Duration {
	return cfg.APIShutdownTimeout
}

func WorkerStopTimeout(cfg *Config) time.Duration {
	return cfg.WorkerShutdownTimeout
}
