package platform

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"go.uber.org/config"
)

type Config struct {
	APIToken                  string        `yaml:"api_token"`
	LogLevel                  string        `yaml:"log_level"`
	APIShutdownTimeoutYAML    string        `yaml:"api_shutdown_timeout"`
	WorkerShutdownTimeoutYAML string        `yaml:"worker_shutdown_timeout"`
	RelayIntervalYAML         string        `yaml:"relay_interval"`
	HealingIntervalYAML       string        `yaml:"healing_interval"`
	LeaseScanIntervalYAML     string        `yaml:"lease_scan_interval"`
	HTTPAddr                  string        `yaml:"http_addr"`
	APIShutdownTimeout        time.Duration `yaml:"-"`
	WorkerShutdownTimeout     time.Duration `yaml:"-"`
	RelayInterval             time.Duration `yaml:"-"`
	HealingInterval           time.Duration `yaml:"-"`
	LeaseScanInterval         time.Duration `yaml:"-"`
	MaxRequestBytes           int           `yaml:"max_request_bytes"`
	MaxPayloadBytes           int           `yaml:"max_payload_bytes"`
	JSONMaxDepth              int           `yaml:"json_max_depth"`
	RateLimitRPM              int           `yaml:"rate_limit_rpm"`
	RateLimitBurst            int           `yaml:"rate_limit_burst"`
	TrustXFFHops              int           `yaml:"trust_xff_hops"`
	LogStackTraces            bool          `yaml:"log_stack_traces"`
}

const (
	ConfigFileEnv                = "HOPPER_CONFIG_FILE"
	APITokenEnv                  = "HOPPER_API_TOKEN"
	APIShutdownTimeoutEnv        = "HOPPER_API_SHUTDOWN_TIMEOUT"
	WorkerShutdownTimeoutEnv     = "HOPPER_WORKER_SHUTDOWN_TIMEOUT"
	RelayIntervalEnv             = "HOPPER_RELAY_INTERVAL"
	HealingIntervalEnv           = "HOPPER_HEALING_INTERVAL"
	LeaseScanIntervalEnv         = "HOPPER_LEASE_SCAN_INTERVAL"
	HTTPAddrEnv                  = "HOPPER_HTTP_ADDR"
	MaxRequestBytesEnv           = "HOPPER_MAX_REQUEST_BYTES"
	MaxPayloadBytesEnv           = "HOPPER_MAX_PAYLOAD_BYTES"
	JSONMaxDepthEnv              = "HOPPER_JSON_MAX_DEPTH"
	RateLimitRPMEnv              = "HOPPER_RATE_LIMIT_RPM"
	RateLimitBurstEnv            = "HOPPER_RATE_LIMIT_BURST"
	TrustXFFHopsEnv              = "HOPPER_TRUST_XFF_HOPS"
	MinAPITokenBytes             = 32
	DefaultAPIShutdownTimeout    = 10 * time.Second
	DefaultWorkerShutdownTimeout = 30 * time.Second
	DefaultRelayInterval         = 2 * time.Second
	DefaultHealingInterval       = 30 * time.Second
	DefaultLeaseScanInterval     = 5 * time.Second
	DefaultHTTPAddr              = ":9999"
	DefaultMaxRequestBytes       = 524288
	DefaultMaxPayloadBytes       = 262144
	DefaultJSONMaxDepth          = 64
	DefaultRateLimitRPM          = 100
	DefaultRateLimitBurst        = 20
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

	relay, relayErr := resolveDuration(
		cfg.RelayIntervalYAML,
		"relay_interval",
		RelayIntervalEnv,
		DefaultRelayInterval,
	)
	if relayErr != nil {
		return relayErr
	}

	healing, healingErr := resolveDuration(
		cfg.HealingIntervalYAML,
		"healing_interval",
		HealingIntervalEnv,
		DefaultHealingInterval,
	)
	if healingErr != nil {
		return healingErr
	}

	lease, leaseErr := resolveDuration(
		cfg.LeaseScanIntervalYAML,
		"lease_scan_interval",
		LeaseScanIntervalEnv,
		DefaultLeaseScanInterval,
	)
	if leaseErr != nil {
		return leaseErr
	}

	cfg.RelayInterval = relay
	cfg.HealingInterval = healing
	cfg.LeaseScanInterval = lease

	return cfg.applyHTTP()
}

func (cfg *Config) applyHTTP() error {
	if addr := os.Getenv(HTTPAddrEnv); addr != "" {
		cfg.HTTPAddr = addr
	}

	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = DefaultHTTPAddr
	}

	var err error

	cfg.MaxRequestBytes, err = resolveInt(cfg.MaxRequestBytes, MaxRequestBytesEnv, DefaultMaxRequestBytes)
	if err != nil {
		return err
	}

	cfg.MaxPayloadBytes, err = resolveInt(cfg.MaxPayloadBytes, MaxPayloadBytesEnv, DefaultMaxPayloadBytes)
	if err != nil {
		return err
	}

	cfg.JSONMaxDepth, err = resolveInt(cfg.JSONMaxDepth, JSONMaxDepthEnv, DefaultJSONMaxDepth)
	if err != nil {
		return err
	}

	cfg.RateLimitRPM, err = resolveInt(cfg.RateLimitRPM, RateLimitRPMEnv, DefaultRateLimitRPM)
	if err != nil {
		return err
	}

	cfg.RateLimitBurst, err = resolveInt(cfg.RateLimitBurst, RateLimitBurstEnv, DefaultRateLimitBurst)
	if err != nil {
		return err
	}

	cfg.TrustXFFHops, err = resolveInt(cfg.TrustXFFHops, TrustXFFHopsEnv, 0)

	return err
}

func resolveInt(yamlVal int, envName string, fallback int) (int, error) {
	raw := os.Getenv(envName)
	if raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("%w: parse %s %q: %w", ErrConfig, envName, raw, err)
		}

		if parsed < 0 {
			return 0, fmt.Errorf("%w: %s must be >= 0", ErrConfig, envName)
		}

		return parsed, nil
	}

	if yamlVal == 0 {
		return fallback, nil
	}

	if yamlVal < 0 {
		return 0, fmt.Errorf("%w: %s must be >= 0", ErrConfig, envName)
	}

	return yamlVal, nil
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

	if cfg.JSONMaxDepth < 1 {
		return fmt.Errorf("%w: json_max_depth must be >= 1", ErrConfig)
	}

	if cfg.MaxRequestBytes < 1 || cfg.MaxPayloadBytes < 1 {
		return fmt.Errorf("%w: byte caps must be >= 1", ErrConfig)
	}

	if cfg.RateLimitRPM < 1 || cfg.RateLimitBurst < 1 {
		return fmt.Errorf("%w: rate limit must be >= 1", ErrConfig)
	}

	return nil
}

func APIStopTimeout(cfg *Config) time.Duration {
	return cfg.APIShutdownTimeout
}

func WorkerStopTimeout(cfg *Config) time.Duration {
	return cfg.WorkerShutdownTimeout
}
