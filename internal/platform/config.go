package platform

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"go.uber.org/config"

	"github.com/flexer2006/hopper/internal/domain"
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
	MongoURI                  string        `yaml:"mongo_uri"`
	AMQPURI                   string        `yaml:"amqp_uri"`
	MongoDatabase             string        `yaml:"mongo_database"`
	MongoJobsCollection       string        `yaml:"mongo_jobs_collection"`
	Prefetch                  int           `yaml:"-"`
	PrefetchYAML              *int          `yaml:"prefetch"`
	WorkerID                  string        `yaml:"worker_id"`
	HTTPTimeoutYAML           string        `yaml:"http_timeout"`
	MongoOutcomeTimeoutYAML   string        `yaml:"mongo_outcome_timeout"`
	PublishConfirmTimeoutYAML string        `yaml:"publish_confirm_timeout"`
	ClaimLeaseYAML            string        `yaml:"claim_lease"`
	QueueJobs                 string        `yaml:"queue_jobs"`
	QueueDLQ                  string        `yaml:"queue_dlq"`
	ExchangeDelay             string        `yaml:"exchange_delay"`
	HTTPTimeout               time.Duration `yaml:"-"`
	MongoOutcomeTimeout       time.Duration `yaml:"-"`
	PublishConfirmTimeout     time.Duration `yaml:"-"`
	ClaimLease                time.Duration `yaml:"-"`
	MaxResponseBytes          int           `yaml:"-"`
	MaxResponseBytesYAML      *int          `yaml:"max_response_bytes"`
	ReplayMax                 int           `yaml:"-"`
	ReplayMaxYAML             *int          `yaml:"replay_max"`
	RetentionDays             int           `yaml:"retention_days"`
	RetentionEnabled          bool          `yaml:"retention_enabled"`
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
	MongoURIEnv                  = "HOPPER_MONGO_URI"
	AMQPURIEnv                   = "HOPPER_AMQP_URI"
	MongoDatabaseEnv             = "HOPPER_MONGO_DATABASE"
	MongoJobsCollectionEnv       = "HOPPER_MONGO_JOBS_COLLECTION"
	PrefetchEnv                  = "HOPPER_PREFETCH"
	WorkerIDEnv                  = "HOPPER_WORKER_ID"
	HTTPTimeoutEnv               = "HOPPER_HTTP_TIMEOUT"
	MaxResponseBytesEnv          = "HOPPER_MAX_RESPONSE_BYTES"
	MongoOutcomeTimeoutEnv       = "HOPPER_MONGO_OUTCOME_TIMEOUT"
	PublishConfirmTimeoutEnv     = "HOPPER_PUBLISH_CONFIRM_TIMEOUT"
	ClaimLeaseEnv                = "HOPPER_CLAIM_LEASE"
	QueueJobsEnv                 = "HOPPER_QUEUE_JOBS"
	QueueDLQEnv                  = "HOPPER_QUEUE_DLQ"
	ExchangeDelayEnv             = "HOPPER_EXCHANGE_DELAY"
	RetentionEnabledEnv          = "HOPPER_RETENTION_ENABLED"
	RetentionDaysEnv             = "HOPPER_RETENTION_DAYS"
	ReplayMaxEnv                 = "HOPPER_REPLAY_MAX"
	MinAPITokenBytes             = 32
	DefaultAPIShutdownTimeout    = 10 * time.Second
	DefaultWorkerShutdownTimeout = 30 * time.Second
	DefaultRelayInterval         = 2 * time.Second
	DefaultHealingInterval       = 30 * time.Second
	DefaultLeaseScanInterval     = 5 * time.Second
	DefaultHTTPTimeout           = 10 * time.Second
	DefaultMongoOutcomeTimeout   = 5 * time.Second
	DefaultPublishConfirmTimeout = 5 * time.Second
	DefaultClaimLease            = 30 * time.Second
	DefaultHTTPAddr              = ":9999"
	DefaultMaxRequestBytes       = 524288
	DefaultMaxPayloadBytes       = 262144
	DefaultMaxResponseBytes      = 1048576
	DefaultJSONMaxDepth          = 64
	DefaultRateLimitRPM          = 100
	DefaultRateLimitBurst        = 20
	DefaultReplayMax             = 20
	leaseBudgetMargin            = 5 * time.Second // FR-46; keep equal to persist.leaseMargin
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

func (cfg *Config) ValidateInfrastructure() error {
	if cfg.MongoURI == "" || cfg.AMQPURI == "" {
		return fmt.Errorf("%w: mongo_uri and amqp_uri are required for production", ErrConfig)
	}

	return nil
}

func (cfg *Config) AttemptBudget() time.Duration {
	return cfg.HTTPTimeout + cfg.MongoOutcomeTimeout + cfg.PublishConfirmTimeout + leaseBudgetMargin
}

func APIStopTimeout(cfg *Config) time.Duration {
	return cfg.APIShutdownTimeout
}

func WorkerStopTimeout(cfg *Config) time.Duration {
	return cfg.WorkerShutdownTimeout
}

func (cfg *Config) FillRuntimeDefaults() {
	if cfg == nil {
		return
	}

	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = DefaultHTTPTimeout
	}

	if cfg.MongoOutcomeTimeout == 0 {
		cfg.MongoOutcomeTimeout = DefaultMongoOutcomeTimeout
	}

	if cfg.PublishConfirmTimeout == 0 {
		cfg.PublishConfirmTimeout = DefaultPublishConfirmTimeout
	}

	if cfg.ClaimLease == 0 {
		cfg.ClaimLease = DefaultClaimLease
	}

	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = DefaultMaxResponseBytes
	}

	if cfg.ReplayMax == 0 {
		cfg.ReplayMax = DefaultReplayMax
	}

	if cfg.QueueJobs == "" {
		cfg.QueueJobs = domain.QueueJobs
	}

	if cfg.QueueDLQ == "" {
		cfg.QueueDLQ = domain.QueueDLQ
	}
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

	return cfg.applyDeliveryTimeouts()
}

func (cfg *Config) applyDeliveryTimeouts() error {
	httpTimeout, httpErr := resolveDuration(
		cfg.HTTPTimeoutYAML,
		"http_timeout",
		HTTPTimeoutEnv,
		DefaultHTTPTimeout,
	)
	if httpErr != nil {
		return httpErr
	}

	outcomeTimeout, outcomeErr := resolveDuration(
		cfg.MongoOutcomeTimeoutYAML,
		"mongo_outcome_timeout",
		MongoOutcomeTimeoutEnv,
		DefaultMongoOutcomeTimeout,
	)
	if outcomeErr != nil {
		return outcomeErr
	}

	confirmTimeout, confirmErr := resolveDuration(
		cfg.PublishConfirmTimeoutYAML,
		"publish_confirm_timeout",
		PublishConfirmTimeoutEnv,
		DefaultPublishConfirmTimeout,
	)
	if confirmErr != nil {
		return confirmErr
	}

	claimLease, claimErr := resolveDuration(
		cfg.ClaimLeaseYAML,
		"claim_lease",
		ClaimLeaseEnv,
		DefaultClaimLease,
	)
	if claimErr != nil {
		return claimErr
	}

	cfg.HTTPTimeout = httpTimeout
	cfg.MongoOutcomeTimeout = outcomeTimeout
	cfg.PublishConfirmTimeout = confirmTimeout
	cfg.ClaimLease = claimLease

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
	if err != nil {
		return err
	}

	cfg.MongoURI = resolveString(cfg.MongoURI, MongoURIEnv, "")
	cfg.AMQPURI = resolveString(cfg.AMQPURI, AMQPURIEnv, "")
	cfg.MongoDatabase = resolveString(cfg.MongoDatabase, MongoDatabaseEnv, "hopper")
	cfg.MongoJobsCollection = resolveString(cfg.MongoJobsCollection, MongoJobsCollectionEnv, "jobs")

	prefetchYAML := 0
	if cfg.PrefetchYAML != nil {
		prefetchYAML = *cfg.PrefetchYAML
	}

	cfg.Prefetch, err = resolvePrefetch(prefetchYAML, cfg.PrefetchYAML != nil)
	if err != nil {
		return err
	}

	return cfg.applyDocumentedKnobs()
}

func (cfg *Config) applyDocumentedKnobs() error {
	var err error

	cfg.MaxResponseBytes, err = resolvePositive(
		cfg.MaxResponseBytesYAML,
		MaxResponseBytesEnv,
		DefaultMaxResponseBytes,
		"max_response_bytes",
	)
	if err != nil {
		return err
	}

	cfg.ReplayMax, err = resolvePositive(
		cfg.ReplayMaxYAML,
		ReplayMaxEnv,
		DefaultReplayMax,
		"replay_max",
	)
	if err != nil {
		return err
	}

	cfg.RetentionDays, err = resolveInt(cfg.RetentionDays, RetentionDaysEnv, 0)
	if err != nil {
		return err
	}

	cfg.RetentionEnabled, err = resolveBool(cfg.RetentionEnabled, RetentionEnabledEnv)
	if err != nil {
		return err
	}

	cfg.WorkerID = resolveString(cfg.WorkerID, WorkerIDEnv, "")
	cfg.QueueJobs = resolveString(cfg.QueueJobs, QueueJobsEnv, domain.QueueJobs)
	cfg.QueueDLQ = resolveString(cfg.QueueDLQ, QueueDLQEnv, domain.QueueDLQ)
	cfg.ExchangeDelay = resolveString(cfg.ExchangeDelay, ExchangeDelayEnv, "")

	return nil
}

func resolvePrefetch(yamlVal int, hasYAML bool) (int, error) {
	if raw := os.Getenv(PrefetchEnv); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("%w: parse %s %q: %w", ErrConfig, PrefetchEnv, raw, err)
		}

		if parsed < 1 {
			return 0, fmt.Errorf("%w: %s must be >= 1", ErrConfig, PrefetchEnv)
		}

		return parsed, nil
	}

	if !hasYAML {
		return 1, nil
	}

	if yamlVal < 1 {
		return 0, fmt.Errorf("%w: prefetch must be >= 1", ErrConfig)
	}

	return yamlVal, nil
}

func resolvePositive(yamlVal *int, envName string, fallback int, yamlKey string) (int, error) {
	if raw := os.Getenv(envName); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("%w: parse %s %q: %w", ErrConfig, envName, raw, err)
		}

		if parsed < 1 {
			return 0, fmt.Errorf("%w: %s must be >= 1", ErrConfig, envName)
		}

		return parsed, nil
	}

	if yamlVal == nil {
		return fallback, nil
	}

	if *yamlVal < 1 {
		return 0, fmt.Errorf("%w: %s must be >= 1", ErrConfig, yamlKey)
	}

	return *yamlVal, nil
}

func resolveString(yamlVal, envName, fallback string) string {
	if raw := os.Getenv(envName); raw != "" {
		return raw
	}

	if yamlVal == "" {
		return fallback
	}

	return yamlVal
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

func resolveBool(yamlVal bool, envName string) (bool, error) {
	raw := os.Getenv(envName)
	if raw == "" {
		return yamlVal, nil
	}

	switch raw {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("%w: parse %s %q", ErrConfig, envName, raw)
	}
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
	err := cfg.validateLimits()
	if err != nil {
		return err
	}

	err = cfg.validateTopology()
	if err != nil {
		return err
	}

	if cfg.ClaimLease < cfg.AttemptBudget() {
		return fmt.Errorf("%w: claim_lease %s below FR-46 budget %s", ErrConfig, cfg.ClaimLease, cfg.AttemptBudget())
	}

	return nil
}

func (cfg *Config) validateLimits() error {
	if len(cfg.APIToken) < MinAPITokenBytes {
		return ErrAPIToken
	}

	if cfg.JSONMaxDepth < 1 {
		return fmt.Errorf("%w: json_max_depth must be >= 1", ErrConfig)
	}

	if cfg.MaxRequestBytes < 1 || cfg.MaxPayloadBytes < 1 || cfg.MaxResponseBytes < 1 {
		return fmt.Errorf("%w: byte caps must be >= 1", ErrConfig)
	}

	if cfg.RateLimitRPM < 1 || cfg.RateLimitBurst < 1 {
		return fmt.Errorf("%w: rate limit must be >= 1", ErrConfig)
	}

	if cfg.Prefetch < 1 {
		return fmt.Errorf("%w: prefetch must be >= 1", ErrConfig)
	}

	if cfg.ReplayMax != DefaultReplayMax {
		return fmt.Errorf(
			"%w: replay_max must be %d until persist history cap is configurable",
			ErrConfig,
			DefaultReplayMax,
		)
	}

	if cfg.RetentionEnabled || cfg.RetentionDays != 0 {
		return fmt.Errorf(
			"%w: retention is not implemented (keep retention_enabled false and retention_days 0)",
			ErrConfig,
		)
	}

	return nil
}

func (cfg *Config) validateTopology() error {
	if (cfg.MongoURI == "") != (cfg.AMQPURI == "") {
		return fmt.Errorf("%w: mongo_uri and amqp_uri must be configured together", ErrConfig)
	}

	if cfg.MongoDatabase == "" || cfg.MongoJobsCollection == "" {
		return fmt.Errorf("%w: mongo database and collection must be set", ErrConfig)
	}

	if cfg.QueueJobs != domain.QueueJobs || cfg.QueueDLQ != domain.QueueDLQ {
		return fmt.Errorf(
			"%w: queue_jobs and queue_dlq must match topology %s/%s",
			ErrConfig,
			domain.QueueJobs,
			domain.QueueDLQ,
		)
	}

	return nil
}
