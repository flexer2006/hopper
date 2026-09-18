package platform_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestParseMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		want    string
		wantErr error
	}{
		{name: "api", args: []string{"api"}, want: platform.ModeAPI},
		{name: "worker", args: []string{"worker"}, want: platform.ModeWorker},
		{name: "help short", args: []string{"-h"}, wantErr: platform.ErrHelp},
		{name: "help long", args: []string{"--help"}, wantErr: platform.ErrHelp},
		{name: "help word", args: []string{"help"}, wantErr: platform.ErrHelp},
		{name: "missing", args: nil, wantErr: platform.ErrInvalidMode},
		{name: "unknown", args: []string{"serve"}, wantErr: platform.ErrInvalidMode},
		{name: "extra", args: []string{"api", "--boom"}, wantErr: platform.ErrInvalidMode},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := platform.ParseMode(tc.args)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ParseMode() err = %v, want %v", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseMode() unexpected err = %v", err)
			}

			if got != tc.want {
				t.Fatalf("ParseMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLoadFileTokenLength(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	shortPath := filepath.Join(dir, "short.yaml")

	writeErr := os.WriteFile(shortPath, []byte("api_token: short\n"), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	_, err := platform.LoadFile(shortPath)
	if !errors.Is(err, platform.ErrAPIToken) {
		t.Fatalf("LoadFile(short) err = %v, want ErrAPIToken", err)
	}

	okPath := filepath.Join(dir, "ok.yaml")
	token := platform.ValidToken()

	writeErr = os.WriteFile(okPath, []byte(platform.MinimalYAML(token)), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	cfg, err := platform.LoadFile(okPath)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.APIToken != token {
		t.Fatalf("APIToken = %q", cfg.APIToken)
	}
}

func TestLoadEnvOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")

	writeErr := os.WriteFile(path, []byte("api_token: short\n"), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.APITokenEnv, platform.ValidToken())

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.APIToken != platform.ValidToken() {
		t.Fatalf("APIToken = %q", cfg.APIToken)
	}

	if cfg.APIShutdownTimeout != platform.DefaultAPIShutdownTimeout {
		t.Fatalf("APIShutdownTimeout = %s", cfg.APIShutdownTimeout)
	}

	if cfg.WorkerShutdownTimeout != platform.DefaultWorkerShutdownTimeout {
		t.Fatalf("WorkerShutdownTimeout = %s", cfg.WorkerShutdownTimeout)
	}

	if cfg.RelayInterval != platform.DefaultRelayInterval {
		t.Fatalf("RelayInterval = %s", cfg.RelayInterval)
	}

	if cfg.HealingInterval != platform.DefaultHealingInterval {
		t.Fatalf("HealingInterval = %s", cfg.HealingInterval)
	}

	if cfg.LeaseScanInterval != platform.DefaultLeaseScanInterval {
		t.Fatalf("LeaseScanInterval = %s", cfg.LeaseScanInterval)
	}

	if cfg.HTTPAddr != platform.DefaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q", cfg.HTTPAddr)
	}

	if cfg.MaxRequestBytes != platform.DefaultMaxRequestBytes || cfg.JSONMaxDepth != platform.DefaultJSONMaxDepth {
		t.Fatalf("http limits request=%d depth=%d", cfg.MaxRequestBytes, cfg.JSONMaxDepth)
	}

	if cfg.RateLimitRPM != platform.DefaultRateLimitRPM || cfg.TrustXFFHops != 0 {
		t.Fatalf("rate rpm=%d xff=%d", cfg.RateLimitRPM, cfg.TrustXFFHops)
	}
}

func TestLoadMissingPath(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, "")

	_, err := platform.Load()
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("Load() err = %v, want ErrConfig", err)
	}
}

func TestNewLogger(t *testing.T) {
	t.Parallel()

	log, err := platform.NewLogger(new(platform.Config{LogLevel: "info"}))
	if err != nil {
		t.Fatal(err)
	}

	syncErr := log.Sync()
	if syncErr != nil {
		t.Logf("logger.Sync: %v", syncErr)
	}

	_, err = platform.NewLogger(new(platform.Config{LogLevel: "not-a-level"}))
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoadFileTimeouts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"api_shutdown_timeout: 7s\nworker_shutdown_timeout: 11s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	cfg, err := platform.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.APIShutdownTimeout != 7*time.Second {
		t.Fatalf("APIShutdownTimeout = %s, want 7s", cfg.APIShutdownTimeout)
	}

	if cfg.WorkerShutdownTimeout != 11*time.Second {
		t.Fatalf("WorkerShutdownTimeout = %s, want 11s", cfg.WorkerShutdownTimeout)
	}

	if platform.APIStopTimeout(&cfg) != 7*time.Second {
		t.Fatalf("APIStopTimeout() = %s, want 7s", platform.APIStopTimeout(&cfg))
	}

	if platform.WorkerStopTimeout(&cfg) != 11*time.Second {
		t.Fatalf("WorkerStopTimeout() = %s, want 11s", platform.WorkerStopTimeout(&cfg))
	}
}

func TestLoadTimeoutEnvOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"api_shutdown_timeout: 7s\nworker_shutdown_timeout: 11s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.APIShutdownTimeoutEnv, "3s")
	t.Setenv(platform.WorkerShutdownTimeoutEnv, "4s")

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.APIShutdownTimeout != 3*time.Second {
		t.Fatalf("APIShutdownTimeout = %s, want 3s", cfg.APIShutdownTimeout)
	}

	if cfg.WorkerShutdownTimeout != 4*time.Second {
		t.Fatalf("WorkerShutdownTimeout = %s, want 4s", cfg.WorkerShutdownTimeout)
	}
}

func TestLoadRelayIntervalEnvOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"relay_interval: 3s\nhealing_interval: 45s\nlease_scan_interval: 8s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.RelayIntervalEnv, "1s")
	t.Setenv(platform.HealingIntervalEnv, "9s")
	t.Setenv(platform.LeaseScanIntervalEnv, "2s")

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.RelayInterval != time.Second {
		t.Fatalf("RelayInterval = %s, want 1s", cfg.RelayInterval)
	}

	if cfg.HealingInterval != 9*time.Second {
		t.Fatalf("HealingInterval = %s, want 9s", cfg.HealingInterval)
	}

	if cfg.LeaseScanInterval != 2*time.Second {
		t.Fatalf("LeaseScanInterval = %s, want 2s", cfg.LeaseScanInterval)
	}
}

func TestLoadFileLeaseScanIntervalYAML(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "lease_scan_interval: 8s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	cfg, err := platform.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.LeaseScanInterval != 8*time.Second {
		t.Fatalf("LeaseScanInterval = %s, want 8s", cfg.LeaseScanInterval)
	}
}

func TestLoadFileRejectsNonPositiveLeaseScan(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "lease_scan_interval: 0s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile() err = %v, want ErrConfig", err)
	}
}

func TestLoadFileRejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "api_shutdown_timeout: 0s\n"

	writeErr := os.WriteFile(path, []byte(body), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile() err = %v, want ErrConfig", err)
	}
}

func TestLoadHTTPAddrEnvOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")

	writeErr := os.WriteFile(path, []byte(platform.MinimalYAML(platform.ValidToken())), 0o600)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.HTTPAddrEnv, "127.0.0.1:0")
	t.Setenv(platform.RateLimitRPMEnv, "50")
	t.Setenv(platform.TrustXFFHopsEnv, "2")

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.HTTPAddr != "127.0.0.1:0" {
		t.Fatalf("HTTPAddr = %q", cfg.HTTPAddr)
	}

	if cfg.RateLimitRPM != 50 || cfg.TrustXFFHops != 2 {
		t.Fatalf("rpm=%d hops=%d", cfg.RateLimitRPM, cfg.TrustXFFHops)
	}
}

func TestLoadInfrastructureConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"mongo_uri: mongodb://mongo:27017/?replicaSet=rs0\n" +
		"amqp_uri: amqp://guest:***@rabbitmq:5672/\n" +
		"mongo_database: custom\n" +
		"mongo_jobs_collection: queued_jobs\n" +
		"prefetch: 4\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := platform.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MongoURI == "" || cfg.AMQPURI == "" || cfg.MongoDatabase != "custom" ||
		cfg.MongoJobsCollection != "queued_jobs" ||
		cfg.Prefetch != 4 {
		t.Fatalf("infrastructure config = %+v", cfg)
	}
}

func TestLoadInfrastructureEnvOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	if err := os.WriteFile(path, []byte(platform.MinimalYAML(platform.ValidToken())), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.MongoURIEnv, "mongodb://mongo:27017/?replicaSet=rs0")
	t.Setenv(platform.AMQPURIEnv, "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv(platform.MongoDatabaseEnv, "envdb")
	t.Setenv(platform.MongoJobsCollectionEnv, "envjobs")
	t.Setenv(platform.PrefetchEnv, "7")

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MongoDatabase != "envdb" || cfg.MongoJobsCollection != "envjobs" || cfg.Prefetch != 7 {
		t.Fatalf("env infrastructure config = %+v", cfg)
	}
}

func TestLoadPrefetchEnvOverridesInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "prefetch: -1\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.PrefetchEnv, "4")

	cfg, err := platform.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Prefetch != 4 {
		t.Fatalf("Prefetch = %d, want 4", cfg.Prefetch)
	}
}

func TestConfigValidateInfrastructure(t *testing.T) {
	t.Parallel()

	valid := platform.Config{MongoURI: "mongo", AMQPURI: "amqp"}
	if err := valid.ValidateInfrastructure(); err != nil {
		t.Fatalf("ValidateInfrastructure() unexpected error: %v", err)
	}
	missing := platform.Config{}
	if err := missing.ValidateInfrastructure(); !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("ValidateInfrastructure() error = %v, want ErrConfig", err)
	}
}

func TestLoadRejectsPartialInfrastructureConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "mongo_uri: mongodb://mongo:27017/?replicaSet=rs0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile() err = %v, want ErrConfig", err)
	}
}

func TestLoadRejectsZeroPrefetch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"mongo_uri: mongodb://mongo:27017/?replicaSet=rs0\n" +
		"amqp_uri: amqp://guest:***@rabbitmq:5672/\n" +
		"prefetch: 0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile() err = %v, want ErrConfig", err)
	}
}

func TestLoadRejectsZeroJSONDepthEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	if err := os.WriteFile(path, []byte(platform.MinimalYAML(platform.ValidToken())), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(platform.ConfigFileEnv, path)
	t.Setenv(platform.JSONMaxDepthEnv, "0")

	_, err := platform.Load()
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("Load() err = %v, want ErrConfig", err)
	}
}

func TestLoadFileAcceptsHopperExampleYAML(t *testing.T) {
	t.Parallel()

	root := testutil.ModuleRoot(t)
	cfg, err := platform.LoadFile(filepath.Join(root, "deploy", "hopper.example.yaml"))
	if err != nil {
		t.Fatalf("LoadFile(hopper.example.yaml) = %v", err)
	}

	if cfg.MongoURI == "" || cfg.AMQPURI == "" || cfg.Prefetch != 1 || cfg.MongoDatabase != "hopper" {
		t.Fatalf("example yaml infrastructure = uri:%t amqp:%t db=%q prefetch=%d",
			cfg.MongoURI != "", cfg.AMQPURI != "", cfg.MongoDatabase, cfg.Prefetch)
	}

	if cfg.ClaimLease != platform.DefaultClaimLease || cfg.HTTPTimeout != platform.DefaultHTTPTimeout ||
		cfg.PublishConfirmTimeout != platform.DefaultPublishConfirmTimeout ||
		cfg.MaxResponseBytes != platform.DefaultMaxResponseBytes || cfg.ReplayMax != platform.DefaultReplayMax {
		t.Fatalf("example yaml knobs = lease=%s http=%s confirm=%s maxBody=%d replay=%d",
			cfg.ClaimLease, cfg.HTTPTimeout, cfg.PublishConfirmTimeout, cfg.MaxResponseBytes, cfg.ReplayMax)
	}
}

func TestLoadFileRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "not_a_hopper_key: 1\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile(unknown key) err = %v, want ErrConfig", err)
	}
}

func TestLoadFileRejectsRetentionEnabled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "retention_enabled: true\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile(retention_enabled) err = %v, want ErrConfig", err)
	}
}

func TestLoadFileRejectsClaimLeaseBelowFR46(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "claim_lease: 10s\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile(short claim_lease) err = %v, want ErrConfig", err)
	}
}

func TestLoadFileRejectsZeroReplayMax(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "replay_max: 0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile(replay_max: 0) err = %v, want ErrConfig", err)
	}
}

func TestLoadFileRejectsWrongQueueName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) + "queue_jobs: other\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := platform.LoadFile(path)
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("LoadFile(queue_jobs) err = %v, want ErrConfig", err)
	}
}

func TestLoadFileAppliesWorkerKnobs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hopper.yaml")
	body := platform.MinimalYAML(platform.ValidToken()) +
		"claim_lease: 40s\n" +
		"http_timeout: 12s\n" +
		"mongo_outcome_timeout: 6s\n" +
		"publish_confirm_timeout: 7s\n" +
		"max_response_bytes: 2048\n" +
		"worker_id: hopper-a\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := platform.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.ClaimLease != 40*time.Second || cfg.HTTPTimeout != 12*time.Second ||
		cfg.MongoOutcomeTimeout != 6*time.Second || cfg.PublishConfirmTimeout != 7*time.Second ||
		cfg.MaxResponseBytes != 2048 || cfg.WorkerID != "hopper-a" {
		t.Fatalf("worker knobs = %+v", cfg)
	}

	if cfg.AttemptBudget() != 12*time.Second+6*time.Second+7*time.Second+5*time.Second {
		t.Fatalf("AttemptBudget() = %s", cfg.AttemptBudget())
	}
}
