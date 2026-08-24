package platform_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/platform"
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
