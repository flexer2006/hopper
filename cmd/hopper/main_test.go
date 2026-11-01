package main

import (
	"errors"
	"testing"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/platform"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRunModes(t *testing.T) {
	if err := run([]string{"--help"}); err != nil {
		t.Fatalf("run(--help) = %v, want nil", err)
	}

	if err := run(nil); !errors.Is(err, platform.ErrInvalidMode) {
		t.Fatalf("run(nil) = %v, want ErrInvalidMode", err)
	}

	if err := run([]string{"unknown"}); !errors.Is(err, platform.ErrInvalidMode) {
		t.Fatalf("run(unknown) = %v, want ErrInvalidMode", err)
	}

	t.Setenv(platform.ConfigFileEnv, "")

	if err := run([]string{"api"}); !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("run(api) without config = %v, want ErrConfig", err)
	}

	if err := run([]string{"worker"}); !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("run(worker) without config = %v, want ErrConfig", err)
	}
}
