//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"testing"
)

func TestLiveATUC0701WorkerSIGKILLRequeue(t *testing.T) {
	requireChaos(t)
	_ = requireCompose(t)

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not in this netns; use HOP-14-operator-chaos.py on the host")
	}

	t.Skip("host operator script HOP-14-operator-chaos.py is the AT-UC07-01 evidence path")
}

func requireChaos(t *testing.T) {
	t.Helper()

	if os.Getenv("HOPPER_INT_CHAOS") != "1" {
		t.Skip("set HOPPER_INT_CHAOS=1; host evidence is HOP-14-operator-chaos.py")
	}
}
