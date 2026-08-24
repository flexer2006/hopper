package platform

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const yamlOwnerRW = 0o600

func ValidToken() string {
	return strings.Repeat("a", MinAPITokenBytes)
}

func MinimalYAML(token string) string {
	return "api_token: \"" + token + "\"\nlog_level: info\nlog_stack_traces: false\n"
}

func WriteTempConfig(dir, token string) (string, error) {
	path := filepath.Join(dir, "hopper.yaml")

	err := os.WriteFile(path, []byte(MinimalYAML(token)), yamlOwnerRW)
	if err != nil {
		return "", fmt.Errorf("%w: write %s: %w", ErrConfig, path, err)
	}

	return path, nil
}
