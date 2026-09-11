package broker_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/flexer2006/hopper/internal/broker"
)

func moduleRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}

	dir := filepath.Dir(file)
	for range 12 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}

		dir = parent
	}

	t.Fatal("go.mod not found")

	return ""
}

func TestEnqueueGoldenMatchesSchemaATCONTRACT02(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join(moduleRoot(t), "..", "docs", "contracts", "enqueue-message.schema.json"))
	if err != nil {
		t.Fatal(err)
	}

	var schema map[string]any

	err = json.Unmarshal(raw, &schema)
	if err != nil {
		t.Fatal(err)
	}

	if schema["additionalProperties"] != false {
		t.Fatal("enqueue-message.schema.json must close additionalProperties")
	}

	required, ok := schema["required"].([]any)
	if !ok || len(required) != 1 || required[0] != "job_id" {
		t.Fatalf("required = %v, want [job_id]", schema["required"])
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties missing")
	}

	jobIDSchema, ok := props["job_id"].(map[string]any)
	if !ok {
		t.Fatal("job_id schema missing")
	}

	pattern, ok := jobIDSchema["pattern"].(string)
	if !ok || pattern == "" {
		t.Fatal("job_id pattern missing")
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}

	body, err := broker.MarshalEnqueue(testJobID)
	if err != nil {
		t.Fatal(err)
	}

	var exemplar map[string]any

	err = json.Unmarshal(body, &exemplar)
	if err != nil {
		t.Fatal(err)
	}

	if len(exemplar) != len(required) {
		t.Fatalf("exemplar extra fields: %v", exemplar)
	}

	for _, key := range required {
		name, ok := key.(string)
		if !ok {
			t.Fatalf("required item %T", key)
		}

		got, exists := exemplar[name]
		if !exists {
			t.Fatalf("exemplar missing %s", name)
		}

		id, ok := got.(string)
		if !ok || !re.MatchString(id) {
			t.Fatalf("job_id %v does not match %s", got, pattern)
		}
	}
}
