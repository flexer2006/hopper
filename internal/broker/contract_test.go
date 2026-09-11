package broker_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
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

func TestDLQGoldenMatchesSchemaATCONTRACT04(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join(moduleRoot(t), "..", "docs", "contracts", "dlq-message.schema.json"))
	if err != nil {
		t.Fatal(err)
	}

	var schema map[string]any

	err = json.Unmarshal(raw, &schema)
	if err != nil {
		t.Fatal(err)
	}

	oneOf, ok := schema["oneOf"].([]any)
	if !ok || len(oneOf) != 3 {
		t.Fatalf("oneOf = %T len=%d, want 3 variants", schema["oneOf"], len(oneOf))
	}

	byTitle := make(map[string]map[string]any, len(oneOf))
	for _, item := range oneOf {
		variant, vok := item.(map[string]any)
		if !vok {
			t.Fatalf("oneOf item %T", item)
		}

		title, tok := variant["title"].(string)
		if !tok {
			t.Fatalf("variant missing title: %v", variant)
		}

		byTitle[title] = variant
	}

	for _, title := range []string{"KnownJobTerminal", "GhostMissingDocument", "MalformedWithoutJob"} {
		if byTitle[title] == nil {
			t.Fatalf("missing oneOf variant %q", title)
		}
	}

	knownBody, err := broker.MarshalKnownDLQ(testJobID, 0, "attempts_exhausted")
	if err != nil {
		t.Fatal(err)
	}

	assertDLQExemplar(t, byTitle["KnownJobTerminal"], knownBody)

	ghostBody, err := broker.MarshalGhostDLQ(testJobID)
	if err != nil {
		t.Fatal(err)
	}

	assertDLQExemplar(t, byTitle["GhostMissingDocument"], ghostBody)

	secret := []byte(`{"password":"hunter2","job_id":"should-not-leak"}`)
	malformedBody, err := broker.MarshalMalformedDLQ(secret)
	if err != nil {
		t.Fatal(err)
	}

	assertDLQExemplar(t, byTitle["MalformedWithoutJob"], malformedBody)

	var malformed map[string]any

	err = json.Unmarshal(malformedBody, &malformed)
	if err != nil {
		t.Fatal(err)
	}

	sum := sha256.Sum256(secret)
	wantHash := hex.EncodeToString(sum[:])

	if malformed["body_sha256"] != wantHash {
		t.Fatalf("body_sha256 = %v, want %s", malformed["body_sha256"], wantHash)
	}

	if _, hasJob := malformed["job_id"]; hasJob {
		t.Fatal("malformed exemplar must not include job_id")
	}
}

func assertDLQExemplar(t *testing.T, variant map[string]any, body []byte) {
	t.Helper()

	if variant["additionalProperties"] != false {
		t.Fatal("variant must close additionalProperties")
	}

	required := strAnySlice(t, variant["required"])
	props, ok := variant["properties"].(map[string]any)
	if !ok {
		t.Fatal("variant properties missing")
	}

	var exemplar map[string]any

	err := json.Unmarshal(body, &exemplar)
	if err != nil {
		t.Fatalf("exemplar json: %v", err)
	}

	if len(exemplar) != len(required) {
		t.Fatalf("exemplar keys %v, required %v", keysOfMap(exemplar), required)
	}

	for _, key := range required {
		if _, exists := exemplar[key]; !exists {
			t.Fatalf("exemplar missing required %q", key)
		}
	}

	for key, val := range exemplar {
		schema, exists := props[key]
		if !exists {
			t.Fatalf("exemplar has unexpected key %q", key)
		}

		assertDLQValue(t, key, val, asStringMap(t, schema))
	}
}

func assertDLQValue(t *testing.T, key string, val any, schema map[string]any) {
	t.Helper()

	if c, ok := schema["const"]; ok && val != c {
		t.Fatalf("%s = %v, want const %v", key, val, c)
	}

	if enum, ok := schema["enum"].([]any); ok {
		if !slices.Contains(enum, val) {
			t.Fatalf("%s = %v, want one of %v", key, val, enum)
		}
	}

	if pattern, ok := schema["pattern"].(string); ok {
		re, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatal(err)
		}

		s, sok := val.(string)
		if !sok || !re.MatchString(s) {
			t.Fatalf("%s = %v does not match %s", key, val, pattern)
		}
	}

	if floor, ok := schema["minimum"].(float64); ok {
		n, nok := val.(float64)
		if !nok || n < floor {
			t.Fatalf("%s = %v, want >= %v", key, val, floor)
		}
	}
}

func strAnySlice(t *testing.T, v any) []string {
	t.Helper()

	items, ok := v.([]any)
	if !ok {
		t.Fatalf("required = %T", v)
	}

	out := make([]string, 0, len(items))
	for _, item := range items {
		s, sok := item.(string)
		if !sok {
			t.Fatalf("required item %T", item)
		}

		out = append(out, s)
	}

	return out
}

func asStringMap(t *testing.T, v any) map[string]any {
	t.Helper()

	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("schema property %T", v)
	}

	return m
}

func keysOfMap(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
