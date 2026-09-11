package httpapi_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v2"

	"github.com/flexer2006/hopper/internal/platform"
)

var publicJobSecrets = []string{
	"fence_token",
	"fence",
	"claim_expires_at",
	"dispatch",
	"producer_idempotency_key",
	"producer_key",
	"request_hash",
	"delivery_starts",
	"not_before",
	"claimed_by",
	"dispatch_history",
	"replay_count",
}

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

func productDocs(t *testing.T, parts ...string) string {
	t.Helper()

	elems := append([]string{moduleRoot(t), "..", "docs"}, parts...)

	return filepath.Join(elems...)
}

func loadYAML(t *testing.T, path string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var doc any

	err = yaml.Unmarshal(raw, &doc)
	if err != nil {
		t.Fatalf("yaml %s: %v", path, err)
	}

	return asMap(t, doc)
}

func loadJSONMap(t *testing.T, path string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var doc map[string]any

	err = json.Unmarshal(raw, &doc)
	if err != nil {
		t.Fatalf("json %s: %v", path, err)
	}

	return doc
}

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()

	switch m := v.(type) {
	case map[string]any:
		return m
	case map[any]any:
		out := make(map[string]any, len(m))
		for key, val := range m {
			ks, ok := key.(string)
			if !ok {
				t.Fatalf("non-string map key %T %v", key, key)
			}

			out[ks] = val
		}

		return out
	default:
		t.Fatalf("want map, got %T", v)

		return nil
	}
}

func child(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()

	v, ok := m[key]
	if !ok {
		t.Fatalf("missing key %q (have %v)", key, keysOf(m))
	}

	return asMap(t, v)
}

func resolveLocalRef(t *testing.T, doc map[string]any, ref string) map[string]any {
	t.Helper()

	const prefix = "#/components/"
	if !strings.HasPrefix(ref, prefix) {
		t.Fatalf("unsupported $ref %s", ref)
	}

	parts := strings.Split(strings.TrimPrefix(ref, prefix), "/")
	cur := child(t, doc, "components")
	for _, part := range parts {
		cur = child(t, cur, part)
	}

	return cur
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func strSlice(t *testing.T, v any) []string {
	t.Helper()

	switch items := v.(type) {
	case []any:
		out := make([]string, 0, len(items))
		for _, item := range items {
			s, ok := item.(string)
			if !ok {
				t.Fatalf("want string, got %T", item)
			}

			out = append(out, s)
		}

		return out
	case []string:
		return items
	default:
		t.Fatalf("want string slice, got %T", v)

		return nil
	}
}

func TestOpenAPIStructuralATCONTRACT01(t *testing.T) {
	t.Parallel()

	doc := loadYAML(t, productDocs(t, "api", "openapi.yaml"))

	if doc["openapi"] != "3.1.0" {
		t.Fatalf("openapi = %v, want 3.1.0", doc["openapi"])
	}

	paths := child(t, doc, "paths")
	wantOps := map[string][]string{
		"/healthz":             {http.MethodGet},
		"/v1/jobs":             {http.MethodGet, http.MethodPost},
		"/v1/jobs/{id}":        {http.MethodGet},
		"/v1/jobs/{id}/replay": {http.MethodPost},
	}

	for path, methods := range wantOps {
		item := child(t, paths, path)
		for _, method := range methods {
			if _, ok := item[strings.ToLower(method)]; !ok {
				t.Errorf("%s %s missing in OpenAPI", method, path)
			}
		}
	}

	sec := doc["security"]
	secList, ok := sec.([]any)
	if !ok || len(secList) != 1 {
		t.Fatalf("document security = %v, want one bearerAuth requirement", sec)
	}

	req := asMap(t, secList[0])
	if _, ok = req["bearerAuth"]; !ok {
		t.Fatalf("document security = %v, want bearerAuth", req)
	}

	health := child(t, child(t, paths, "/healthz"), "get")
	if _, exists := health["security"]; !exists {
		t.Fatal("/healthz must override security to empty")
	}

	if len(strSlice(t, health["security"])) != 0 {
		t.Fatalf("/healthz security = %v, want []", health["security"])
	}

	for _, op := range []map[string]any{
		child(t, child(t, paths, "/v1/jobs"), "post"),
		child(t, child(t, paths, "/v1/jobs"), "get"),
		child(t, child(t, paths, "/v1/jobs/{id}"), "get"),
		child(t, child(t, paths, "/v1/jobs/{id}/replay"), "post"),
	} {
		if raw, exists := op["security"]; exists && len(strSlice(t, raw)) == 0 {
			t.Fatal("protected /v1 operation must not set security: []")
		}
	}

	bearer := child(t, child(t, child(t, doc, "components"), "securitySchemes"), "bearerAuth")
	if bearer["type"] != "http" || bearer["scheme"] != "bearer" {
		t.Fatalf("bearerAuth = %v", bearer)
	}

	postJobs := child(t, child(t, paths, "/v1/jobs"), "post")
	params := postJobs["parameters"]
	paramList, ok := params.([]any)
	if !ok || len(paramList) == 0 {
		t.Fatal("POST /v1/jobs missing parameters")
	}

	foundKey := false
	for _, raw := range paramList {
		item := asMap(t, raw)
		if ref, hasRef := item["$ref"].(string); hasRef {
			resolved := resolveLocalRef(t, doc, ref)
			if resolved["name"] == "Idempotency-Key" && resolved["required"] == true {
				foundKey = true

				break
			}

			continue
		}

		if item["name"] == "Idempotency-Key" && item["required"] == true {
			foundKey = true

			break
		}
	}

	if !foundKey {
		t.Fatal("POST /v1/jobs must require Idempotency-Key")
	}

	errSchema := child(t, child(t, child(t, doc, "components"), "schemas"), "ErrorResponse")
	if errSchema["additionalProperties"] != false {
		t.Fatal("ErrorResponse must set additionalProperties false")
	}

	required := strSlice(t, errSchema["required"])
	if strings.Join(required, ",") != "error,code" {
		t.Fatalf("ErrorResponse.required = %v", required)
	}

	props := child(t, errSchema, "properties")
	for _, rfc := range []string{"type", "title", "status"} {
		if _, exists := props[rfc]; exists {
			t.Errorf("ErrorResponse must not declare RFC 7807 field %s", rfc)
		}
	}

	schemaFile := loadJSONMap(t, productDocs(t, "contracts", "error-response.schema.json"))
	openCodes := strSlice(t, child(t, props, "code")["enum"])
	fileCodes := strSlice(t, child(t, child(t, schemaFile, "properties"), "code")["enum"])
	slices.Sort(openCodes)
	slices.Sort(fileCodes)
	if !slices.Equal(openCodes, fileCodes) {
		t.Fatalf("ErrorResponse enum drift OpenAPI=%v schema=%v", openCodes, fileCodes)
	}

	fx := newFixture(t)
	routes, ok := fx.h.(chi.Routes)
	if !ok {
		t.Fatalf("handler %T does not implement chi.Routes", fx.h)
	}

	seen := make(map[string]bool)
	err := chi.Walk(routes, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		seen[method+" "+strings.TrimSuffix(route, "/")] = true

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	wantWalk := []string{
		http.MethodGet + " /healthz",
		http.MethodPost + " /v1/jobs",
		http.MethodGet + " /v1/jobs",
		http.MethodGet + " /v1/jobs/{id}",
		http.MethodPost + " /v1/jobs/{id}/replay",
	}
	for _, key := range wantWalk {
		if !seen[key] {
			t.Errorf("chi mux missing %s (have %v)", key, seen)
		}
	}
}

func TestPublicJobOmitsInternalsATCONTRACT03(t *testing.T) {
	t.Parallel()

	doc := loadYAML(t, productDocs(t, "api", "openapi.yaml"))
	job := child(t, child(t, child(t, doc, "components"), "schemas"), "Job")
	props := child(t, job, "properties")

	for _, name := range publicJobSecrets {
		if _, exists := props[name]; exists {
			t.Errorf("OpenAPI Job must not expose %s", name)
		}
	}

	mongo := loadJSONMap(t, productDocs(t, "contracts", "job-document.schema.json"))
	mongoProps := child(t, mongo, "properties")
	for _, name := range []string{
		"dispatch", "producer_idempotency_key", "request_hash", "delivery_starts",
		"not_before", "dispatch_history", "claimed_by", "replay_count",
	} {
		if _, exists := mongoProps[name]; !exists {
			t.Errorf("job-document.schema.json lost internal field %s (denylist would be vacuous)", name)
		}
	}

	fx := newFixture(t)
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), idemKey, createJSON)
	var id idBody

	err := json.NewDecoder(created.Body).Decode(&id)
	closeBody(t, created)
	if err != nil {
		t.Fatal(err)
	}

	got := doReq(t, fx.h, http.MethodGet, "/v1/jobs/"+id.ID, platform.ValidToken(), "", "")
	defer closeBody(t, got)

	if got.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d", got.StatusCode)
	}

	raw, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}

	var body map[string]any

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatalf("GET json: %v", err)
	}

	for _, name := range publicJobSecrets {
		if _, exists := body[name]; exists {
			t.Errorf("GET job leaked %s: %s", name, raw)
		}
	}

	for _, name := range strSlice(t, job["required"]) {
		if _, exists := body[name]; !exists {
			t.Errorf("GET job missing OpenAPI required %s: %s", name, raw)
		}
	}
}

func TestREADMECommandsATDEPLOY01(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join(moduleRoot(t), "README.md"))
	if err != nil {
		t.Fatal(err)
	}

	text := string(raw)
	for _, cmd := range []string{"hopper api", "hopper worker"} {
		if !strings.Contains(text, cmd) {
			t.Errorf("README missing command string %q", cmd)
		}
	}

	spec, err := os.ReadFile(productDocs(t, "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(spec), "hopper api") || strings.Contains(string(spec), "hopper worker") {
		t.Fatal("OpenAPI must not document process commands; AT-DEPLOY-01 stays on README")
	}
}
