package httpapi_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/platform"
)

const (
	canaryPayload = "CANARY_HOP15_PAYLOAD_9f3a"
	canaryAuth    = "CANARY_HOP15_MARKER_7c2exxxxxxxxxxxxxxxx"
)

func TestV1ReadsRequireAuthATSEC01(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	paths := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/jobs"},
		{http.MethodGet, "/v1/jobs/aaaaaaaaaaaaaaaaaaaaaaaa"},
		{http.MethodPost, "/v1/jobs/aaaaaaaaaaaaaaaaaaaaaaaa/replay"},
	}

	for _, tc := range paths {
		res := doReq(t, fx.h, tc.method, tc.path, "", "", "")
		got := decodeErr(t, res)
		if res.StatusCode != http.StatusUnauthorized || got.Code != "unauthorized" {
			t.Fatalf("%s %s status=%d body=%+v", tc.method, tc.path, res.StatusCode, got)
		}
	}
}

func TestHealthzOmitsSecretsATSEC24(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	res := doReq(t, fx.h, http.MethodGet, "/healthz", "", "", "")
	defer closeBody(t, res)

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}

	var body map[string]any

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatal(err)
	}

	if len(body) != 2 {
		t.Fatalf("health keys = %v, want status+checks", keysOf(body))
	}

	if body["status"] != "ok" {
		t.Fatalf("status = %v", body["status"])
	}

	checks, ok := body["checks"].(map[string]any)
	if !ok || checks["mongo"] != "up" || checks["amqp"] != "up" {
		t.Fatalf("checks = %v", body["checks"])
	}

	blob := string(raw)
	for _, leak := range []string{
		platform.ValidToken(),
		"mongodb://",
		"amqp://",
		"api_token",
	} {
		if strings.Contains(blob, leak) {
			t.Fatalf("health leaked %q", leak)
		}
	}
}

func TestRecovererLogsOmitAuthorizationATSEC09(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zapcore.ErrorLevel)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+canaryAuth)
	rec := httptest.NewRecorder()
	httpapi.RecovererWithLog(zap.New(core)).ServeHTTP(rec, req)

	res := rec.Result()
	defer closeBody(t, res)

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d", res.StatusCode)
	}

	if logs.Len() < 1 {
		t.Fatal("expected panic log")
	}

	assertLogsOmitSecrets(t, logs.All(), canaryAuth, "Bearer")
}

func TestReplayLogsOmitPayloadATSEC09(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zapcore.InfoLevel)
	fx := newFixtureLog(t, zap.New(core))
	body := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"secret":"` + canaryPayload + `"}}`
	created := doReq(t, fx.h, http.MethodPost, "/v1/jobs", platform.ValidToken(), "sec-09", body)
	defer closeBody(t, created)

	var id idBody

	err := json.NewDecoder(created.Body).Decode(&id)
	if err != nil {
		t.Fatal(err)
	}

	out, err := fx.st.Claim(t.Context(), deliver.ClaimIn{ID: id.ID, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}

	err = fx.st.CommitOutcome(t.Context(), deliver.OutcomeIn{
		ID:           id.ID,
		FenceToken:   out.FenceToken,
		Status:       domain.StatusDead,
		AttemptsDone: 1,
		Cycle:        out.Cycle,
	})
	if err != nil {
		t.Fatal(err)
	}

	replayed := doReq(t, fx.h, http.MethodPost, "/v1/jobs/"+id.ID+"/replay", platform.ValidToken(), "", "")
	defer closeBody(t, replayed)

	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status = %d", replayed.StatusCode)
	}

	if logs.FilterMessage("job replayed").Len() < 1 {
		t.Fatal("expected job replayed log")
	}

	assertLogsOmitSecrets(t, logs.All(), canaryPayload, platform.ValidToken(), testTarget)
}

func TestHopperRuntimeUIDATSEC07(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	compose := loadYAML(t, filepath.Join(root, "deploy", "compose.yaml"))
	common := asMap(t, compose["x-hopper-common"])
	user, ok := common["user"].(string)
	if !ok {
		t.Fatalf("x-hopper-common.user = %T %v", common["user"], common["user"])
	}

	assertHopperUID(t, "compose", user)

	dockerfile, err := os.ReadFile(filepath.Join(root, "deploy", "go.Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(dockerfile), "USER 65532:65532") {
		t.Fatal("go.Dockerfile missing USER 65532:65532")
	}
}

func assertHopperUID(t *testing.T, src, spec string) {
	t.Helper()

	uidStr, _, ok := strings.Cut(spec, ":")
	if !ok {
		uidStr = spec
	}

	uid, err := strconv.Atoi(uidStr)
	if err != nil {
		t.Fatalf("%s user %q: %v", src, spec, err)
	}

	if uid <= 10000 {
		t.Fatalf("%s uid %d, want >10000 (SEC-19)", src, uid)
	}
}

func assertLogsOmitSecrets(t *testing.T, entries []observer.LoggedEntry, forbidden ...string) {
	t.Helper()

	blob := logBlob(entries)
	for _, item := range forbidden {
		if item != "" && strings.Contains(blob, item) {
			t.Fatalf("log leaked %q in %s", item, blob)
		}
	}
}

func logBlob(entries []observer.LoggedEntry) string {
	var b strings.Builder

	for i := range entries {
		entry := &entries[i]
		b.WriteString(entry.Message)
		b.WriteByte(' ')
		for key, val := range entry.ContextMap() {
			b.WriteString(key)
			b.WriteByte('=')

			_, ferr := fmt.Fprintf(&b, "%v", val)
			if ferr != nil {
				b.WriteString("<unprintable>")
			}

			b.WriteByte(' ')
		}
	}

	return b.String()
}
