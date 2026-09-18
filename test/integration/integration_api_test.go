//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

var publicDenylist = []string{
	"fence_token",
	"fence",
	"claim_expires_at",
	"dispatch",
	"producer_idempotency_key",
	"producer_key",
	"request_hash",
	"delivery_starts",
	"not_before",
}

func TestLiveATUC1101Healthz(t *testing.T) {
	cfg := requireCompose(t)
	_ = cfg

	resp, raw, err := apiRequest(t.Context(), http.MethodGet, healthzPath, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		safeLogf(t, "healthz status=%d body=%s", resp.StatusCode, string(raw))
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}

	var body map[string]any

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatal(err)
	}

	if body["status"] != "ok" {
		t.Fatalf("status = %v", body["status"])
	}

	checks, ok := body["checks"].(map[string]any)
	if !ok || checks["mongo"] != "up" || checks["amqp"] != "up" {
		t.Fatalf("checks = %v", body["checks"])
	}
}

func TestLiveATUC0101PostJob(t *testing.T) {
	cfg := requireCompose(t)

	idem := fmt.Sprintf("hop13-uc01-%d", time.Now().UnixNano())
	id := postAcceptedJob(t, cfg, idem)
	assertPublicJobShape(t, cfg, id)
}

func TestLiveATGET01PublicShape(t *testing.T) {
	cfg := requireCompose(t)

	idem := fmt.Sprintf("hop13-get-%d", time.Now().UnixNano())
	id := postAcceptedJob(t, cfg, idem)
	assertPublicJobShape(t, cfg, id)
}

func TestLiveATIDEM04SameKeySameID(t *testing.T) {
	cfg := requireCompose(t)

	idem := fmt.Sprintf("hop13-idem4-%d", time.Now().UnixNano())
	status, raw1 := postJobs(t, cfg.APIToken, idem, nil)
	if status != http.StatusAccepted && status != http.StatusServiceUnavailable {
		safeLogf(t, "first POST status=%d body=%s", status, string(raw1))
		t.Fatalf("first status = %d", status)
	}

	firstID := decodeJobID(t, raw1)

	status, raw2 := postJobs(t, cfg.APIToken, idem, nil)
	if status != http.StatusAccepted && status != http.StatusServiceUnavailable {
		safeLogf(t, "second POST status=%d body=%s", status, string(raw2))
		t.Fatalf("second status = %d", status)
	}

	secondID := decodeJobID(t, raw2)
	if secondID != firstID {
		t.Fatalf("ids differ: first=%q second=%q", firstID, secondID)
	}
}

func TestLiveATIDEM05SameKeyDifferentPayload(t *testing.T) {
	cfg := requireCompose(t)

	idem := fmt.Sprintf("hop13-idem5-%d", time.Now().UnixNano())
	firstBody := []byte(`{"type":"http_post","target":"https://example.com/","payload":{"n":1},"max_attempts":1}`)
	secondBody := []byte(`{"type":"http_post","target":"https://example.com/","payload":{"n":2},"max_attempts":1}`)

	status, _ := postJobs(t, cfg.APIToken, idem, firstBody)
	if status != http.StatusAccepted {
		t.Fatalf("first status = %d", status)
	}

	status, raw := postJobs(t, cfg.APIToken, idem, secondBody)
	if status != http.StatusConflict {
		safeLogf(t, "conflict POST status=%d body=%s", status, string(raw))
		t.Fatalf("status = %d, want 409", status)
	}
}

func TestLiveATUC0502ReplayNonDead409(t *testing.T) {
	cfg := requireCompose(t)

	idem := fmt.Sprintf("hop13-replay-%d", time.Now().UnixNano())
	id := postAcceptedJob(t, cfg, idem)

	replay, replayRaw, err := apiRequest(
		t.Context(),
		http.MethodPost,
		"/v1/jobs/"+id+"/replay",
		cfg.APIToken,
		"",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if replay.StatusCode != http.StatusConflict {
		safeLogf(t, "replay status=%d body=%s", replay.StatusCode, string(replayRaw))
		t.Fatalf("replay status = %d, want 409 for non-dead job", replay.StatusCode)
	}
}

func TestLiveATLIST01DeadList(t *testing.T) {
	cfg := requireCompose(t)

	bad, badRaw, err := apiRequest(t.Context(), http.MethodGet, "/v1/jobs?status=queued", cfg.APIToken, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if bad.StatusCode != http.StatusBadRequest {
		safeLogf(t, "list queued status=%d body=%s", bad.StatusCode, string(badRaw))
		t.Fatalf("status = %d, want 400", bad.StatusCode)
	}

	resp, raw, err := apiRequest(t.Context(), http.MethodGet, "/v1/jobs?status=dead", cfg.APIToken, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		safeLogf(t, "list dead status=%d body=%s", resp.StatusCode, string(raw))
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body struct {
		Items []map[string]any `json:"items"`
	}

	err = json.Unmarshal(raw, &body)
	if err != nil {
		t.Fatal(err)
	}

	if len(body.Items) > 50 {
		t.Fatalf("list len = %d, want ≤50", len(body.Items))
	}

	var prev time.Time

	for i, item := range body.Items {
		if item["status"] != "dead" {
			t.Fatalf("items[%d].status = %v", i, item["status"])
		}

		for _, key := range publicDenylist {
			if _, exists := item[key]; exists {
				t.Fatalf("list item leaked %q", key)
			}
		}

		if _, exists := item["payload"]; exists {
			t.Fatal("list item leaked payload")
		}

		stamp, ok := item["updated_at"].(string)
		if !ok {
			t.Fatalf("items[%d] missing updated_at", i)
		}

		parsed, parseErr := time.Parse(time.RFC3339Nano, stamp)
		if parseErr != nil {
			parsed, parseErr = time.Parse(time.RFC3339, stamp)
		}

		if parseErr != nil {
			t.Fatalf("updated_at = %q: %v", stamp, parseErr)
		}

		if i > 0 && parsed.After(prev) {
			t.Fatalf("list not desc: %s after %s", parsed, prev)
		}

		prev = parsed
	}
}

func assertPublicJobShape(t *testing.T, cfg labConfig, id string) {
	t.Helper()

	resp, raw, err := apiRequest(t.Context(), http.MethodGet, "/v1/jobs/"+id, cfg.APIToken, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		safeLogf(t, "GET status=%d body=%s", resp.StatusCode, string(raw))
		t.Fatalf("GET status = %d", resp.StatusCode)
	}

	var job map[string]any

	err = json.Unmarshal(raw, &job)
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range publicDenylist {
		if _, exists := job[key]; exists {
			t.Fatalf("public GET must omit %q", key)
		}
	}

	if job["id"] != id {
		t.Fatalf("id = %v, want %s", job["id"], id)
	}

	target, ok := job["target"].(string)
	if !ok || !strings.HasPrefix(target, "https://") {
		t.Fatalf("target = %v", job["target"])
	}
}

func TestLiveATRELAY01PendingPublished(t *testing.T) {
	cfg := requireCompose(t)

	store := openLeaseStore(t, cfg, cfg.Collection, 5*time.Second)
	defer store.Close(t.Context())

	idem := fmt.Sprintf("hop13-relay01-%d", time.Now().UnixNano())
	id := postAcceptedJob(t, cfg, idem)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err = waitNotPending(ctx, store, id)
	if err != nil {
		t.Fatal(err)
	}
}
