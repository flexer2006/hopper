//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v2"

	"github.com/flexer2006/hopper/internal/testutil"
)

const (
	healthzPath  = "/healthz"
	deployConfig = "deploy/hopper.yaml"
)

var redactSecrets = regexp.MustCompile(`(?i)(Bearer\s+\S+|mongodb(?:\+srv)?://[^@\s]+@|amqp://[^@\s]+@|api_token:\s*\S+)`)

type labConfig struct {
	APIToken   string
	MongoURI   string
	MongoDB    string
	AMQPURI    string
	Collection string
}

func loadLabConfig(t *testing.T) labConfig {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(testutil.ModuleRoot(t), deployConfig))
	if err != nil {
		t.Fatal(err)
	}

	var doc map[string]any

	err = yaml.Unmarshal(raw, &doc)
	if err != nil {
		t.Fatal(err)
	}

	cfg := labConfig{
		APIToken:   stringField(doc, "api_token"),
		MongoURI:   loopbackMongo(stringField(doc, "mongo_uri")),
		MongoDB:    stringField(doc, "mongo_database"),
		AMQPURI:    loopbackAMQP(stringField(doc, "amqp_uri")),
		Collection: stringField(doc, "mongo_jobs_collection"),
	}

	if cfg.APIToken == "" || cfg.MongoURI == "" || cfg.MongoDB == "" || cfg.AMQPURI == "" {
		t.Fatal("deploy/hopper.yaml missing required fields")
	}

	return cfg
}

func stringField(doc map[string]any, key string) string {
	val, ok := doc[key]
	if !ok {
		return ""
	}

	s, ok := val.(string)
	if !ok {
		return ""
	}

	return s
}

func requireCompose(t *testing.T) labConfig {
	t.Helper()

	cfg := loadLabConfig(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if !pollHealthz(ctx) {
		t.Skip("compose not up: healthz not 200 with mongo+amqp")
	}

	return cfg
}

func pollHealthz(ctx context.Context) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if healthzUp(client) {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func healthzUp(client *http.Client) bool {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, apiBase()+healthzPath, nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)

		return false
	}

	var body map[string]any

	err = json.NewDecoder(resp.Body).Decode(&body)
	if err != nil {
		return false
	}

	if body["status"] != "ok" {
		return false
	}

	checks, ok := body["checks"].(map[string]any)
	if !ok {
		return false
	}

	return checks["mongo"] == "up" && checks["amqp"] == "up"
}

func apiRequest(
	ctx context.Context,
	method, path, token, idemKey string,
	body []byte,
) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, apiBase()+path, reader)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}

	client := &http.Client{Timeout: 15 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	raw, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return resp, nil, readErr
	}

	return resp, raw, nil
}

func safeLogf(t *testing.T, format string, args ...any) {
	t.Helper()

	msg := redactSecrets.ReplaceAllString(fmt.Sprintf(format, args...), "[redacted]")
	t.Log(msg)
}

const maxHeldGet = 256

func drainQueueOnce(ch *amqp.Channel, queue string, match func(amqp.Delivery) bool, held *[]uint64) (bool, error) {
	for {
		msg, ok, err := ch.Get(queue, false)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}

		if match(msg) {
			return true, ch.Ack(msg.DeliveryTag, false)
		}

		*held = append(*held, msg.DeliveryTag)
		if len(*held) >= maxHeldGet {
			return false, fmt.Errorf("held %d unmatched messages on %s", len(*held), queue)
		}
	}
}

func nackHeld(ch *amqp.Channel, held []uint64) {
	for _, tag := range held {
		_ = ch.Nack(tag, false, true)
	}
}

func drainGetMatch(ctx context.Context, ch *amqp.Channel, queue string, match func(amqp.Delivery) bool) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	held := make([]uint64, 0, 8)

	defer func() { nackHeld(ch, held) }()

	for {
		found, err := drainQueueOnce(ch, queue, match, &held)
		if err != nil {
			return err
		}

		if found {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

var defaultJobJSON = []byte(`{"type":"http_post","target":"https://example.com/","payload":{"n":1},"max_attempts":1}`)

func postJobs(t *testing.T, token, idem string, body []byte) (int, []byte) {
	t.Helper()

	if body == nil {
		body = defaultJobJSON
	}

	resp, raw, err := apiRequest(t.Context(), http.MethodPost, "/v1/jobs", token, idem, body)
	if err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, raw
}

func decodeJobID(t *testing.T, raw []byte) string {
	t.Helper()

	var created map[string]any

	err := json.Unmarshal(raw, &created)
	if err != nil {
		t.Fatal(err)
	}

	id, ok := created["id"].(string)
	if !ok || id == "" {
		t.Fatalf("created id = %v", created["id"])
	}

	return id
}

func postAcceptedJob(t *testing.T, cfg labConfig, idem string) string {
	t.Helper()

	status, raw := postJobs(t, cfg.APIToken, idem, nil)
	if status != http.StatusAccepted {
		safeLogf(t, "POST status=%d body=%s", status, string(raw))
		t.Fatalf("POST status = %d, want 202", status)
	}

	id := decodeJobID(t, raw)
	if len(id) != 24 {
		t.Fatalf("created id = %v", id)
	}

	return id
}
