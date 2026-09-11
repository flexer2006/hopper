package hopper_test

import (
	"net/url"
	"os"
	"strings"
	"testing"
)

func apiBase() string {
	if v := strings.TrimSpace(os.Getenv("HOPPER_INT_BASE_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}

	return "http://127.0.0.1:8080"
}

func composeDNS() bool {
	return os.Getenv("HOPPER_INT_COMPOSE_DNS") == "1"
}

func loopbackMongo(uri string) string {
	if composeDNS() {
		return uri
	}

	rewritten := strings.ReplaceAll(uri, "mongodb://mongo:", "mongodb://127.0.0.1:")
	rewritten = strings.ReplaceAll(rewritten, "@mongo:", "@127.0.0.1:")

	return setQuery(rewritten, "directConnection", "true")
}

func loopbackAMQP(uri string) string {
	if composeDNS() {
		return uri
	}

	rewritten := strings.ReplaceAll(uri, "@rabbitmq:", "@127.0.0.1:")
	rewritten = strings.ReplaceAll(rewritten, "amqp://rabbitmq:", "amqp://127.0.0.1:")
	rewritten = strings.ReplaceAll(rewritten, "amqps://rabbitmq:", "amqps://127.0.0.1:")

	return rewritten
}

func setQuery(uri, key, value string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return uri
	}

	query := parsed.Query()
	query.Set(key, value)
	parsed.RawQuery = query.Encode()

	return parsed.String()
}

func TestLoopbackMongoRewritesHostAndSetsDirectConnection(t *testing.T) {
	t.Setenv("HOPPER_INT_COMPOSE_DNS", "")

	got := loopbackMongo("mongodb://mongo:27017/?replicaSet=rs0")
	if !strings.Contains(got, "127.0.0.1") {
		t.Fatalf("host rewrite missing")
	}

	if !strings.Contains(got, "directConnection=true") {
		t.Fatal("directConnection missing")
	}

	if !strings.Contains(got, "replicaSet=rs0") {
		t.Fatal("replicaSet dropped")
	}
}

func TestLoopbackMongoKeepsComposeDNS(t *testing.T) {
	t.Setenv("HOPPER_INT_COMPOSE_DNS", "1")

	in := "mongodb://mongo:27017/?replicaSet=rs0"
	if loopbackMongo(in) != in {
		t.Fatal("compose DNS URI rewritten")
	}
}

func TestLoopbackAMQPRewritesHost(t *testing.T) {
	t.Setenv("HOPPER_INT_COMPOSE_DNS", "")

	got := loopbackAMQP("amqp://hopper:x@rabbitmq:5672/")
	if !strings.Contains(got, "@127.0.0.1:") {
		t.Fatal("amqp host rewrite missing")
	}

	if strings.Contains(got, "@rabbitmq:") {
		t.Fatal("amqp still uses rabbitmq hostname")
	}
}

func TestAPIBaseEnv(t *testing.T) {
	t.Setenv("HOPPER_INT_BASE_URL", "http://127.0.0.1:9999/")
	if apiBase() != "http://127.0.0.1:9999" {
		t.Fatalf("apiBase = %s", apiBase())
	}
}
