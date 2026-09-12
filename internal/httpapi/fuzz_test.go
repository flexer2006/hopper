package httpapi_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flexer2006/hopper/internal/platform"
)

func FuzzCreateJobBody(f *testing.F) {
	f.Add([]byte(`{`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"type":"http_post","target":"https://example.invalid/webhook","payload":{},"extra":1}`))
	f.Add([]byte(`{"type":"http_post","target":"ftp://example.com/","payload":{}}`))
	f.Add([]byte(`{"type":"http_post","target":"http://127.0.0.1/hook","payload":{}}`))
	f.Add([]byte(`{"type":"http_post","target":"https://user@example.com/","payload":{}}`))
	f.Add([]byte(`{"type":"http_post","target":"https://example.invalid/webhook","payload":` + nestedJSON(10) + `}`))
	oversize := `{"type":"http_post","target":"https://example.invalid/webhook","payload":{"n":"` +
		string(bytes.Repeat([]byte("x"), 600)) + `"}}`
	f.Add([]byte(oversize))

	fx := newFixture(f)

	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 600 {
			body = body[:600]
		}

		sum := sha256.Sum256(body)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+platform.ValidToken())
		req.Header.Set("Idempotency-Key", hex.EncodeToString(sum[:8]))
		rec := httptest.NewRecorder()
		fx.h.ServeHTTP(rec, req)

		res := rec.Result()
		_, copyErr := io.Copy(io.Discard, res.Body)
		if copyErr != nil {
			t.Fatal(copyErr)
		}

		err := res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}

		if res.StatusCode >= http.StatusInternalServerError {
			t.Fatalf("status %d for %q", res.StatusCode, body)
		}
	})
}

func nestedJSON(depth int) string {
	var b bytes.Buffer

	for range depth {
		b.WriteString(`{"a":`)
	}

	b.WriteByte('1')

	for range depth {
		b.WriteByte('}')
	}

	return b.String()
}
