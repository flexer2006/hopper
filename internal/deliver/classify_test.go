package deliver_test

import (
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

func TestClassifyPostSuccessAndTerminal(t *testing.T) {
	t.Parallel()

	kind, class, detail := deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusOK}, nil)
	if kind != domain.OutcomeSuccess || class != "" || detail == "" {
		t.Fatalf("2xx = %s %s %q", kind, class, detail)
	}

	kind, class, _ = deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusMovedPermanently}, nil)
	if kind != domain.OutcomeFailure || class != domain.ClassTerminalHTTP {
		t.Fatalf("301 = %s %s", kind, class)
	}

	_, class, _ = deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusNotFound}, nil)
	if class != domain.ClassTerminalHTTP {
		t.Fatalf("404 class = %s", class)
	}

	_, class, _ = deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusServiceUnavailable}, nil)
	if class != domain.ClassRetryable {
		t.Fatalf("503 class = %s", class)
	}

	_, class, _ = deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusRequestTimeout}, nil)
	if class != domain.ClassRetryable {
		t.Fatalf("408 class = %s", class)
	}

	_, class, _ = deliver.ClassifyPost(deliver.HTTPResult{StatusCode: http.StatusTooManyRequests}, nil)
	if class != domain.ClassRetryable {
		t.Fatalf("429 class = %s", class)
	}
}

func TestClassifyLocalSentinels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err   error
		class domain.FailureClass
	}{
		{deliver.ErrBlocked, domain.ClassNonRetryableLocal},
		{deliver.ErrEmptyDNS, domain.ClassNonRetryableLocal},
		{deliver.ErrInvalidHTTP, domain.ClassNonRetryableLocal},
		{domain.ErrInvalidTarget, domain.ClassNonRetryableLocal},
		{deliver.ErrBodyLimit, domain.ClassRetryable},
		{errors.New("dial tcp"), domain.ClassRetryable},
		{&net.DNSError{IsNotFound: true, Err: "no such host"}, domain.ClassNonRetryableLocal},
		{&net.DNSError{IsTimeout: true, Err: "timeout"}, domain.ClassRetryable},
	}

	for i, tc := range cases {
		t.Run(string(tc.class)+string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()

			got := deliver.ClassifyLocal(tc.err)
			if got != tc.class {
				t.Fatalf("ClassifyLocal(%v) = %s, want %s", tc.err, got, tc.class)
			}

			kind, class, detail := deliver.ClassifyPost(deliver.HTTPResult{}, tc.err)
			if kind != domain.OutcomeFailure || class != tc.class || detail == "" {
				t.Fatalf("ClassifyPost err = %s %s %q", kind, class, detail)
			}
		})
	}
}

func TestClassifyLocalNil(t *testing.T) {
	t.Parallel()

	if deliver.ClassifyLocal(nil) != "" {
		t.Fatal("nil local class")
	}
}
