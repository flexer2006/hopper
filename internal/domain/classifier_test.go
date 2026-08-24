package domain_test

import (
	"errors"
	"net/http"
	"strconv"
	"testing"

	"github.com/flexer2006/hopper/internal/domain"
)

func TestClassifyHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    int
		outcome domain.Outcome
		class   domain.FailureClass
		wantErr error
	}{
		{
			name:    "informational 1xx terminal",
			code:    199,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassTerminalHTTP,
		},
		{
			name:    "continue 1xx",
			code:    http.StatusContinue,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassTerminalHTTP,
		},
		{name: "200", code: http.StatusOK, outcome: domain.OutcomeSuccess},
		{name: "299", code: 299, outcome: domain.OutcomeSuccess},
		{
			name:    "301",
			code:    http.StatusMovedPermanently,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassTerminalHTTP,
		},
		{
			name:    "404 AT-UC04-02",
			code:    http.StatusNotFound,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassTerminalHTTP,
		},
		{
			name:    "408 AT-UC03-03",
			code:    http.StatusRequestTimeout,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassRetryable,
		},
		{
			name:    "429 AT-UC03-03",
			code:    http.StatusTooManyRequests,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassRetryable,
		},
		{
			name:    "500",
			code:    http.StatusInternalServerError,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassRetryable,
		},
		{
			name:    "599",
			code:    599,
			outcome: domain.OutcomeFailure,
			class:   domain.ClassRetryable,
		},
		{name: "99", code: 99, wantErr: domain.ErrInvalidHTTPStatus},
		{name: "600", code: 600, wantErr: domain.ErrInvalidHTTPStatus},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outcome, class, err := domain.ClassifyHTTP(tc.code)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ClassifyHTTP() err = %v, want %v", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ClassifyHTTP() unexpected err = %v", err)
			}

			if outcome != tc.outcome || class != tc.class {
				t.Fatalf("got (%s,%s), want (%s,%s)", outcome, class, tc.outcome, tc.class)
			}
		})
	}
}

func TestClassifyLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    domain.LocalKind
		class   domain.FailureClass
		wantErr error
	}{
		{name: "transport", kind: domain.LocalTransport, class: domain.ClassRetryable},
		{name: "dns timeout", kind: domain.LocalDNSTimeout, class: domain.ClassRetryable},
		{name: "servfail", kind: domain.LocalSERVFAIL, class: domain.ClassRetryable},
		{name: "nxdomain", kind: domain.LocalNXDOMAIN, class: domain.ClassNonRetryableLocal},
		{name: "ssrf", kind: domain.LocalSSRF, class: domain.ClassNonRetryableLocal},
		{name: "invalid url", kind: domain.LocalInvalidURL, class: domain.ClassNonRetryableLocal},
		{name: "malformed", kind: domain.LocalMalformedBody, class: domain.ClassNonRetryableLocal},
		{name: "unspecified", kind: domain.LocalUnspecified, wantErr: domain.ErrInvalidLocalKind},
		{name: "unknown", kind: domain.LocalKind(255), wantErr: domain.ErrInvalidLocalKind},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			class, err := domain.ClassifyLocal(tc.kind)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ClassifyLocal() err = %v, want %v", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ClassifyLocal() unexpected err = %v", err)
			}

			if class != tc.class {
				t.Fatalf("class = %s, want %s", class, tc.class)
			}
		})
	}
}

func TestBackoffAndDelayQueue(t *testing.T) {
	t.Parallel()

	t.Run("seconds", func(t *testing.T) {
		t.Parallel()

		want := map[int]int{0: 1, 1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 60, 7: 60}
		for n, seconds := range want {
			got, err := domain.BackoffSeconds(n)
			if err != nil {
				t.Fatalf("BackoffSeconds(%d) err = %v", n, err)
			}

			if got != seconds {
				t.Fatalf("BackoffSeconds(%d) = %d, want %d", n, got, seconds)
			}
		}

		_, err := domain.BackoffSeconds(-1)
		if !errors.Is(err, domain.ErrInvalidBackoff) {
			t.Fatalf("BackoffSeconds(-1) err = %v, want ErrInvalidBackoff", err)
		}
	})

	t.Run("queues", func(t *testing.T) {
		t.Parallel()

		for _, seconds := range []int{1, 2, 4, 8, 16, 32, 60} {
			queue, err := domain.DelayQueue(seconds)
			if err != nil {
				t.Fatalf("DelayQueue(%d) err = %v", seconds, err)
			}

			want := "jobs.delay." + strconv.Itoa(seconds) + "s"
			if queue != want {
				t.Fatalf("DelayQueue(%d) = %s, want %s", seconds, queue, want)
			}
		}

		for _, seconds := range []int{0, 3, 64} {
			_, err := domain.DelayQueue(seconds)
			if !errors.Is(err, domain.ErrInvalidBackoff) {
				t.Fatalf("DelayQueue(%d) err = %v, want ErrInvalidBackoff", seconds, err)
			}
		}
	})
}
