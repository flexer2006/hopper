package domain_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/flexer2006/hopper/internal/domain"
)

func TestParseTarget(t *testing.T) {
	t.Parallel()

	oversize := "https://example.com/" + strings.Repeat("x", domain.MaxTargetRunes)

	tests := []struct {
		name    string
		raw     string
		wantErr error
	}{
		{name: "https", raw: "https://example.com/hook"},
		{name: "http", raw: "http://example.com/hook"},
		{name: "uppercase scheme", raw: "HTTPS://example.com/hook"},
		{name: "empty", raw: "", wantErr: domain.ErrInvalidTarget},
		{name: "relative", raw: "/hook", wantErr: domain.ErrInvalidTarget},
		{name: "ftp", raw: "ftp://example.com/", wantErr: domain.ErrInvalidTarget},
		{name: "userinfo", raw: "https://user@" + "example.com/", wantErr: domain.ErrInvalidTarget},
		{name: "opaque", raw: "http:opaque", wantErr: domain.ErrInvalidTarget},
		{name: "empty host", raw: "https://", wantErr: domain.ErrInvalidTarget},
		{name: "bad parse", raw: "http://[", wantErr: domain.ErrInvalidTarget},
		{name: "too long", raw: oversize, wantErr: domain.ErrInvalidTarget},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := domain.ParseTarget(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ParseTarget() err = %v, want %v", err, tc.wantErr)
				}

				if parsed != nil {
					t.Fatal("ParseTarget() url != nil on error")
				}

				valErr := domain.ValidateTarget(tc.raw)
				if !errors.Is(valErr, tc.wantErr) {
					t.Fatalf("ValidateTarget() err = %v, want %v", valErr, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseTarget() unexpected err = %v", err)
			}

			if parsed == nil || parsed.Host == "" {
				t.Fatal("ParseTarget() missing host")
			}

			if utf8.RuneCountInString(tc.raw) > domain.MaxTargetRunes {
				t.Fatal("accepted oversized target")
			}
		})
	}
}

func TestOutboundIdempotencyKey(t *testing.T) {
	t.Parallel()

	got := domain.OutboundIdempotencyKey("abc", 2, 3)
	want := "hopper/abc/2/3"
	if got != want {
		t.Fatalf("OutboundIdempotencyKey() = %q, want %q (AT-IDEM-01)", got, want)
	}

	job := mustRunning(t, 2)
	key := domain.OutboundIdempotencyKey(job.ID, job.Cycle, job.AttemptNumber())
	if key != "hopper/job-1/0/1" {
		t.Fatalf("claim key = %q", key)
	}

	_, err := job.RecordFailure(&domain.Attempt{
		Error:      "delivery failed",
		StatusCode: http.StatusBadGateway,
	})
	if err != nil {
		t.Fatalf("RecordFailure() err = %v", err)
	}

	if claimErr := job.Claim(); claimErr != nil {
		t.Fatalf("Claim() err = %v", claimErr)
	}

	next := domain.OutboundIdempotencyKey(job.ID, job.Cycle, job.AttemptNumber())
	if next != "hopper/job-1/0/2" {
		t.Fatalf("retry key = %q, want hopper/job-1/0/2 (AT-IDEM-07)", next)
	}
}

func TestAttemptValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		row     domain.Attempt
		wantErr error
	}{
		{
			name: "success",
			row: domain.Attempt{
				Outcome:    domain.OutcomeSuccess,
				StatusCode: http.StatusOK,
			},
		},
		{
			name: "failure",
			row: domain.Attempt{
				Error:        failureText,
				Outcome:      domain.OutcomeFailure,
				FailureClass: domain.ClassRetryable,
			},
		},
		{
			name:    "negative duration",
			row:     domain.Attempt{Outcome: domain.OutcomeSuccess, StatusCode: http.StatusOK, DurationMS: -1},
			wantErr: domain.ErrInvalidAttempt,
		},
		{
			name: "success with class",
			row: domain.Attempt{
				Outcome:      domain.OutcomeSuccess,
				StatusCode:   http.StatusOK,
				FailureClass: domain.ClassRetryable,
			},
			wantErr: domain.ErrInvalidAttempt,
		},
		{
			name:    "success zero status",
			row:     domain.Attempt{Outcome: domain.OutcomeSuccess},
			wantErr: domain.ErrInvalidAttempt,
		},
		{
			name:    "failure empty class",
			row:     domain.Attempt{Error: failureText, Outcome: domain.OutcomeFailure},
			wantErr: domain.ErrInvalidAttempt,
		},
		{
			name:    "failure empty error",
			row:     domain.Attempt{Outcome: domain.OutcomeFailure, FailureClass: domain.ClassRetryable},
			wantErr: domain.ErrInvalidAttempt,
		},
		{
			name:    "unknown outcome",
			row:     domain.Attempt{Outcome: "maybe"},
			wantErr: domain.ErrInvalidAttempt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.row.Validate()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Validate() err = %v, want %v", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("Validate() unexpected err = %v", err)
			}
		})
	}

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		var row *domain.Attempt
		if err := row.Validate(); !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("Validate(nil) err = %v, want ErrInvalidAttempt", err)
		}
	})
}

func FuzzParseTarget(f *testing.F) {
	f.Add("https://example.com/hook")
	f.Add("http://example.com/")
	f.Add("https://user@example.com/")
	f.Add("ftp://example.com/")
	f.Add("http:opaque")
	f.Add("")
	f.Add("http://example.com/%zz")
	f.Add("https://")
	f.Add("/relative")

	f.Fuzz(func(t *testing.T, raw string) {
		parsed, err := domain.ParseTarget(raw)
		if err != nil {
			if parsed != nil {
				t.Fatalf("ParseTarget(%q) err with non-nil URL", raw)
			}

			return
		}

		if parsed == nil {
			t.Fatalf("ParseTarget(%q) nil URL without error", raw)
		}

		if !parsed.IsAbs() {
			t.Fatalf("accepted non-absolute %q", raw)
		}

		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			t.Fatalf("accepted scheme %q in %q", parsed.Scheme, raw)
		}

		if parsed.User != nil {
			t.Fatalf("accepted userinfo in %q", raw)
		}

		if parsed.Opaque != "" {
			t.Fatalf("accepted opaque %q", raw)
		}

		if parsed.Host == "" {
			t.Fatalf("accepted empty host %q", raw)
		}

		if utf8.RuneCountInString(raw) > domain.MaxTargetRunes {
			t.Fatalf("accepted oversized %q", raw)
		}
	})
}
