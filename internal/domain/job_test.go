package domain_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/domain"
)

const (
	testJobID     = "job-1"
	testTargetURL = "https://example.com/hook"
	failureText   = "delivery failed"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func mustQueued(t *testing.T, maxAttempts int) *domain.Job {
	t.Helper()

	job, err := domain.NewJob(domain.NewParams{
		ID:          testJobID,
		Target:      testTargetURL,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("NewJob() err = %v", err)
	}

	return job
}

func mustRunning(t *testing.T, maxAttempts int) *domain.Job {
	t.Helper()

	job := mustQueued(t, maxAttempts)
	if err := job.Claim(); err != nil {
		t.Fatalf("Claim() err = %v", err)
	}

	return job
}

func TestNewJobDefaultsAndRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  domain.NewParams
		wantErr error
		wantMax int
		wantTyp domain.JobType
	}{
		{
			name: "defaults type and max_attempts",
			params: domain.NewParams{
				ID:     testJobID,
				Target: testTargetURL,
			},
			wantMax: domain.DefaultMaxAttempts,
			wantTyp: domain.TypeHTTPPost,
		},
		{
			name: "empty id",
			params: domain.NewParams{
				Target: testTargetURL,
			},
			wantErr: domain.ErrInvalidJobID,
		},
		{
			name: "invalid type",
			params: domain.NewParams{
				ID:     testJobID,
				Target: testTargetURL,
				Type:   "cron",
			},
			wantErr: domain.ErrInvalidType,
		},
		{
			name: "max attempts high",
			params: domain.NewParams{
				ID:          testJobID,
				Target:      testTargetURL,
				MaxAttempts: domain.MaxMaxAttempts + 1,
			},
			wantErr: domain.ErrInvalidMaxAttempts,
		},
		{
			name: "max attempts negative",
			params: domain.NewParams{
				ID:          testJobID,
				Target:      testTargetURL,
				MaxAttempts: -1,
			},
			wantErr: domain.ErrInvalidMaxAttempts,
		},
		{
			name: "invalid target",
			params: domain.NewParams{
				ID:     testJobID,
				Target: "ftp://example.com/",
			},
			wantErr: domain.ErrInvalidTarget,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			job, err := domain.NewJob(tc.params)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("NewJob() err = %v, want %v", err, tc.wantErr)
				}

				if job != nil {
					t.Fatal("NewJob() job != nil on error")
				}

				return
			}

			if err != nil {
				t.Fatalf("NewJob() unexpected err = %v", err)
			}

			if job.Status != domain.StatusQueued {
				t.Fatalf("Status = %s, want queued", job.Status)
			}

			if job.Type != tc.wantTyp {
				t.Fatalf("Type = %s, want %s", job.Type, tc.wantTyp)
			}

			if job.MaxAttempts != tc.wantMax {
				t.Fatalf("MaxAttempts = %d, want %d", job.MaxAttempts, tc.wantMax)
			}

			if job.Cycle != 0 || job.AttemptsDone != 0 || job.DeliveryStarts != 0 || job.ReplayCount != 0 {
				t.Fatalf("counters not zero: %+v", job)
			}
		})
	}
}

func TestClaimReleaseAndDeliveryCap(t *testing.T) {
	t.Parallel()

	t.Run("claim from running", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		if err := job.Claim(); !errors.Is(err, domain.ErrIllegalTransition) {
			t.Fatalf("Claim() err = %v, want ErrIllegalTransition", err)
		}
	})

	t.Run("release lease", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		if err := job.ReleaseLease(); err != nil {
			t.Fatalf("ReleaseLease() err = %v", err)
		}

		if job.Status != domain.StatusQueued {
			t.Fatalf("Status = %s, want queued", job.Status)
		}

		if job.AttemptsDone != 0 {
			t.Fatalf("AttemptsDone = %d, want 0", job.AttemptsDone)
		}
	})

	t.Run("release from queued", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 2)
		if err := job.ReleaseLease(); !errors.Is(err, domain.ErrIllegalTransition) {
			t.Fatalf("ReleaseLease() err = %v, want ErrIllegalTransition", err)
		}
	})

	t.Run("delivery_starts cap", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 1)
		job.DeliveryStarts = job.DeliveryStartsCap()
		if err := job.Claim(); !errors.Is(err, domain.ErrDeliveryCap) {
			t.Fatalf("Claim() err = %v, want ErrDeliveryCap", err)
		}

		if job.Status != domain.StatusDead {
			t.Fatalf("Status = %s, want dead", job.Status)
		}
	})
}

func TestReplayKeepsAttempts(t *testing.T) {
	t.Parallel()

	job := mustRunning(t, 1)
	route, err := job.RecordFailure(&domain.Attempt{
		Error:      failureText,
		StatusCode: http.StatusNotFound,
	})
	if err != nil {
		t.Fatalf("RecordFailure() err = %v", err)
	}

	if route.Queue != domain.QueueDLQ || job.Status != domain.StatusDead {
		t.Fatalf("route=%+v status=%s, want dlq/dead", route, job.Status)
	}

	history := len(job.Attempts)
	if replayErr := job.Replay(); replayErr != nil {
		t.Fatalf("Replay() err = %v", replayErr)
	}

	if job.Status != domain.StatusQueued {
		t.Fatalf("Status = %s, want queued", job.Status)
	}

	if job.Cycle != 1 || job.AttemptsDone != 0 || job.DeliveryStarts != 0 || job.ReplayCount != 1 {
		t.Fatalf("replay counters: cycle=%d done=%d starts=%d replay=%d",
			job.Cycle, job.AttemptsDone, job.DeliveryStarts, job.ReplayCount)
	}

	if len(job.Attempts) != history {
		t.Fatalf("attempts len = %d, want %d (DL-08 retain)", len(job.Attempts), history)
	}
}

func TestReplayRejects(t *testing.T) {
	t.Parallel()

	t.Run("not dead", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 1)
		if err := job.Replay(); !errors.Is(err, domain.ErrReplayNotDead) {
			t.Fatalf("Replay() err = %v, want ErrReplayNotDead", err)
		}
	})

	t.Run("cap", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 1)
		job.Status = domain.StatusDead
		job.ReplayCount = domain.ReplayCap
		if err := job.Replay(); !errors.Is(err, domain.ErrReplayCap) {
			t.Fatalf("Replay() err = %v, want ErrReplayCap", err)
		}
	})
}

func TestAssertHTTPAllowed(t *testing.T) {
	t.Parallel()

	t.Run("running ok", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		if err := job.AssertHTTPAllowed(); err != nil {
			t.Fatalf("AssertHTTPAllowed() err = %v", err)
		}
	})

	t.Run("skip when success recorded", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		job.Attempts = []domain.Attempt{{
			Cycle:      job.Cycle,
			Number:     job.AttemptNumber(),
			Outcome:    domain.OutcomeSuccess,
			StatusCode: http.StatusOK,
		}}
		if err := job.AssertHTTPAllowed(); !errors.Is(err, domain.ErrSkipHTTP) {
			t.Fatalf("AssertHTTPAllowed() err = %v, want ErrSkipHTTP", err)
		}
	})

	t.Run("forbidden statuses", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 1)
		statuses := []domain.Status{domain.StatusQueued, domain.StatusSucceeded, domain.StatusDead}
		for _, status := range statuses {
			job.Status = status
			if err := job.AssertHTTPAllowed(); !errors.Is(err, domain.ErrHTTPForbidden) {
				t.Fatalf("status %s err = %v, want ErrHTTPForbidden", status, err)
			}
		}
	})

	t.Run("unknown status", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 1)
		job.Status = "retry_wait"
		if err := job.AssertHTTPAllowed(); !errors.Is(err, domain.ErrIllegalTransition) {
			t.Fatalf("AssertHTTPAllowed() err = %v, want ErrIllegalTransition", err)
		}
	})
}

func TestHasSuccessForMiss(t *testing.T) {
	t.Parallel()

	job := mustQueued(t, 1)
	job.Attempts = []domain.Attempt{{
		Cycle:      0,
		Number:     1,
		Outcome:    domain.OutcomeFailure,
		StatusCode: http.StatusNotFound,
	}}
	if job.HasSuccessFor(0, 1) {
		t.Fatal("HasSuccessFor() true for failure row")
	}

	if job.HasSuccessFor(1, 1) {
		t.Fatal("HasSuccessFor() true for other cycle")
	}
}

func TestRecordSuccess(t *testing.T) {
	t.Parallel()

	t.Run("2xx", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		err := job.RecordSuccess(&domain.Attempt{StatusCode: http.StatusOK, DurationMS: 12})
		if err != nil {
			t.Fatalf("RecordSuccess() err = %v", err)
		}

		if job.Status != domain.StatusSucceeded {
			t.Fatalf("Status = %s, want succeeded", job.Status)
		}

		if job.AttemptsDone != 1 || len(job.Attempts) != 1 {
			t.Fatalf("attempts done=%d len=%d", job.AttemptsDone, len(job.Attempts))
		}

		if !job.HasSuccessFor(0, 1) {
			t.Fatal("HasSuccessFor(0,1) = false")
		}
	})

	t.Run("from queued", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 2)
		err := job.RecordSuccess(&domain.Attempt{StatusCode: http.StatusOK})
		if !errors.Is(err, domain.ErrIllegalTransition) {
			t.Fatalf("RecordSuccess() err = %v, want ErrIllegalTransition", err)
		}
	})

	t.Run("non 2xx", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		err := job.RecordSuccess(&domain.Attempt{StatusCode: http.StatusNotFound})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordSuccess() err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("invalid status", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		err := job.RecordSuccess(&domain.Attempt{StatusCode: 0})
		if !errors.Is(err, domain.ErrInvalidHTTPStatus) {
			t.Fatalf("RecordSuccess() err = %v, want ErrInvalidHTTPStatus", err)
		}
	})

	t.Run("nil row", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		err := job.RecordSuccess(nil)
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordSuccess(nil) err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("negative duration", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		err := job.RecordSuccess(&domain.Attempt{StatusCode: http.StatusOK, DurationMS: -1})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordSuccess() err = %v, want ErrInvalidAttempt", err)
		}
	})
}

func TestRecordFailureHTTPRoutes(t *testing.T) {
	t.Parallel()

	t.Run("404 fail-fast dead AT-UC04-02", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 5)
		route, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusNotFound,
		})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		if route.Queue != domain.QueueDLQ || route.Status != domain.StatusDead {
			t.Fatalf("route=%+v, want dlq/dead", route)
		}

		if job.Attempts[0].FailureClass != domain.ClassTerminalHTTP {
			t.Fatalf("class = %s, want terminal_http", job.Attempts[0].FailureClass)
		}
	})

	t.Run("408 then exhaust AT-UC03-03", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		first, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusRequestTimeout,
		})
		if err != nil {
			t.Fatalf("first RecordFailure() err = %v", err)
		}

		if first.Queue != "jobs.delay.1s" || first.DelaySeconds != 1 || job.Status != domain.StatusQueued {
			t.Fatalf("first route=%+v status=%s", first, job.Status)
		}

		if claimErr := job.Claim(); claimErr != nil {
			t.Fatalf("Claim() err = %v", claimErr)
		}

		second, secondErr := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusTooManyRequests,
		})
		if secondErr != nil {
			t.Fatalf("second RecordFailure() err = %v", secondErr)
		}

		if second.Queue != domain.QueueDLQ || job.Status != domain.StatusDead {
			t.Fatalf("second route=%+v status=%s, want dlq/dead", second, job.Status)
		}
	})

	t.Run("503 delay then 5xx dead", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		route, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusServiceUnavailable,
		})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		if route.Queue != "jobs.delay.1s" {
			t.Fatalf("queue = %s, want jobs.delay.1s", route.Queue)
		}
	})

	t.Run("from queued", func(t *testing.T) {
		t.Parallel()

		job := mustQueued(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{Error: failureText, StatusCode: http.StatusNotFound})
		if !errors.Is(err, domain.ErrIllegalTransition) {
			t.Fatalf("RecordFailure() err = %v, want ErrIllegalTransition", err)
		}
	})

	t.Run("success outcome rejected", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			Outcome:    domain.OutcomeSuccess,
			StatusCode: http.StatusNotFound,
		})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("2xx as failure", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{Error: failureText, StatusCode: http.StatusOK})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("empty error", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{StatusCode: http.StatusNotFound})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("unknown class", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{Error: failureText, FailureClass: "bogus"})
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("invalid backoff n", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 5)
		job.AttemptsDone = -1
		_, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusBadGateway,
		})
		if !errors.Is(err, domain.ErrInvalidBackoff) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidBackoff", err)
		}

		if len(job.Attempts) != 0 {
			t.Fatal("attempts mutated after backoff error")
		}
	})

	t.Run("truncate error runes", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 1)
		long := strings.Repeat("é", 1025)
		_, err := job.RecordFailure(&domain.Attempt{
			Error:      long,
			StatusCode: http.StatusNotFound,
		})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		got := job.Attempts[0].Error
		if utf8.RuneCountInString(got) != 1024 {
			t.Fatalf("error runes = %d, want 1024", utf8.RuneCountInString(got))
		}
	})

	t.Run("invalid http status", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(&domain.Attempt{Error: failureText, StatusCode: 99})
		if !errors.Is(err, domain.ErrInvalidHTTPStatus) {
			t.Fatalf("RecordFailure() err = %v, want ErrInvalidHTTPStatus", err)
		}
	})

	t.Run("local nxdomain dead", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 5)
		class, classErr := domain.ClassifyLocal(domain.LocalNXDOMAIN)
		if classErr != nil {
			t.Fatalf("ClassifyLocal() err = %v", classErr)
		}

		route, err := job.RecordFailure(&domain.Attempt{Error: failureText, FailureClass: class})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		if route.Queue != domain.QueueDLQ {
			t.Fatalf("queue = %s, want dlq", route.Queue)
		}
	})

	t.Run("local transport delay", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 5)
		class, classErr := domain.ClassifyLocal(domain.LocalTransport)
		if classErr != nil {
			t.Fatalf("ClassifyLocal() err = %v", classErr)
		}

		route, err := job.RecordFailure(&domain.Attempt{Error: failureText, FailureClass: class})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		if route.Queue != "jobs.delay.1s" || route.DelaySeconds != 1 {
			t.Fatalf("route=%+v, want 1s delay", route)
		}
	})

	t.Run("nil row", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 2)
		_, err := job.RecordFailure(nil)
		if !errors.Is(err, domain.ErrInvalidAttempt) {
			t.Fatalf("RecordFailure(nil) err = %v, want ErrInvalidAttempt", err)
		}
	})

	t.Run("1xx terminal", func(t *testing.T) {
		t.Parallel()

		job := mustRunning(t, 5)
		route, err := job.RecordFailure(&domain.Attempt{
			Error:      failureText,
			StatusCode: http.StatusContinue,
		})
		if err != nil {
			t.Fatalf("RecordFailure() err = %v", err)
		}

		if route.Queue != domain.QueueDLQ {
			t.Fatalf("queue = %s, want dlq", route.Queue)
		}
	})
}
