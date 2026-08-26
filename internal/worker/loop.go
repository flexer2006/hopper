package worker

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

type Delivery interface {
	Body() []byte
	Ack() error
}

type AuxiliaryDLQ interface {
	Publish(ctx context.Context, body []byte) error
}

type Relayer interface {
	Tick(ctx context.Context) error
}

type Worker struct {
	jobs   deliver.Jobs
	http   deliver.HTTP
	aux    AuxiliaryDLQ
	relay  Relayer
	log    *zap.Logger
	now    func() time.Time
	id     string
	budget time.Duration
}

type Config struct {
	Now           func() time.Time
	WorkerID      string
	AttemptBudget time.Duration
}

const (
	defaultWorkerID = "worker"
	defaultBudget   = 25 * time.Second
)

func New(
	jobs deliver.Jobs,
	client deliver.HTTP,
	aux AuxiliaryDLQ,
	relay Relayer,
	log *zap.Logger,
	cfg Config,
) *Worker {
	if log == nil {
		log = zap.NewNop()
	}

	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	id := cfg.WorkerID
	if id == "" {
		id = defaultWorkerID
	}

	budget := cfg.AttemptBudget
	if budget <= 0 {
		budget = defaultBudget
	}

	w := new(Worker)
	w.jobs = jobs
	w.http = client
	w.aux = aux
	w.relay = relay
	w.log = log
	w.now = now
	w.id = id
	w.budget = budget

	return w
}

func (w *Worker) Process(ctx context.Context, msg Delivery) error {
	id, parseErr := parseJobID(msg.Body())
	if parseErr != nil {
		body, merr := malformedDLQ(msg.Body())
		if merr != nil {
			return merr
		}

		return w.poison(ctx, msg, body)
	}

	out, err := w.jobs.Claim(ctx, deliver.ClaimIn{ID: id, WorkerID: w.id})
	if err != nil {
		if errors.Is(err, domain.ErrDeliveryCap) {
			return w.afterMongo(ctx, msg)
		}

		return w.claimErr(ctx, msg, id, err)
	}

	job := snapshot(&out)

	skipErr := job.AssertHTTPAllowed()
	if errors.Is(skipErr, domain.ErrSkipHTTP) {
		return w.afterMongo(ctx, msg)
	}

	if skipErr != nil {
		return skipErr
	}

	return w.deliver(ctx, msg, &out, job)
}

func (w *Worker) claimErr(ctx context.Context, msg Delivery, id string, err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		body, merr := ghostDLQ(id)
		if merr != nil {
			return merr
		}

		return w.poison(ctx, msg, body)
	case errors.Is(err, deliver.ErrNotDue),
		errors.Is(err, deliver.ErrLeaseHeld),
		errors.Is(err, deliver.ErrTerminal),
		errors.Is(err, deliver.ErrNotRunning),
		errors.Is(err, deliver.ErrClaimLost):
		return msg.Ack()
	default:
		return err
	}
}

func (w *Worker) poison(ctx context.Context, msg Delivery, body []byte) error {
	if w.aux == nil {
		return ErrAuxiliary
	}

	err := w.aux.Publish(ctx, body)
	if err != nil {
		return err
	}

	return msg.Ack()
}

func (w *Worker) afterMongo(ctx context.Context, msg Delivery) error {
	if w.relay != nil {
		tickErr := w.relay.Tick(ctx)
		if tickErr != nil {
			w.log.Warn("relay tick", zap.Error(tickErr))
		}
	}

	return msg.Ack()
}

func (w *Worker) deliver(ctx context.Context, msg Delivery, out *deliver.ClaimOut, job *domain.Job) error {
	started := w.now()
	res, postErr := w.http.Post(ctx, deliver.HTTPRequest{
		Payload: out.Payload,
		Target:  out.Target,
		JobID:   out.ID,
		Cycle:   out.Cycle,
		Attempt: out.Attempt,
	})

	duration := max(int(w.now().Sub(started).Milliseconds()), 0)

	outcome, err := w.commit(ctx, out, job, res, postErr, duration)
	if err != nil {
		return err
	}

	w.log.Info("delivery",
		zap.String("job_id", out.ID),
		zap.Int("cycle", out.Cycle),
		zap.Int("attempt", out.Attempt),
		zap.String("outcome", outcome),
	)

	return w.afterMongo(ctx, msg)
}

func (w *Worker) commit(
	ctx context.Context,
	out *deliver.ClaimOut,
	job *domain.Job,
	res deliver.HTTPResult,
	postErr error,
	duration int,
) (string, error) {
	kind, class, detail := deliver.ClassifyPost(res, postErr)

	if kind == domain.OutcomeSuccess {
		row := domain.Attempt{
			At:           w.now(),
			Error:        "",
			Outcome:      domain.OutcomeSuccess,
			FailureClass: "",
			Cycle:        0,
			Number:       0,
			DurationMS:   duration,
			StatusCode:   res.StatusCode,
		}

		recErr := job.RecordSuccess(&row)
		if recErr != nil {
			return "", recErr
		}

		return "success", w.jobs.CommitOutcome(ctx, successIn(out, &row, job))
	}

	row := domain.Attempt{
		At:           w.now(),
		Error:        clipRunes(detail, maxErrorRunes),
		Outcome:      domain.OutcomeFailure,
		FailureClass: class,
		Cycle:        0,
		Number:       0,
		DurationMS:   duration,
		StatusCode:   res.StatusCode,
	}

	route, recErr := job.RecordFailure(&row)
	if recErr != nil {
		return "", recErr
	}

	return string(row.FailureClass), w.jobs.CommitOutcome(ctx, failIn(out, &row, route, job))
}

func snapshot(out *deliver.ClaimOut) *domain.Job {
	attempts := append([]domain.Attempt(nil), out.Attempts...)

	return &domain.Job{
		Attempts:       attempts,
		ID:             out.ID,
		Target:         out.Target,
		Type:           domain.TypeHTTPPost,
		Status:         domain.StatusRunning,
		Cycle:          out.Cycle,
		AttemptsDone:   out.Attempt - 1,
		DeliveryStarts: 0,
		MaxAttempts:    out.MaxAttempts,
		ReplayCount:    0,
	}
}
