package domain

import "fmt"

type Status string

type JobType string

type Job struct {
	Attempts       []Attempt
	ID             string
	Target         string
	Type           JobType
	Status         Status
	Cycle          int
	AttemptsDone   int
	DeliveryStarts int
	MaxAttempts    int
	ReplayCount    int
}

type Route struct {
	Queue        string
	Status       Status
	DelaySeconds int
}

type NewParams struct {
	ID          string
	Target      string
	Type        JobType
	MaxAttempts int
}

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead"

	TypeHTTPPost JobType = "http_post"

	DefaultMaxAttempts  = 5
	MinMaxAttempts      = 1
	MaxMaxAttempts      = 20
	ReplayCap           = 20
	DeliveryStartsSlack = 3
)

func NewJob(params NewParams) (*Job, error) {
	if params.ID == "" {
		return nil, ErrInvalidJobID
	}

	jobType := params.Type
	if jobType == "" {
		jobType = TypeHTTPPost
	}

	if jobType != TypeHTTPPost {
		return nil, ErrInvalidType
	}

	maxAttempts := params.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}

	if maxAttempts < MinMaxAttempts || maxAttempts > MaxMaxAttempts {
		return nil, ErrInvalidMaxAttempts
	}

	valErr := ValidateTarget(params.Target)
	if valErr != nil {
		return nil, valErr
	}

	return &Job{
		Attempts:       nil,
		ID:             params.ID,
		Target:         params.Target,
		Type:           jobType,
		Status:         StatusQueued,
		Cycle:          0,
		AttemptsDone:   0,
		DeliveryStarts: 0,
		MaxAttempts:    maxAttempts,
		ReplayCount:    0,
	}, nil
}

func (j *Job) AttemptNumber() int {
	return j.AttemptsDone + 1
}

func (j *Job) DeliveryStartsCap() int {
	return j.MaxAttempts + DeliveryStartsSlack
}

func (j *Job) Claim() error {
	if j.Status != StatusQueued {
		return fmt.Errorf("%w: claim from %s", ErrIllegalTransition, j.Status)
	}

	if j.DeliveryStarts >= j.DeliveryStartsCap() {
		j.Status = StatusDead

		return ErrDeliveryCap
	}

	j.Status = StatusRunning
	j.DeliveryStarts++

	return nil
}

func (j *Job) ReleaseLease() error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: release from %s", ErrIllegalTransition, j.Status)
	}

	j.Status = StatusQueued

	return nil
}

func (j *Job) Replay() error {
	if j.Status != StatusDead {
		return ErrReplayNotDead
	}

	if j.ReplayCount >= ReplayCap {
		return ErrReplayCap
	}

	j.Cycle++
	j.AttemptsDone = 0
	j.DeliveryStarts = 0
	j.ReplayCount++
	j.Status = StatusQueued

	return nil
}

func (j *Job) AssertHTTPAllowed() error {
	switch j.Status {
	case StatusRunning:
		if j.HasSuccessFor(j.Cycle, j.AttemptNumber()) {
			return ErrSkipHTTP
		}

		return nil
	case StatusQueued, StatusSucceeded, StatusDead:
		return fmt.Errorf("%w: status %s", ErrHTTPForbidden, j.Status)
	default:
		return fmt.Errorf("%w: status %s", ErrIllegalTransition, j.Status)
	}
}

func (j *Job) HasSuccessFor(cycle, attempt int) bool {
	for i := range j.Attempts {
		row := j.Attempts[i]
		if row.Cycle == cycle && row.Number == attempt && row.Outcome == OutcomeSuccess {
			return true
		}
	}

	return false
}
