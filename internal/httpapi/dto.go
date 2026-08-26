package httpapi

import (
	"encoding/json"
	"time"

	"github.com/flexer2006/hopper/internal/query"
)

type createRequest struct {
	Payload     json.RawMessage `json:"payload"`
	Type        string          `json:"type"`
	Target      string          `json:"target"`
	MaxAttempts int             `json:"max_attempts"`
}

type idBody struct {
	ID string `json:"id"`
}

type replayBody struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Cycle  int    `json:"cycle"`
}

type listBody struct {
	Items []jobSummaryDTO `json:"items"`
}

type jobSummaryDTO struct {
	UpdatedAt    time.Time `json:"updated_at"`
	ID           string    `json:"id"`
	Type         string    `json:"type"`
	Status       string    `json:"status"`
	Cycle        int       `json:"cycle"`
	AttemptsDone int       `json:"attempts_done"`
	MaxAttempts  int       `json:"max_attempts"`
}

type jobDTO struct {
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Payload       json.RawMessage `json:"payload"`
	Attempts      []attemptDTO    `json:"attempts"`
	ReplayHistory []replayDTO     `json:"replay_history,omitempty"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Target        string          `json:"target"`
	Status        string          `json:"status"`
	Cycle         int             `json:"cycle"`
	AttemptsDone  int             `json:"attempts_done"`
	MaxAttempts   int             `json:"max_attempts"`
}

type attemptDTO struct {
	At           time.Time `json:"at"`
	Error        string    `json:"error,omitempty"`
	Outcome      string    `json:"outcome"`
	FailureClass string    `json:"failure_class,omitempty"`
	Cycle        int       `json:"cycle"`
	Attempt      int       `json:"attempt"`
	DurationMS   int       `json:"duration_ms"`
	StatusCode   int       `json:"status_code,omitempty"`
}

type replayDTO struct {
	At        time.Time `json:"at"`
	By        string    `json:"by,omitempty"`
	FromCycle int       `json:"from_cycle"`
	ToCycle   int       `json:"to_cycle"`
}

type healthBody struct {
	Checks map[string]string `json:"checks"`
	Status string            `json:"status"`
}

func publicJob(job query.Job) jobDTO { //nolint:gocritic // hugeParam: query.Job public view
	attempts := make([]attemptDTO, 0, len(job.Attempts))
	for i := range job.Attempts {
		row := job.Attempts[i]
		attempts = append(attempts, attemptDTO{
			At:           row.At,
			Error:        row.Error,
			Outcome:      row.Outcome,
			FailureClass: row.FailureClass,
			Cycle:        row.Cycle,
			Attempt:      row.Number,
			DurationMS:   row.DurationMS,
			StatusCode:   row.StatusCode,
		})
	}

	payload := job.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	out := jobDTO{
		CreatedAt:     job.CreatedAt,
		UpdatedAt:     job.UpdatedAt,
		Payload:       payload,
		Attempts:      attempts,
		ReplayHistory: publicReplays(job.ReplayHistory),
		ID:            job.ID,
		Type:          string(job.Type),
		Target:        job.Target,
		Status:        string(job.Status),
		Cycle:         job.Cycle,
		AttemptsDone:  job.AttemptsDone,
		MaxAttempts:   job.MaxAttempts,
	}

	return out
}

func publicSummary(job query.Job) jobSummaryDTO { //nolint:gocritic // hugeParam: query.Job summary view
	return jobSummaryDTO{
		UpdatedAt:    job.UpdatedAt,
		ID:           job.ID,
		Type:         string(job.Type),
		Status:       string(job.Status),
		Cycle:        job.Cycle,
		AttemptsDone: job.AttemptsDone,
		MaxAttempts:  job.MaxAttempts,
	}
}

func publicReplays(rows []query.ReplayEvent) []replayDTO {
	if len(rows) == 0 {
		return nil
	}

	out := make([]replayDTO, 0, len(rows))
	for i := range rows {
		row := rows[i]
		out = append(out, replayDTO{
			At:        row.At,
			By:        row.By,
			FromCycle: row.FromCycle,
			ToCycle:   row.ToCycle,
		})
	}

	return out
}
