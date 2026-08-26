package persist

import (
	"bytes"
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/query"
)

type jobDoc struct {
	Attempts        []attemptDoc  `bson:"attempts"`
	DispatchHistory []dispatchDoc `bson:"dispatch_history,omitempty"`
	ReplayHistory   []replayDoc   `bson:"replay_history,omitempty"`
	Payload         bson.Raw      `bson:"payload"`
	Dispatch        dispatchDoc   `bson:"dispatch"`
	CreatedAt       time.Time     `bson:"created_at"`
	UpdatedAt       time.Time     `bson:"updated_at"`
	NotBefore       *time.Time    `bson:"not_before,omitempty"`
	ClaimExpiresAt  *time.Time    `bson:"claim_expires_at,omitempty"`
	ID              string        `bson:"_id"` //nolint:tagliatelle // Mongo document primary key
	Type            string        `bson:"type"`
	Target          string        `bson:"target"`
	Status          string        `bson:"status"`
	ProducerKey     string        `bson:"producer_idempotency_key"`
	RequestHash     string        `bson:"request_hash"`
	FenceToken      string        `bson:"fence_token,omitempty"`
	ClaimedBy       string        `bson:"claimed_by,omitempty"`
	Cycle           int           `bson:"cycle"`
	AttemptsDone    int           `bson:"attempts_done"`
	DeliveryStarts  int           `bson:"delivery_starts"`
	MaxAttempts     int           `bson:"max_attempts"`
	ReplayCount     int           `bson:"replay_count"`
}

type attemptDoc struct {
	At           time.Time `bson:"at"`
	Error        string    `bson:"error,omitempty"`
	Outcome      string    `bson:"outcome"`
	FailureClass string    `bson:"failure_class,omitempty"`
	Cycle        int       `bson:"cycle"`
	Number       int       `bson:"attempt"`
	DurationMS   int       `bson:"duration_ms"`
	StatusCode   int       `bson:"status_code,omitempty"`
}

type dispatchDoc struct {
	CreatedAt   time.Time  `bson:"created_at"`
	PublishedAt *time.Time `bson:"published_at,omitempty"`
	NotBefore   *time.Time `bson:"not_before,omitempty"`
	Intent      string     `bson:"intent"`
	Queue       string     `bson:"queue"`
	Status      string     `bson:"status"`
	Generation  int        `bson:"generation"`
	Cycle       int        `bson:"cycle,omitempty"`
	Attempt     int        `bson:"attempt,omitempty"`
}

type replayDoc struct {
	At        time.Time `bson:"at"`
	By        string    `bson:"by,omitempty"`
	FromCycle int       `bson:"from_cycle"`
	ToCycle   int       `bson:"to_cycle"`
}

func payloadRaw(data []byte) (bson.Raw, error) {
	if len(data) == 0 {
		data = []byte("{}")
	}

	if len(data) > maxPayloadBytes {
		return nil, ErrTooLarge
	}

	if !json.Valid(data) || !bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		return nil, ErrPayload
	}

	var val any

	err := json.Unmarshal(data, &val)
	if err != nil {
		return nil, ErrPayload
	}

	encoded, err := bson.Marshal(val)
	if err != nil {
		return nil, ErrPayload
	}

	return encoded, nil
}

//nolint:gocritic // hugeParam: matches enqueue.Record
func insertDoc(rec enqueue.Record, now time.Time) (jobDoc, error) {
	if !validHex(rec.ID, hexIDLen) {
		return jobDoc{}, ErrInvalidID
	}

	if !validHex(rec.RequestHash, hexHashLen) {
		return jobDoc{}, ErrInvalidHash
	}

	if rec.ProducerKey == "" || len(rec.ProducerKey) > maxProducerKey {
		return jobDoc{}, ErrInvalidKey
	}

	job, err := domain.NewJob(domain.NewParams{
		ID:          rec.ID,
		Target:      rec.Target,
		Type:        rec.Type,
		MaxAttempts: rec.MaxAttempts,
	})
	if err != nil {
		return jobDoc{}, err
	}

	raw, rawErr := payloadRaw(rec.Payload)
	if rawErr != nil {
		return jobDoc{}, rawErr
	}

	return jobDoc{
		Attempts:        []attemptDoc{},
		DispatchHistory: nil,
		ReplayHistory:   nil,
		Payload:         raw,
		Dispatch: dispatchDoc{
			CreatedAt:   now,
			PublishedAt: nil,
			NotBefore:   nil,
			Intent:      dispatch.IntentEnqueue,
			Queue:       domain.QueueJobs,
			Status:      dispatch.StatusPending,
			Generation:  firstGeneration,
			Cycle:       job.Cycle,
			Attempt:     0,
		},
		CreatedAt:      now,
		UpdatedAt:      now,
		NotBefore:      nil,
		ClaimExpiresAt: nil,
		ID:             job.ID,
		Type:           string(job.Type),
		Target:         job.Target,
		Status:         string(job.Status),
		ProducerKey:    rec.ProducerKey,
		RequestHash:    rec.RequestHash,
		FenceToken:     "",
		ClaimedBy:      "",
		Cycle:          job.Cycle,
		AttemptsDone:   job.AttemptsDone,
		DeliveryStarts: job.DeliveryStarts,
		MaxAttempts:    job.MaxAttempts,
		ReplayCount:    job.ReplayCount,
	}, nil
}

func (d *jobDoc) existing() enqueue.Existing {
	return enqueue.Existing{
		ID:             d.ID,
		RequestHash:    d.RequestHash,
		DispatchStatus: d.Dispatch.Status,
		Queue:          d.Dispatch.Queue,
		Kind:           d.Dispatch.Intent,
		Generation:     d.Dispatch.Generation,
	}
}

func (d *jobDoc) intent() dispatch.Intent {
	return dispatch.Intent{
		ID:         d.ID,
		Queue:      d.Dispatch.Queue,
		Kind:       d.Dispatch.Intent,
		Generation: d.Dispatch.Generation,
	}
}

func (d *jobDoc) queryJob() query.Job {
	job := d.querySummary()
	job.Payload = payloadJSON(d.Payload)
	job.Attempts = queryAttempts(d.Attempts)
	job.ReplayHistory = queryReplays(d.ReplayHistory)

	return job
}

func (d *jobDoc) querySummary() query.Job {
	return query.Job{
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
		Payload:       nil,
		Attempts:      nil,
		ReplayHistory: nil,
		ID:            d.ID,
		Type:          domain.JobType(d.Type),
		Target:        d.Target,
		Status:        domain.Status(d.Status),
		Cycle:         d.Cycle,
		AttemptsDone:  d.AttemptsDone,
		MaxAttempts:   d.MaxAttempts,
		ReplayCount:   d.ReplayCount,
	}
}

func payloadJSON(raw bson.Raw) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}

	var val map[string]any

	err := bson.Unmarshal(raw, &val)
	if err != nil {
		return json.RawMessage("{}")
	}

	encoded, err := json.Marshal(val)
	if err != nil {
		return json.RawMessage("{}")
	}

	return encoded
}

func queryAttempts(rows []attemptDoc) []query.Attempt {
	out := make([]query.Attempt, 0, len(rows))
	for i := range rows {
		row := rows[i]
		out = append(out, query.Attempt{
			At:           row.At,
			Error:        row.Error,
			Outcome:      row.Outcome,
			FailureClass: row.FailureClass,
			Cycle:        row.Cycle,
			Number:       row.Number,
			DurationMS:   row.DurationMS,
			StatusCode:   row.StatusCode,
		})
	}

	return out
}

func queryReplays(rows []replayDoc) []query.ReplayEvent {
	out := make([]query.ReplayEvent, 0, len(rows))
	for i := range rows {
		row := rows[i]
		out = append(out, query.ReplayEvent{
			At:        row.At,
			By:        row.By,
			FromCycle: row.FromCycle,
			ToCycle:   row.ToCycle,
		})
	}

	return out
}

func domainAttemptRows(rows []attemptDoc) []domain.Attempt {
	out := make([]domain.Attempt, 0, len(rows))
	for i := range rows {
		row := rows[i]
		out = append(out, domain.Attempt{
			At:           row.At,
			Error:        row.Error,
			Outcome:      domain.Outcome(row.Outcome),
			FailureClass: domain.FailureClass(row.FailureClass),
			Cycle:        row.Cycle,
			Number:       row.Number,
			DurationMS:   row.DurationMS,
			StatusCode:   row.StatusCode,
		})
	}

	return out
}

func mapAttempts(rows []domain.Attempt) []attemptDoc {
	out := make([]attemptDoc, 0, len(rows))
	for i := range rows {
		row := rows[i]
		out = append(out, attemptDoc{
			At:           row.At,
			Error:        row.Error,
			Outcome:      string(row.Outcome),
			FailureClass: string(row.FailureClass),
			Cycle:        row.Cycle,
			Number:       row.Number,
			DurationMS:   row.DurationMS,
			StatusCode:   row.StatusCode,
		})
	}

	return out
}
