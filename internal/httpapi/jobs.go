package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/replay"
)

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "missing Idempotency-Key", codeValidation)

		return
	}

	if len(key) > maxIdempotencyKey {
		writeErr(w, http.StatusBadRequest, "invalid Idempotency-Key", codeValidation)

		return
	}

	raw, rec, err := h.parseCreate(r)
	if err != nil {
		h.writeParseErr(w, err)

		return
	}

	if h.enqueue == nil {
		writeErr(w, http.StatusServiceUnavailable, "enqueue unavailable", codeServiceUnavailable)

		return
	}

	rec.ProducerKey = key
	rec.RequestHash = requestHash(raw)

	out, err := h.enqueue.Enqueue(r.Context(), rec)
	if err != nil {
		h.writeEnqueueErr(w, err)

		return
	}

	if !out.Accepted {
		writeErrID(w, http.StatusServiceUnavailable, "confirm failed", codeServiceUnavailable, out.ID)

		return
	}

	h.log.Info("job accepted", zapJobID(out.ID))
	writeJSON(w, http.StatusAccepted, idBody{ID: out.ID})
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get(listStatusKey) != statusDeadQ {
		writeErr(w, http.StatusBadRequest, "status must be dead", codeValidation)

		return
	}

	if h.query == nil {
		writeErr(w, http.StatusServiceUnavailable, "query unavailable", codeServiceUnavailable)

		return
	}

	jobs, err := h.query.ListDead(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "query failed", codeServiceUnavailable)

		return
	}

	items := make([]jobSummaryDTO, 0, len(jobs))
	for i := range jobs {
		items = append(items, publicSummary(jobs[i]))
	}

	writeJSON(w, http.StatusOK, listBody{Items: items})
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !validJobID(id) {
		writeErr(w, http.StatusBadRequest, "invalid job id", codeValidation)

		return
	}

	if h.query == nil {
		writeErr(w, http.StatusServiceUnavailable, "query unavailable", codeServiceUnavailable)

		return
	}

	job, err := h.query.Get(r.Context(), id)
	if err != nil {
		h.writeQueryErr(w, err)

		return
	}

	writeJSON(w, http.StatusOK, publicJob(job))
}

func (h *Handler) replayJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !validJobID(id) {
		writeErr(w, http.StatusBadRequest, "invalid job id", codeValidation)

		return
	}

	if h.replay == nil {
		writeErr(w, http.StatusServiceUnavailable, "replay unavailable", codeServiceUnavailable)

		return
	}

	out, err := h.replay.Replay(r.Context(), replay.Request{ID: id, By: ""})
	if err != nil {
		h.writeReplayErr(w, err)

		return
	}

	if !out.Accepted {
		writeErrID(w, http.StatusServiceUnavailable, "confirm failed", codeServiceUnavailable, out.ID)

		return
	}

	h.log.Info("job replayed", zapJobID(out.ID))
	writeJSON(w, http.StatusAccepted, replayBody{
		ID:     out.ID,
		Status: string(out.Status),
		Cycle:  out.Cycle,
	})
}

func (h *Handler) parseCreate(r *http.Request) ([]byte, enqueue.Record, error) {
	raw, err := readCapped(r.Body, h.maxBody)
	if err != nil {
		return nil, enqueue.Record{}, err
	}

	if jsonTooDeep(raw, h.maxDepth) {
		return nil, enqueue.Record{}, errJSONDepth
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var req createRequest

	err = dec.Decode(&req)
	if err != nil {
		return nil, enqueue.Record{}, errJSONSyntax
	}

	if dec.More() {
		return nil, enqueue.Record{}, errJSONSyntax
	}

	payload, err := boundPayload(req.Payload, h.maxPayload)
	if err != nil {
		return nil, enqueue.Record{}, err
	}

	if req.Type != string(domain.TypeHTTPPost) {
		return nil, enqueue.Record{}, errJSONSyntax
	}

	return raw, enqueue.Record{
		Payload:     payload,
		ID:          "",
		Target:      req.Target,
		ProducerKey: "",
		RequestHash: "",
		Type:        domain.JobType(req.Type),
		MaxAttempts: req.MaxAttempts,
	}, nil
}

func (h *Handler) writeParseErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errBodyTooLarge), errors.Is(err, errPayloadTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "payload too large", codePayloadTooLarge)
	default:
		writeErr(w, http.StatusBadRequest, "invalid request", codeValidation)
	}
}

func (h *Handler) writeEnqueueErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, enqueue.ErrIdempotencyConflict):
		writeErr(w, http.StatusConflict, "idempotency conflict", codeIdempotency)
	case errors.Is(err, enqueue.ErrTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "payload too large", codePayloadTooLarge)
	case errors.Is(err, enqueue.ErrInvalid), errors.Is(err, domain.ErrInvalidTarget),
		errors.Is(err, domain.ErrInvalidType), errors.Is(err, domain.ErrInvalidMaxAttempts):
		writeErr(w, http.StatusBadRequest, "invalid request", codeValidation)
	default:
		writeErr(w, http.StatusServiceUnavailable, "enqueue failed", codeServiceUnavailable)
	}
}

func (h *Handler) writeQueryErr(w http.ResponseWriter, err error) {
	if errors.Is(err, domain.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found", codeNotFound)

		return
	}

	writeErr(w, http.StatusServiceUnavailable, "query failed", codeServiceUnavailable)
}

func (h *Handler) writeReplayErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found", codeNotFound)
	case errors.Is(err, domain.ErrReplayNotDead), errors.Is(err, domain.ErrReplayCap),
		errors.Is(err, replay.ErrInvalid):
		writeErr(w, http.StatusConflict, "conflict", codeConflict)
	default:
		writeErr(w, http.StatusServiceUnavailable, "replay failed", codeServiceUnavailable)
	}
}

func validJobID(id string) bool {
	if len(id) != jobIDLen {
		return false
	}

	for i := range len(id) {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}
