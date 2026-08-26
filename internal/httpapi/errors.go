package httpapi

import (
	"encoding/json"
	"net/http"
)

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
	ID    string `json:"id,omitempty"`
}

const (
	codeValidation         = "validation_failed"
	codeUnauthorized       = "unauthorized"
	codeNotFound           = "not_found"
	codeConflict           = "conflict"
	codePayloadTooLarge    = "payload_too_large"
	codeServiceUnavailable = "service_unavailable"
	codeRateLimited        = "rate_limited"
	codeIdempotency        = "idempotency_conflict"
	contentTypeJSON        = "application/json"
)

func writeErr(w http.ResponseWriter, status int, msg, code string) {
	writeErrID(w, status, msg, code, "")
}

func writeErrID(w http.ResponseWriter, status int, msg, code, id string) {
	writeJSON(w, status, errorBody{Error: msg, Code: code, ID: id})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	encErr := enc.Encode(body)
	if encErr != nil {
		return
	}
}
