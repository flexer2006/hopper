package domain

import "net/http"

type Outcome string

type FailureClass string

type LocalKind uint8

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
)

const (
	ClassRetryable         FailureClass = "retryable"
	ClassTerminalHTTP      FailureClass = "terminal_http"
	ClassNonRetryableLocal FailureClass = "non_retryable_local"
)

const (
	LocalUnspecified LocalKind = iota
	LocalTransport
	LocalDNSTimeout
	LocalSERVFAIL
	LocalNXDOMAIN
	LocalSSRF
	LocalInvalidURL
	LocalMalformedBody
)

const maxHTTPStatusCode = 599

func ClassifyHTTP(code int) (Outcome, FailureClass, error) {
	if code < http.StatusContinue || code > maxHTTPStatusCode {
		return "", "", ErrInvalidHTTPStatus
	}

	switch {
	case code >= http.StatusOK && code < http.StatusMultipleChoices:
		return OutcomeSuccess, "", nil
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return OutcomeFailure, ClassRetryable, nil
	case code >= http.StatusInternalServerError:
		return OutcomeFailure, ClassRetryable, nil
	default:
		return OutcomeFailure, ClassTerminalHTTP, nil
	}
}

func ClassifyLocal(kind LocalKind) (FailureClass, error) {
	switch kind {
	case LocalTransport, LocalDNSTimeout, LocalSERVFAIL:
		return ClassRetryable, nil
	case LocalNXDOMAIN, LocalSSRF, LocalInvalidURL, LocalMalformedBody:
		return ClassNonRetryableLocal, nil
	case LocalUnspecified:
		return "", ErrInvalidLocalKind
	default:
		return "", ErrInvalidLocalKind
	}
}
