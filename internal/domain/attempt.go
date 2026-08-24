package domain

import (
	"time"
	"unicode/utf8"
)

type Attempt struct {
	At           time.Time
	Error        string
	Outcome      Outcome
	FailureClass FailureClass
	Cycle        int
	Number       int
	DurationMS   int
	StatusCode   int
}

const maxErrorRunes = 1024

func (a *Attempt) Validate() error {
	if a == nil {
		return ErrInvalidAttempt
	}

	return a.validateForAppend()
}

func (a *Attempt) validateForAppend() error {
	if a.DurationMS < 0 {
		return ErrInvalidAttempt
	}

	switch a.Outcome {
	case OutcomeSuccess:
		if a.FailureClass != "" || a.StatusCode == 0 {
			return ErrInvalidAttempt
		}
	case OutcomeFailure:
		if a.FailureClass == "" || a.Error == "" {
			return ErrInvalidAttempt
		}
	default:
		return ErrInvalidAttempt
	}

	return nil
}

func truncateRunes(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}

	runes := []rune(s)

	return string(runes[:maxRunes])
}
