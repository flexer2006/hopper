package domain

import (
	"fmt"
	"strconv"
)

func (j *Job) RecordSuccess(row *Attempt) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: success from %s", ErrIllegalTransition, j.Status)
	}

	if row == nil {
		return ErrInvalidAttempt
	}

	outcome, _, classErr := ClassifyHTTP(row.StatusCode)
	if classErr != nil {
		return classErr
	}

	if outcome != OutcomeSuccess {
		return ErrInvalidAttempt
	}

	row.Outcome = OutcomeSuccess
	row.FailureClass = ""
	row.Error = ""

	prepared, prepErr := j.prepareRow(row)
	if prepErr != nil {
		return prepErr
	}

	j.Attempts = append(j.Attempts, prepared)
	j.AttemptsDone++
	j.Status = StatusSucceeded

	return nil
}

func (j *Job) RecordFailure(row *Attempt) (Route, error) {
	if j.Status != StatusRunning {
		return Route{}, fmt.Errorf("%w: failure from %s", ErrIllegalTransition, j.Status)
	}

	if row == nil {
		return Route{}, ErrInvalidAttempt
	}

	if row.Outcome != OutcomeFailure && row.Outcome != "" {
		return Route{}, ErrInvalidAttempt
	}

	classErr := applyFailureClass(row)
	if classErr != nil {
		return Route{}, classErr
	}

	row.Outcome = OutcomeFailure
	row.Error = truncateRunes(row.Error, maxErrorRunes)

	prepared, prepErr := j.prepareRow(row)
	if prepErr != nil {
		return Route{}, prepErr
	}

	attemptsBefore := j.AttemptsDone

	route, routeErr := previewRoute(prepared.FailureClass, attemptsBefore, attemptsBefore+1, j.MaxAttempts)
	if routeErr != nil {
		return Route{}, routeErr
	}

	j.Attempts = append(j.Attempts, prepared)
	j.AttemptsDone++
	j.Status = route.Status

	return route, nil
}

func applyFailureClass(row *Attempt) error {
	if row.StatusCode == 0 {
		return nil
	}

	outcome, class, classErr := ClassifyHTTP(row.StatusCode)
	if classErr != nil {
		return classErr
	}

	if outcome != OutcomeFailure {
		return ErrInvalidAttempt
	}

	row.FailureClass = class

	return nil
}

func (j *Job) prepareRow(row *Attempt) (Attempt, error) {
	row.Cycle = j.Cycle
	row.Number = j.AttemptNumber()

	valErr := row.validateForAppend()
	if valErr != nil {
		return Attempt{}, valErr
	}

	return *row, nil
}

func previewRoute(class FailureClass, attemptsBefore, attemptsAfter, maxAttempts int) (Route, error) {
	switch class {
	case ClassTerminalHTTP, ClassNonRetryableLocal:
		return Route{Queue: QueueDLQ, Status: StatusDead, DelaySeconds: 0}, nil
	case ClassRetryable:
		if attemptsAfter >= maxAttempts {
			return Route{Queue: QueueDLQ, Status: StatusDead, DelaySeconds: 0}, nil
		}

		seconds, backErr := BackoffSeconds(attemptsBefore)
		if backErr != nil {
			return Route{}, backErr
		}

		queue := delayQueuePrefix + strconv.Itoa(seconds) + delayQueueSuffix

		return Route{Queue: queue, Status: StatusQueued, DelaySeconds: seconds}, nil
	default:
		return Route{}, ErrInvalidAttempt
	}
}
