package domain

import "strconv"

const (
	QueueJobs = "jobs"
	QueueDLQ  = "jobs.dlq"

	backoffShiftCap   = 6
	backoffCapSeconds = 60
	delayQueuePrefix  = "jobs.delay."
	delayQueueSuffix  = "s"
	minDelaySeconds   = 1
)

func BackoffSeconds(attemptsDone int) (int, error) {
	if attemptsDone < 0 {
		return 0, ErrInvalidBackoff
	}

	if attemptsDone >= backoffShiftCap {
		return backoffCapSeconds, nil
	}

	return minDelaySeconds << attemptsDone, nil
}

func DelayQueue(seconds int) (string, error) {
	if !isClassicDelay(seconds) {
		return "", ErrInvalidBackoff
	}

	return delayQueuePrefix + strconv.Itoa(seconds) + delayQueueSuffix, nil
}

func isClassicDelay(seconds int) bool {
	if seconds == backoffCapSeconds {
		return true
	}

	maxPower := minDelaySeconds << (backoffShiftCap - 1)
	if seconds < minDelaySeconds || seconds > maxPower {
		return false
	}

	return seconds&(seconds-1) == 0
}
