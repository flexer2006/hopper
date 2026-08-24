package domain

import "strconv"

const outboundKeyPrefix = "hopper/"

func OutboundIdempotencyKey(jobID string, cycle, attempt int) string {
	return outboundKeyPrefix + jobID + "/" + strconv.Itoa(cycle) + "/" + strconv.Itoa(attempt)
}
