package persist

import "time"

func CheckLeaseBudget(lease, httpTimeout, outcomeTimeout, confirmTimeout time.Duration) error {
	need := httpTimeout + outcomeTimeout + confirmTimeout + leaseMargin
	if lease < need {
		return ErrLeaseBudget
	}

	return nil
}
