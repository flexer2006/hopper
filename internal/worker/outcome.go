package worker

import (
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

func successIn(out *deliver.ClaimOut, row *domain.Attempt, job *domain.Job) deliver.OutcomeIn {
	return deliver.OutcomeIn{
		Attempts:     []domain.Attempt{*row},
		ID:           out.ID,
		FenceToken:   out.FenceToken,
		Queue:        "",
		Status:       domain.StatusSucceeded,
		DelaySeconds: 0,
		AttemptsDone: job.AttemptsDone,
		Cycle:        out.Cycle,
	}
}

func failIn(out *deliver.ClaimOut, row *domain.Attempt, route domain.Route, job *domain.Job) deliver.OutcomeIn {
	return deliver.OutcomeIn{
		Attempts:     []domain.Attempt{*row},
		ID:           out.ID,
		FenceToken:   out.FenceToken,
		Queue:        route.Queue,
		Status:       route.Status,
		DelaySeconds: route.DelaySeconds,
		AttemptsDone: job.AttemptsDone,
		Cycle:        out.Cycle,
	}
}
