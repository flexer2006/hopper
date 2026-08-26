package worker

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

type Source interface {
	Next(ctx context.Context) (Delivery, error)
}

func (w *Worker) Run(ctx context.Context, src Source) error {
	err := w.consume(ctx, src)
	if err != nil {
		return fmt.Errorf("worker consume: %w", err)
	}

	return nil
}

func (w *Worker) consume(ctx context.Context, src Source) error {
	for {
		msg, err := src.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			return err
		}

		procCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.budget)
		procErr := w.Process(procCtx, msg)

		cancel()

		if procErr != nil && ctx.Err() == nil {
			w.log.Warn("process", zap.Error(procErr))
		}
	}
}
