package enqueue

import (
	"context"

	"github.com/flexer2006/hopper/internal/domain"
)

type Record struct {
	Payload     []byte
	ID          string
	Target      string
	ProducerKey string
	RequestHash string
	Type        domain.JobType
	MaxAttempts int
}

type Existing struct {
	ID             string
	RequestHash    string
	DispatchStatus string
}

type Store interface {
	Insert(ctx context.Context, rec Record) error
	ByProducerKey(ctx context.Context, key string) (Existing, error)
}
