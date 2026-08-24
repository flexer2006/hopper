package replay

import "context"

type Request struct {
	ID string
	By string
}

type Store interface {
	Replay(ctx context.Context, rec Request) error
}
