package deliver

import "context"

type HTTPRequest struct {
	Payload []byte
	Target  string
	JobID   string
	Cycle   int
	Attempt int
}

type HTTPResult struct {
	StatusCode int
	BytesRead  int
}

type HTTP interface {
	Post(ctx context.Context, req HTTPRequest) (HTTPResult, error)
}
