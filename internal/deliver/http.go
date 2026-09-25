package deliver

import "context"

type HTTPRequest struct {
	Payload        []byte
	Target, JobID  string
	Cycle, Attempt int
}

type HTTPResult struct{ StatusCode, BytesRead int }

type HTTP interface {
	Post(ctx context.Context, req HTTPRequest) (HTTPResult, error)
}
