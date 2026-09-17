package worker //nolint:testpackage // parseJobID

import (
	"testing"
)

func BenchmarkParseJobID(b *testing.B) {
	body := []byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`)
	b.ReportAllocs()
	for b.Loop() {
		id, err := parseJobID(body)
		if err != nil || id == "" {
			b.Fatal(err)
		}
	}
}
