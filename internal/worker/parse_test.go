package worker_test

import (
	"net/http"
	"testing"
)

func TestMalformedUppercaseID(t *testing.T) {
	t.Parallel()

	aux := &stubAux{}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, aux, &stubRelay{}, newClock(), "w1")
	deliv := &memDelivery{body: []byte(`{"job_id":"AAAAAAAAAAAAAAAAAAAAAAAA"}`)}

	err := wkr.Process(t.Context(), deliv)
	if err != nil || !deliv.acked.Load() || len(aux.bodies) != 1 {
		t.Fatalf("uppercase id must be malformed")
	}
}

func TestMalformedDuplicateJobIDField(t *testing.T) {
	t.Parallel()

	aux := &stubAux{}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, aux, &stubRelay{}, newClock(), "w1")
	deliv := &memDelivery{body: []byte(`{"job_id":"0123456789abcdef01234567","job_id":"abcdef0123456789abcdef01"}`)}

	err := wkr.Process(t.Context(), deliv)
	if err != nil || !deliv.acked.Load() || len(aux.bodies) != 1 {
		t.Fatalf("duplicate job_id member must be rejected as malformed")
	}
}

func TestMalformedInvalidUTF8(t *testing.T) {
	t.Parallel()

	aux := &stubAux{}
	wkr := newWorker(t, newStore(newClock()), &stubHTTP{code: http.StatusOK}, aux, &stubRelay{}, newClock(), "w1")
	deliv := &memDelivery{body: []byte("{\"job_id\":\"\xff\"}")}

	err := wkr.Process(t.Context(), deliv)
	if err != nil || !deliv.acked.Load() || len(aux.bodies) != 1 {
		t.Fatalf("invalid utf-8 must be rejected as malformed")
	}
}
