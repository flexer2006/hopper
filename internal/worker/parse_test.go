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
