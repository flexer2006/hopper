package worker_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/worker"
)

type seqHTTP struct {
	codes []int
	n     atomic.Int32
}

func (s *seqHTTP) Post(context.Context, deliver.HTTPRequest) (deliver.HTTPResult, error) {
	i := int(s.n.Add(1) - 1)
	if i >= len(s.codes) {
		i = len(s.codes) - 1
	}

	return deliver.HTTPResult{StatusCode: s.codes[i]}, nil
}

func BenchmarkProcessSuccess(b *testing.B) {
	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := worker.New(st, httpStub, &stubAux{}, &stubRelay{}, nil, worker.Config{
		Now:      clk.now,
		WorkerID: "bench",
	})
	ctx := b.Context()
	payload := []byte(`{"n":1}`)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		b.StopTimer()
		i++
		id := fmt.Sprintf("%024x", i)
		err := st.Insert(ctx, enqueue.Record{
			Payload:     payload,
			ID:          id,
			Target:      testTarget,
			ProducerKey: fmt.Sprintf("k-%d", i),
			RequestHash: fmt.Sprintf("%064x", i),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		body, err := broker.MarshalEnqueue(id)
		if err != nil {
			b.Fatal(err)
		}
		deliv := &memDelivery{body: body}
		b.StartTimer()
		err = wkr.Process(ctx, deliv)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestHOP16W2DelayBuckets(t *testing.T) {
	t.Parallel()

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	httpStub := &seqHTTP{codes: []int{http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK}}
	wkr := worker.New(st, httpStub, &stubAux{}, &stubRelay{}, nil, worker.Config{
		Now:      clk.now,
		WorkerID: "w2",
	})
	err := st.Insert(t.Context(), enqueue.Record{
		Payload:     []byte(`{"n":1}`),
		ID:          testJobID,
		Target:      testTarget,
		ProducerKey: "hop16-w2",
		RequestHash: testHash,
		Type:        domain.TypeHTTPPost,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := broker.MarshalEnqueue(testJobID)
	if err != nil {
		t.Fatal(err)
	}

	err = wkr.Process(t.Context(), &memDelivery{body: body})
	if err != nil {
		t.Fatal(err)
	}

	pending, err := st.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Queue != "jobs.delay.1s" {
		t.Fatalf("after 500 #1 pending=%+v err=%v", pending, err)
	}

	clk.add(time.Second)
	err = wkr.Process(t.Context(), &memDelivery{body: body})
	if err != nil {
		t.Fatal(err)
	}

	pending, err = st.ListPending(t.Context(), 8)
	if err != nil || len(pending) != 1 || pending[0].Queue != "jobs.delay.2s" {
		t.Fatalf("after 500 #2 pending=%+v err=%v", pending, err)
	}

	clk.add(2 * time.Second)
	err = wkr.Process(t.Context(), &memDelivery{body: body})
	if err != nil {
		t.Fatal(err)
	}

	got := getJob(t, st)
	if got.Status != domain.StatusSucceeded || httpStub.n.Load() != 3 {
		t.Fatalf("status=%s posts=%d", got.Status, httpStub.n.Load())
	}
}

func TestHOP16W3RSS(t *testing.T) { //nolint:paralleltest // process-wide VmRSS; must not share the binary
	if os.Getenv("HOP16_W3") != "1" {
		t.Skip("HOP16_W3 not set")
	}

	clk := newClock()
	st := persist.NewMemory(clk.now, 30*time.Second)
	httpStub := &stubHTTP{code: http.StatusOK}
	wkr := worker.New(st, httpStub, &stubAux{}, &stubRelay{}, nil, worker.Config{
		Now:      clk.now,
		WorkerID: "w3",
	})
	ctx := t.Context()
	payload := []byte(`{"pad":"` + strings.Repeat("b", 2048-10) + `"}`)
	const jobs = 10000
	rss := make([]int64, 0, 11)
	rss = append(rss, readRSS(t))
	for i := range jobs {
		id := fmt.Sprintf("%024x", i+1)
		err := st.Insert(ctx, enqueue.Record{
			Payload:     payload,
			ID:          id,
			Target:      testTarget,
			ProducerKey: fmt.Sprintf("w3-%d", i+1),
			RequestHash: fmt.Sprintf("%064x", i+1),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		body, err := broker.MarshalEnqueue(id)
		if err != nil {
			t.Fatal(err)
		}
		err = wkr.Process(ctx, &memDelivery{body: body})
		if err != nil {
			t.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			runtime.GC()
			rss = append(rss, readRSS(t))
		}
	}

	runtime.GC()
	final := readRSS(t)
	rss = append(rss, final)
	t.Logf("w3 rss_kib=%v final_kib=%d jobs=%d posts=%d", rss, final, jobs, httpStub.n.Load())
	if httpStub.n.Load() != jobs {
		t.Fatalf("posts=%d want %d", httpStub.n.Load(), jobs)
	}
}

func readRSS(t *testing.T) int64 {
	t.Helper()

	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}

	for line := range strings.SplitSeq(string(raw), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("rss line %q", line)
		}
		kib, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}

		return kib
	}

	t.Fatal("VmRSS missing")

	return 0
}
