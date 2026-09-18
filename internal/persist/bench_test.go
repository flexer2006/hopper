package persist_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/persist"
)

func BenchmarkInsert(b *testing.B) {
	st := persist.NewMemory(func() time.Time {
		return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	}, time.Second)
	payload := []byte(`{"pad":"` + repeatA(503) + `"}`)
	ctx := b.Context()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		err := st.Insert(ctx, enqueue.Record{
			Payload:     payload,
			ID:          fmt.Sprintf("%024x", i),
			Target:      "https://example.invalid/hop16",
			ProducerKey: fmt.Sprintf("k-%d", i),
			RequestHash: fmt.Sprintf("%064x", i),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertThenClaim(b *testing.B) {
	st := persist.NewMemory(time.Now, 30*time.Second)
	payload := []byte(`{"pad":"` + repeatA(503) + `"}`)
	ctx := b.Context()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		id := fmt.Sprintf("%024x", i)
		err := st.Insert(ctx, enqueue.Record{
			Payload:     payload,
			ID:          id,
			Target:      "https://example.invalid/hop16",
			ProducerKey: fmt.Sprintf("k-%d", i),
			RequestHash: fmt.Sprintf("%064x", i),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		_, err = st.Claim(ctx, deliver.ClaimIn{ID: id, WorkerID: "w1"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestHOP16DumpProfiles(t *testing.T) {
	t.Parallel()

	if os.Getenv("HOP16_PPROF") != "1" {
		t.Skip("HOP16_PPROF not set")
	}

	dir := os.Getenv("HOP16_PPROF_DIR")
	if dir == "" {
		t.Fatal("HOP16_PPROF_DIR required")
	}

	st := persist.NewMemory(time.Now, 30*time.Second)
	payload := []byte(`{"pad":"` + repeatA(503) + `"}`)
	ctx := t.Context()
	const n = 256
	for i := range n {
		id := fmt.Sprintf("%024x", i+1)
		err := st.Insert(ctx, enqueue.Record{
			Payload:     payload,
			ID:          id,
			Target:      "https://example.invalid/hop16",
			ProducerKey: fmt.Sprintf("k-%d", i+1),
			RequestHash: fmt.Sprintf("%064x", i+1),
			Type:        domain.TypeHTTPPost,
			MaxAttempts: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = st.Claim(ctx, deliver.ClaimIn{ID: id, WorkerID: "w1"})
		if err != nil {
			t.Fatal(err)
		}
	}

	writeLookup(t, dir, "goroutine-before-gc.pprof", "goroutine")
	writeLookup(t, dir, "goroutineleak.pprof", "goroutineleak")
	writeLookup(t, dir, "heap-before-gc.pprof", "heap")
	runtime.GC()
	writeLookup(t, dir, "heap-after-gc.pprof", "heap")
	writeLookup(t, dir, "allocs.pprof", "allocs")
}

func writeLookup(t *testing.T, dir, name, kind string) {
	t.Helper()

	prof := pprof.Lookup(kind)
	if prof == nil {
		t.Fatalf("pprof profile %q missing", kind)
	}

	if filepath.Base(name) != name || strings.Contains(name, "..") {
		t.Fatalf("invalid profile name %q", name)
	}

	path := filepath.Join(dir, name)
	file, err := os.Create(path) //nolint:gosec // G703: name is a fixed basename; dir is operator HOP16_PPROF_DIR.
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			t.Errorf("close %s: %v", name, closeErr)
		}
	}()

	err = prof.WriteTo(file, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func repeatA(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'a'
	}

	return string(buf)
}
