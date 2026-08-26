package egress_test

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/egress"
)

const (
	testJobID   = "aaaaaaaaaaaaaaaaaaaaaaaa"
	fixtureHost = "example.com"
	publicIP    = "8.8.8.8"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func testReq(target string) deliver.HTTPRequest {
	return deliver.HTTPRequest{
		Payload: []byte(`{}`),
		Target:  target,
		JobID:   testJobID,
		Cycle:   0,
		Attempt: 1,
	}
}

func fixtureAddr() netip.Addr {
	return netip.MustParseAddr(publicIP)
}

func TestDeniedPrefixes(t *testing.T) {
	t.Parallel()

	blocked := []string{
		"127.0.0.1",
		"::1",
		"10.0.0.1",
		"172.16.0.1",
		"192.168.1.1",
		"169.254.169.254",
		"100.64.0.1",
		"198.18.0.1",
		"192.0.2.1",
		"198.51.100.1",
		"203.0.113.1",
		"0.1.2.3",
		"192.0.0.1",
		"224.0.0.1",
		"240.0.0.1",
		"255.255.255.255",
		"::",
		"::ffff:127.0.0.1",
		"::ffff:169.254.169.254",
		"::ffff:100.64.0.1",
		"2001:db8::1",
		"2001:2::1",
		"fc00::1",
		"fe80::1",
		"fe80::1%eth0",
		"64:ff9b::1",
		"64:ff9b:1::1",
		"ff01::1",
		"ff02::1",
	}

	for _, s := range blocked {
		if !egress.Denied(netip.MustParseAddr(s)) {
			t.Fatalf("Denied(%s) = false", s)
		}
	}

	if egress.Denied(fixtureAddr()) {
		t.Fatal("8.8.8.8 denied")
	}

	if egress.Denied(netip.MustParseAddr("::ffff:8.8.8.8")) {
		t.Fatal("mapped public 8.8.8.8 denied after Unmap")
	}

	if !egress.Denied(netip.Addr{}) {
		t.Fatal("zero Addr allowed")
	}
}

func TestPostBlocksLoopbackWithoutDial(t *testing.T) {
	t.Parallel()

	var dials atomic.Int32
	c := blockedDialHarness(&dials)

	_, err := c.Post(t.Context(), testReq("http://127.0.0.1/"))
	if !errors.Is(err, egress.ErrBlocked) {
		t.Fatalf("err = %v, want blocked", err)
	}

	if dials.Load() != 0 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostBlocksMetadataWithoutDial(t *testing.T) {
	t.Parallel()

	var dials atomic.Int32
	c := blockedDialHarness(&dials)

	_, err := c.Post(t.Context(), testReq("http://169.254.169.254/latest/meta-data"))
	if !errors.Is(err, egress.ErrBlocked) {
		t.Fatalf("err = %v, want blocked", err)
	}

	if dials.Load() != 0 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostBlocksMappedLoopback(t *testing.T) {
	t.Parallel()

	_, err := egress.New().Post(t.Context(), testReq("http://[::ffff:127.0.0.1]/"))
	if !errors.Is(err, egress.ErrBlocked) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostBlocksCGNATAndBenchmark(t *testing.T) {
	t.Parallel()

	c := egress.New()
	for _, raw := range []string{"http://100.64.0.1/", "http://198.18.1.1/"} {
		_, err := c.Post(t.Context(), testReq(raw))
		if !errors.Is(err, egress.ErrBlocked) {
			t.Fatalf("%s err = %v", raw, err)
		}
	}
}

func TestPostRejectsUserinfoAndScheme(t *testing.T) {
	t.Parallel()

	c := egress.New()
	_, err := c.Post(t.Context(), testReq("https://user@example.com/"))
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("userinfo err = %v", err)
	}

	_, err = c.Post(t.Context(), testReq("ftp://example.com/"))
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("ftp err = %v", err)
	}
}

func TestPostInvalidRequest(t *testing.T) {
	t.Parallel()

	c := egress.New()
	_, err := c.Post(t.Context(), deliver.HTTPRequest{Target: "https://example.com/", Attempt: 1})
	if !errors.Is(err, egress.ErrInvalidRequest) {
		t.Fatalf("err = %v", err)
	}

	_, err = c.Post(t.Context(), deliver.HTTPRequest{
		Target:  "https://example.com/",
		JobID:   testJobID,
		Cycle:   -1,
		Attempt: 1,
	})
	if !errors.Is(err, egress.ErrInvalidRequest) {
		t.Fatalf("negative cycle err = %v", err)
	}
}

func TestPostInvalidPort(t *testing.T) {
	t.Parallel()

	_, err := egress.New().Post(t.Context(), testReq("http://8.8.8.8:70000/"))
	if !errors.Is(err, egress.ErrInvalidRequest) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostMixedDNSRejected(t *testing.T) {
	t.Parallel()

	var dials atomic.Int32
	c := egress.NewHarness(func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{fixtureAddr(), netip.MustParseAddr("127.0.0.1")}, nil
	}, countingDial(&dials), nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, egress.ErrBlocked) {
		t.Fatalf("err = %v", err)
	}

	if dials.Load() != 0 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostEmptyDNSRejected(t *testing.T) {
	t.Parallel()

	c := egress.NewHarness(func(context.Context, string) ([]netip.Addr, error) {
		return nil, nil
	}, nil, nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, egress.ErrEmptyResolution) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostLookupError(t *testing.T) {
	t.Parallel()

	want := errors.New("nxdomain")
	c := egress.NewHarness(func(context.Context, string) ([]netip.Addr, error) {
		return nil, want
	}, nil, nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostLookupDNSErrorOmitsName(t *testing.T) {
	t.Parallel()

	c := egress.NewHarness(func(context.Context, string) ([]netip.Addr, error) {
		return nil, &net.DNSError{
			Err:        "no such host",
			Name:       fixtureHost,
			Server:     "8.8.8.8:53",
			IsNotFound: true,
		}
	}, nil, nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if err == nil {
		t.Fatal("expected lookup error")
	}

	msg := err.Error()
	if strings.Contains(msg, fixtureHost) || strings.Contains(msg, "8.8.8.8:53") {
		t.Fatalf("lookup error embeds name: %q", msg)
	}

	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || !dnsErr.IsNotFound {
		t.Fatalf("want DNSError IsNotFound, got %v", err)
	}
}

func TestPostResolveContextHasDeadline(t *testing.T) {
	t.Parallel()

	var sawDeadline bool
	c := egress.NewHarness(func(ctx context.Context, _ string) ([]netip.Addr, error) {
		_, sawDeadline = ctx.Deadline()

		return nil, errors.New("stop")
	}, nil, nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if err == nil {
		t.Fatal("expected lookup stop")
	}

	if !sawDeadline {
		t.Fatal("resolve context has no deadline")
	}
}

func TestPostDialError(t *testing.T) {
	t.Parallel()

	want := errors.New("dial refused")
	c := egress.NewHarness(func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{fixtureAddr()}, nil
	}, func(context.Context, string, string) (net.Conn, error) {
		return nil, want
	}, nil)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}

	msg := err.Error()
	if strings.Contains(msg, fixtureHost) || strings.Contains(msg, "http://") {
		t.Fatalf("transport error embeds url: %q", msg)
	}
}

func TestPostSuccessPinsAndHeaders(t *testing.T) {
	t.Parallel()

	var gotHost, gotKey, gotCT string
	var dials atomic.Int32

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotKey = r.Header.Get("Idempotency-Key")
		gotCT = r.Header.Get("Content-Type")

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)

			return
		}

		if string(body) != `{"ok":true}` {
			t.Errorf("body = %s", body)
		}

		w.WriteHeader(http.StatusCreated)

		if _, err := w.Write([]byte("ok")); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	srv.Start()
	t.Cleanup(srv.Close)

	ip := fixtureAddr()
	c := egress.NewHarness(
		func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{ip}, nil
		},
		pinDialTo(t, srv, ip, "80", &dials),
		nil,
	)

	res, err := c.Post(t.Context(), deliver.HTTPRequest{
		Payload: []byte(`{"ok":true}`),
		Target:  "http://" + fixtureHost + "/hook",
		JobID:   testJobID,
		Cycle:   2,
		Attempt: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusCreated || res.BytesRead != 2 {
		t.Fatalf("result = %+v", res)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d", dials.Load())
	}

	if gotHost != fixtureHost {
		t.Fatalf("Host = %q", gotHost)
	}

	if gotKey != domain.OutboundIdempotencyKey(testJobID, 2, 3) {
		t.Fatalf("Idempotency-Key = %q", gotKey)
	}

	if gotCT != "application/json" {
		t.Fatalf("Content-Type = %q", gotCT)
	}
}

func TestPostPublicIPLiteralSkipsLookup(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	var dials atomic.Int32
	ip := fixtureAddr()
	c := egress.NewHarness(
		func(context.Context, string) ([]netip.Addr, error) {
			t.Fatal("lookup must not run for IP literal")

			return nil, errors.New("lookup")
		},
		pinDialTo(t, srv, ip, "80", &dials),
		nil,
	)

	res, err := c.Post(t.Context(), testReq("http://"+publicIP+"/hook"))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", res.StatusCode)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostZeroRedirects(t *testing.T) {
	t.Parallel()

	var dials atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://169.254.169.254/")
		w.WriteHeader(http.StatusFound)

		if _, err := w.Write([]byte("no")); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	c := pinClient(t, srv, fixtureAddr(), &dials)

	res, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusFound {
		t.Fatalf("status = %d", res.StatusCode)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d, redirect followed?", dials.Load())
	}
}

func TestPostBodyLimit(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)

		if _, err := w.Write([]byte(strings.Repeat("a", 1<<20+1))); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	var dials atomic.Int32
	c := pinClient(t, srv, fixtureAddr(), &dials)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, egress.ErrBodyLimit) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostBodyLimitOnRedirect(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://169.254.169.254/")
		w.WriteHeader(http.StatusFound)

		if _, err := w.Write([]byte(strings.Repeat("b", 1<<20+1))); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	var dials atomic.Int32
	c := pinClient(t, srv, fixtureAddr(), &dials)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if !errors.Is(err, egress.ErrBodyLimit) {
		t.Fatalf("err = %v", err)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostNoCrossHostReuse(t *testing.T) {
	t.Parallel()

	var dials atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := pinClient(t, srv, fixtureAddr(), &dials)

	for range 2 {
		_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
		if err != nil {
			t.Fatal(err)
		}
	}

	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2 isolated transports", dials.Load())
	}
}

func TestPostTLSSNI(t *testing.T) {
	t.Parallel()

	var sawName string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil {
			sawName = r.TLS.ServerName
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	srv.StartTLS()
	t.Cleanup(srv.Close)

	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())

	ip := fixtureAddr()
	var dials atomic.Int32
	c := egress.NewHarness(
		func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{ip}, nil
		},
		pinDialTo(t, srv, ip, "443", &dials),
		roots,
	)

	_, err := c.Post(t.Context(), testReq("https://"+fixtureHost+"/"))
	if err != nil {
		t.Fatal(err)
	}

	if sawName != fixtureHost {
		t.Fatalf("SNI = %q", sawName)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostHTTP1Only(t *testing.T) {
	t.Parallel()

	var proto string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto = r.Proto
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	var dials atomic.Int32
	c := pinClient(t, srv, fixtureAddr(), &dials)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if err != nil {
		t.Fatal(err)
	}

	if proto != "HTTP/1.0" && proto != "HTTP/1.1" {
		t.Fatalf("proto = %q, HTTP/2 enabled?", proto)
	}
}

func TestPostDoesNotUseHTTPProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9")
	t.Setenv("http_proxy", "http://127.0.0.1:9")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	var dials atomic.Int32
	c := pinClient(t, srv, fixtureAddr(), &dials)

	_, err := c.Post(t.Context(), testReq("http://"+fixtureHost+"/"))
	if err != nil {
		t.Fatal(err)
	}

	if dials.Load() != 1 {
		t.Fatalf("dials = %d", dials.Load())
	}
}

func TestPostDefaultLookupLocalhostBlocked(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)

	_, err := egress.New().Post(ctx, testReq("http://localhost/"))
	if err == nil {
		t.Fatal("localhost allowed")
	}

	if errors.Is(err, egress.ErrBlocked) {
		return
	}

	t.Logf("localhost lookup err (default resolver exercised): %v", err)
}

func TestNewIsHTTP(t *testing.T) {
	t.Parallel()

	var _ deliver.HTTP = egress.New()
}

func pinClient(t *testing.T, srv *httptest.Server, ip netip.Addr, dials *atomic.Int32) *egress.Client {
	t.Helper()

	return egress.NewHarness(
		func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{ip}, nil
		},
		pinDialTo(t, srv, ip, "80", dials),
		nil,
	)
}

func pinDialTo(
	t *testing.T,
	srv *httptest.Server,
	ip netip.Addr,
	port string,
	dials *atomic.Int32,
) func(context.Context, string, string) (net.Conn, error) {
	t.Helper()

	want := egress.PinString(ip, port)

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		if addr != want {
			return nil, fmt.Errorf("dial pin: got %s want %s", addr, want)
		}

		d := net.Dialer{}

		return d.DialContext(ctx, network, srv.Listener.Addr().String())
	}
}

func blockedDialHarness(dials *atomic.Int32) *egress.Client {
	return egress.NewHarness(nil, countingDial(dials), nil)
}

func countingDial(dials *atomic.Int32) func(context.Context, string, string) (net.Conn, error) {
	return func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)

		return nil, errors.New("dialed")
	}
}
