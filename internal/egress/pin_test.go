package egress //nolint:testpackage // pinDial, dialTCP, lookupHost, parseAddrPort

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"
)

func TestPinDialRejectsNetworkAndAddr(t *testing.T) {
	t.Parallel()

	dial := New().pinDial("8.8.8.8:80")

	_, err := dial(t.Context(), "udp", "8.8.8.8:80")
	if !errors.Is(err, errDialMismatch) {
		t.Fatalf("udp err = %v", err)
	}

	_, err = dial(t.Context(), "tcp", "1.1.1.1:80")
	if !errors.Is(err, errDialMismatch) {
		t.Fatalf("addr err = %v", err)
	}
}

func TestDialTCPDefaultDialer(t *testing.T) {
	t.Parallel()

	lc := net.ListenConfig{}

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if closeErr := ln.Close(); closeErr != nil {
			t.Log(closeErr)
		}
	})

	done := make(chan struct{})

	go func() {
		defer close(done)

		conn, accErr := ln.Accept()
		if accErr != nil {
			return
		}

		if closeErr := conn.Close(); closeErr != nil {
			return
		}
	}()

	conn, err := New().dialTCP(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func TestLookupHostDefaultResolver(t *testing.T) {
	t.Parallel()

	addrs, err := New().lookupHost(t.Context(), "localhost")
	if err != nil {
		t.Log(err)

		return
	}

	if len(addrs) == 0 {
		t.Fatal("empty localhost addrs")
	}
}

func TestParseAddrPortRejectsOverflow(t *testing.T) {
	t.Parallel()

	_, err := parseAddrPort(netip.MustParseAddr("8.8.8.8"), "70000")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("err = %v", err)
	}
}

func TestScrubTransportPassthrough(t *testing.T) {
	t.Parallel()

	want := errors.New("plain")
	got := scrubTransport(want)
	if !errors.Is(got, want) {
		t.Fatal("passthrough")
	}
}

func TestTransportHardeningInvariants(t *testing.T) {
	t.Parallel()

	client := NewWithLimits(7*time.Second, 2048)
	pin := netip.MustParseAddrPort("93.184.216.34:443")
	tr := client.transport(pin, "example.com")

	if !tr.DisableKeepAlives {
		t.Fatal("DisableKeepAlives must be true")
	}

	if tr.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 must be false")
	}

	if tr.TLSNextProto == nil || len(tr.TLSNextProto) != 0 {
		t.Fatalf("TLSNextProto = %v, want non-nil empty map", tr.TLSNextProto)
	}

	if tr.Protocols == nil || !tr.Protocols.HTTP1() || tr.Protocols.HTTP2() || tr.Protocols.UnencryptedHTTP2() {
		t.Fatalf("Protocols = %+v, want HTTP/1.1 only", tr.Protocols)
	}

	if tr.TLSClientConfig == nil || tr.TLSClientConfig.MinVersion != tls.VersionTLS12 ||
		tr.TLSClientConfig.ServerName != "example.com" || tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("TLSClientConfig = %+v", tr.TLSClientConfig)
	}

	if tr.ResponseHeaderTimeout != 7*time.Second {
		t.Fatalf("ResponseHeaderTimeout = %s, want 7s", tr.ResponseHeaderTimeout)
	}

	proxyURL, proxyErr := tr.Proxy(nil)
	if proxyURL != nil || proxyErr != nil {
		t.Fatalf("Proxy = (%v, %v), want (nil, nil)", proxyURL, proxyErr)
	}

	if !errors.Is(rejectRedirect(nil, nil), http.ErrUseLastResponse) {
		t.Fatal("rejectRedirect must return http.ErrUseLastResponse")
	}
}
