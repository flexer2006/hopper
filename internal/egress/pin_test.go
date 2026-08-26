package egress //nolint:testpackage // pinDial, dialTCP, lookupHost, parseAddrPort

import (
	"errors"
	"net"
	"net/netip"
	"testing"
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
