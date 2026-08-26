package domain_test

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/flexer2006/hopper/internal/domain"
)

func TestAddrDenied(t *testing.T) {
	t.Parallel()

	blocked := []string{
		"127.0.0.1",
		"169.254.169.254",
		"100.64.0.1",
		"::1",
		"::ffff:127.0.0.1",
		"8.8.8.8",
	}
	want := []bool{true, true, true, true, true, false}

	for i, s := range blocked {
		if domain.AddrDenied(netip.MustParseAddr(s)) != want[i] {
			t.Fatalf("AddrDenied(%s) = %v, want %v", s, !want[i], want[i])
		}
	}

	if !domain.AddrDenied(netip.Addr{}) {
		t.Fatal("zero Addr allowed")
	}
}

func TestAdmitTarget(t *testing.T) {
	t.Parallel()

	_, err := domain.AdmitTarget("https://example.com/hook")
	if err != nil {
		t.Fatal(err)
	}

	_, err = domain.AdmitTarget("http://127.0.0.1/")
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("loopback err = %v", err)
	}

	_, err = domain.AdmitTarget("http://[::ffff:127.0.0.1]/")
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("mapped loopback err = %v", err)
	}

	_, err = domain.AdmitTarget("ftp://example.com/")
	if !errors.Is(err, domain.ErrInvalidTarget) {
		t.Fatalf("ftp err = %v", err)
	}
}
