package egress_test

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/flexer2006/hopper/internal/domain"
	"github.com/flexer2006/hopper/internal/egress"
)

func FuzzAdmit(f *testing.F) {
	f.Add("http://127.0.0.1/")
	f.Add("http://169.254.169.254/")
	f.Add("https://example.com/")
	f.Add("ftp://example.com/")
	f.Add("https://user@example.com/")
	f.Add("http://[::ffff:127.0.0.1]/")
	f.Add("http://100.64.0.1/")
	f.Add("http://8.8.8.8/")

	f.Fuzz(func(t *testing.T, raw string) {
		parsed, err := domain.ParseTarget(raw)
		if err != nil {
			if parsed != nil {
				t.Fatalf("ParseTarget err with URL")
			}

			return
		}

		if parsed.User != nil {
			t.Fatalf("userinfo accepted")
		}

		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			t.Fatalf("scheme %s", parsed.Scheme)
		}

		host := parsed.Hostname()
		ip, parseErr := netip.ParseAddr(host)
		if parseErr != nil {
			return
		}

		blocked := egress.Denied(ip)
		loop := ip.Unmap().IsLoopback() || ip.Unmap().IsLinkLocalUnicast()
		if loop && !blocked {
			t.Fatalf("loopback/link-local allowed: %s", raw)
		}
	})
}
