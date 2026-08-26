package egress

import (
	"crypto/x509"
	"net/netip"
)

func NewHarness(lookup lookupFunc, dial dialFunc, roots *x509.CertPool) *Client {
	return &Client{lookup: lookup, dial: dial, roots: roots}
}

func Denied(ip netip.Addr) bool {
	return denied(ip)
}

func PinString(ip netip.Addr, port string) string {
	ap, err := parseAddrPort(ip, port)
	if err != nil {
		return ""
	}

	return ap.String()
}
