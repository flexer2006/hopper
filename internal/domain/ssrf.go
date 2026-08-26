package domain

import (
	"fmt"
	"net/netip"
	"net/url"
	"slices"
)

//nolint:gochecknoglobals // immutable process-lifetime CIDR table
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8").Masked(),
	netip.MustParsePrefix("10.0.0.0/8").Masked(),
	netip.MustParsePrefix("100.64.0.0/10").Masked(),
	netip.MustParsePrefix("127.0.0.0/8").Masked(),
	netip.MustParsePrefix("169.254.0.0/16").Masked(),
	netip.MustParsePrefix("172.16.0.0/12").Masked(),
	netip.MustParsePrefix("192.0.0.0/24").Masked(),
	netip.MustParsePrefix("192.0.2.0/24").Masked(),
	netip.MustParsePrefix("192.168.0.0/16").Masked(),
	netip.MustParsePrefix("198.18.0.0/15").Masked(),
	netip.MustParsePrefix("198.51.100.0/24").Masked(),
	netip.MustParsePrefix("203.0.113.0/24").Masked(),
	netip.MustParsePrefix("224.0.0.0/4").Masked(),
	netip.MustParsePrefix("240.0.0.0/4").Masked(),
	netip.MustParsePrefix("::/128").Masked(),
	netip.MustParsePrefix("::1/128").Masked(),
	netip.MustParsePrefix("fe80::/10").Masked(),
	netip.MustParsePrefix("fc00::/7").Masked(),
	netip.MustParsePrefix("2001:db8::/32").Masked(),
	netip.MustParsePrefix("2001:2::/48").Masked(),
	netip.MustParsePrefix("64:ff9b::/96").Masked(),
	netip.MustParsePrefix("64:ff9b:1::/48").Masked(),
	netip.MustParsePrefix("ff00::/8").Masked(),
}

func AdmitTarget(raw string) (*url.URL, error) {
	parsed, err := ParseTarget(raw)
	if err != nil {
		return nil, err
	}

	ip, ipErr := netip.ParseAddr(parsed.Hostname())
	if ipErr != nil {
		return parsed, nil //nolint:nilerr // hostname is not a literal IP; DNS SSRF is egress
	}

	if AddrDenied(ip) {
		return nil, fmt.Errorf("%w: blocked destination", ErrInvalidTarget)
	}

	return parsed, nil
}

func AddrDenied(ip netip.Addr) bool {
	ip = ip.Unmap().WithZone("")
	if !ip.IsValid() {
		return true
	}

	if ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsPrivate() ||
		!ip.IsGlobalUnicast() {
		return true
	}

	return slices.ContainsFunc(blockedPrefixes, func(prefix netip.Prefix) bool {
		return prefix.Contains(ip)
	})
}
