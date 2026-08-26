package egress

import (
	"net/netip"
	"slices"
)

// ADR-011 denylist after Unmap. IsPrivate is RFC1918+ULA only — not CGNAT.
//
//nolint:gochecknoglobals // immutable process-lifetime CIDR table
var blockedPrefixes = []netip.Prefix{
	mustPrefix("0.0.0.0/8"),
	mustPrefix("10.0.0.0/8"),
	mustPrefix("100.64.0.0/10"),
	mustPrefix("127.0.0.0/8"),
	mustPrefix("169.254.0.0/16"),
	mustPrefix("172.16.0.0/12"),
	mustPrefix("192.0.0.0/24"),
	mustPrefix("192.0.2.0/24"),
	mustPrefix("192.168.0.0/16"),
	mustPrefix("198.18.0.0/15"),
	mustPrefix("198.51.100.0/24"),
	mustPrefix("203.0.113.0/24"),
	mustPrefix("224.0.0.0/4"),
	mustPrefix("240.0.0.0/4"),
	mustPrefix("::/128"),
	mustPrefix("::1/128"),
	mustPrefix("fe80::/10"),
	mustPrefix("fc00::/7"),
	mustPrefix("2001:db8::/32"),
	mustPrefix("2001:2::/48"),
	mustPrefix("64:ff9b::/96"),
	mustPrefix("64:ff9b:1::/48"),
	mustPrefix("ff00::/8"),
}

func mustPrefix(s string) netip.Prefix {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		panic(err)
	}

	return p.Masked()
}

func denied(ip netip.Addr) bool {
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
