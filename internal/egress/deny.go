package egress

import (
	"net/netip"

	"github.com/flexer2006/hopper/internal/domain"
)

func denied(ip netip.Addr) bool {
	return domain.AddrDenied(ip)
}
