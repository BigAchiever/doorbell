package routing

import (
	"fmt"
	"net"
	"os"
)

// SelfAddress works out how a sibling container should reach this one.
//
// Why this is not simply "gw:3000": that hostname resolves through the
// project's L7 balancer and load-balances across every container of the
// service. Forwarding a request there would round-robin it straight back into
// the pool, possibly to the same container that could not serve it — a loop,
// not a route. Peer forwarding needs the specific container.
//
// Preference order:
//  1. DOORBELL_SELF_ADDR, if the operator set it explicitly.
//  2. The first non-loopback address on a real interface. On Zerops each
//     container sits on the project's private VXLAN, so this is the address
//     siblings can actually dial.
func SelfAddress(port string) (string, error) {
	if explicit := os.Getenv("DOORBELL_SELF_ADDR"); explicit != "" {
		return explicit, nil
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("routing: list interfaces: %w", err)
	}

	// IPv6 first: Zerops projects are IPv6-native on the private network, and
	// preferring it avoids picking up a stray docker-style IPv4 bridge.
	var v4 string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() || ipnet.IP.IsLinkLocalUnicast() {
				continue
			}
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				if v4 == "" {
					v4 = ip4.String()
				}
				continue
			}
			return net.JoinHostPort(ipnet.IP.String(), port), nil
		}
	}
	if v4 != "" {
		return net.JoinHostPort(v4, port), nil
	}
	return "", fmt.Errorf("routing: no usable non-loopback address found")
}
