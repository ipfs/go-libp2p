package swarm

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// The 250ms value is from happy eyeballs RFC 8305. This is a rough estimate of 1 RTT
const (
	// duration by which TCP dials are delayed relative to the last QUIC dial
	PublicTCPDelay  = 250 * time.Millisecond
	PrivateTCPDelay = 30 * time.Millisecond

	// duration by which QUIC dials are delayed relative to previous QUIC dial
	PublicQUICDelay  = 250 * time.Millisecond
	PrivateQUICDelay = 30 * time.Millisecond

	// RelayDelay is the duration by which relay dials are delayed relative to direct addresses
	RelayDelay = 500 * time.Millisecond

	// delay for other transport addresses. This will apply to /webrtc-direct.
	PublicOtherDelay  = 1 * time.Second
	PrivateOtherDelay = 100 * time.Millisecond
)

// NoDelayDialRanker ranks addresses with no delay. This is useful for simultaneous connect requests.
func NoDelayDialRanker(addrs []ma.Multiaddr) []network.AddrDelay {
	return getAddrDelay(addrs, 0, 0, 0, 0)
}

// DefaultDialRanker determines the ranking of outgoing connection attempts.
//
// Addresses are grouped into three distinct groups:
//
//   - private addresses (localhost and local networks (RFC 1918))
//   - public addresses
//   - relay addresses
//
// Within each group, the addresses are ranked according to the ranking logic described below.
// We then dial addresses according to this ranking, with short timeouts applied between dial attempts.
// This ranking logic dramatically reduces the number of simultaneous dial attempts, while introducing
// no additional latency in the vast majority of cases.
//
// Private and public address groups are dialed in parallel.
// Dialing relay addresses is delayed by 500 ms, if we have any non-relay alternatives.
//
// Within each group (private, public, relay addresses) we apply the following ranking logic:
//
//  1. If both IPv6 QUIC and IPv4 QUIC addresses are present, we do a Happy Eyeballs RFC 8305 style ranking.
//     First dial the IPv6 QUIC address with the lowest port. After this we dial the IPv4 QUIC address with
//     the lowest port delayed by 250ms (PublicQUICDelay) for public addresses, and 30ms (PrivateQUICDelay)
//     for local addresses. After this we dial all the rest of the addresses delayed by 250ms (PublicQUICDelay)
//     for public addresses, and 30ms (PrivateQUICDelay) for local addresses.
//  2. If only one of QUIC IPv6 or QUIC IPv4 addresses are present, dial the QUIC address with the lowest port
//     first. After this we dial the rest of the QUIC addresses delayed by 250ms (PublicQUICDelay) for public
//     addresses, and 30ms (PrivateQUICDelay) for local addresses.
//  3. If a QUIC or WebTransport address is present, TCP addresses dials are delayed relative to the last QUIC dial:
//     We prefer to end up with a QUIC connection. For public addresses, the delay introduced is 250ms (PublicTCPDelay),
//     and for private addresses 30ms (PrivateTCPDelay).
//  4. For the TCP addresses we follow a strategy similar to QUIC with an optimisation for handling the long TCP
//     handshake time described in 6. If both IPv6 TCP and IPv4 TCP addresses are present, we do a Happy Eyeballs
//     style ranking. First dial the IPv6 TCP address with the lowest port. After this, dial the IPv4 TCP address
//     with the lowest port delayed by 250ms (PublicTCPDelay) for public addresses, and 30ms (PrivateTCPDelay)
//     for local addresses. After this we dial all the rest of the addresses delayed by 250ms (PublicTCPDelay) for
//     public addresses, and 30ms (PrivateTCPDelay) for local addresses.
//  5. If only one of TCP IPv6 or TCP IPv4 addresses are present, dial the TCP address with the lowest port
//     first. After this we dial the rest of the TCP addresses delayed by 250ms (PublicTCPDelay) for public
//     addresses, and 30ms (PrivateTCPDelay) for local addresses.
//  6. When a TCP socket is connected and awaiting security and muxer upgrade, we stop new dials for 2*PublicTCPDelay
//     to allow for the upgrade to complete.
//  7. WebRTC Direct, and other IP transport addresses are dialed 1 second after the last QUIC or TCP dial.
//     We only ever need to dial these if the peer doesn't have any other transport available, in which
//     case these are dialed immediately.
//
// We dial lowest ports first as they are more likely to be the listen port.
func DefaultDialRanker(addrs []ma.Multiaddr) []network.AddrDelay {
	// filterAddrs partitions addrs in place, so len(addrs) shrinks on every call
	// below. Hold on to the total up front: that is what res ends up holding.
	totalAddrs := len(addrs)
	relay, addrs := filterAddrs(addrs, isRelayAddr)
	pvt, addrs := filterAddrs(addrs, manet.IsPrivateAddr)
	public, addrs := filterAddrs(addrs, func(a ma.Multiaddr) bool { return isProtocolAddr(a, ma.P_IP4) || isProtocolAddr(a, ma.P_IP6) })

	var relayOffset time.Duration
	if len(public) > 0 {
		// if there is a public direct address available delay relay dials
		relayOffset = RelayDelay
	}

	res := make([]network.AddrDelay, 0, totalAddrs)
	res = append(res, getAddrDelay(pvt, PrivateTCPDelay, PrivateQUICDelay, PrivateOtherDelay, 0)...)
	res = append(res, getAddrDelay(public, PublicTCPDelay, PublicQUICDelay, PublicOtherDelay, 0)...)
	res = append(res, getAddrDelay(relay, PublicTCPDelay, PublicQUICDelay, PublicOtherDelay, relayOffset)...)
	var maxDelay time.Duration
	if len(res) > 0 {
		maxDelay = res[len(res)-1].Delay
	}

	for i := range addrs {
		res = append(res, network.AddrDelay{Addr: addrs[i], Delay: maxDelay + PublicOtherDelay})
	}

	return res
}

// getAddrDelay ranks a group of addresses according to the ranking logic explained in
// documentation for defaultDialRanker.
// offset is used to delay all addresses by a fixed duration. This is useful for delaying all relay
// addresses relative to direct addresses.
func getAddrDelay(addrs []ma.Multiaddr, tcpDelay time.Duration, quicDelay time.Duration,
	otherDelay time.Duration, offset time.Duration) []network.AddrDelay {
	if len(addrs) == 0 {
		return nil
	}

	sort.Slice(addrs, func(i, j int) bool { return score(addrs[i]) < score(addrs[j]) })

	// addrs is now sorted by (Transport, IPVersion). Reorder addrs for happy eyeballs dialing.
	// For QUIC and TCP, if we have both IPv6 and IPv4 addresses, move the
	// highest priority IPv4 address to the second position.
	happyEyeballsQUIC := false
	happyEyeballsTCP := false
	// tcpStartIdx is the index of the first TCP Address
	var tcpStartIdx int
	{
		i := 0
		// If the first QUIC address is IPv6 move the first QUIC IPv4 address to second position
		if isQUICAddr(addrs[0]) && isProtocolAddr(addrs[0], ma.P_IP6) {
			for j := 1; j < len(addrs); j++ {
				if isQUICAddr(addrs[j]) && isProtocolAddr(addrs[j], ma.P_IP4) {
					// The first IPv4 address is at position j
					// Move the jth element at position 1 shifting the affected elements
					if j > 1 {
						a := addrs[j]
						copy(addrs[2:], addrs[1:j])
						addrs[1] = a
					}
					happyEyeballsQUIC = true
					i = j + 1
					break
				}
			}
		}

		for tcpStartIdx = i; tcpStartIdx < len(addrs); tcpStartIdx++ {
			if isProtocolAddr(addrs[tcpStartIdx], ma.P_TCP) {
				break
			}
		}

		// If the first TCP address is IPv6 move the first TCP IPv4 address to second position
		if tcpStartIdx < len(addrs) && isProtocolAddr(addrs[tcpStartIdx], ma.P_IP6) {
			for j := tcpStartIdx + 1; j < len(addrs); j++ {
				if isProtocolAddr(addrs[j], ma.P_TCP) && isProtocolAddr(addrs[j], ma.P_IP4) {
					// First TCP IPv4 address is at position j, move it to position tcpStartIdx+1
					// which is the second priority TCP address
					if j > tcpStartIdx+1 {
						a := addrs[j]
						copy(addrs[tcpStartIdx+2:], addrs[tcpStartIdx+1:j])
						addrs[tcpStartIdx+1] = a
					}
					happyEyeballsTCP = true
					break
				}
			}
		}
	}

	res := make([]network.AddrDelay, 0, len(addrs))
	var tcpFirstDialDelay time.Duration
	var lastQUICOrTCPDelay time.Duration
	for i, addr := range addrs {
		var delay time.Duration
		switch {
		case isQUICAddr(addr):
			// We dial an IPv6 address, then after quicDelay an IPv4
			// address, then after a further quicDelay we dial the rest of the addresses.
			if i == 1 {
				delay = quicDelay
			}
			if i > 1 {
				// If we have happy eyeballs for QUIC, dials after the second position
				// will be delayed by 2*quicDelay
				if happyEyeballsQUIC {
					delay = 2 * quicDelay
				} else {
					delay = quicDelay
				}
			}
			lastQUICOrTCPDelay = delay
			tcpFirstDialDelay = delay + tcpDelay
		case isProtocolAddr(addr, ma.P_TCP):
			// We dial an IPv6 address, then after tcpDelay an IPv4
			// address, then after a further tcpDelay we dial the rest of the addresses.
			if i == tcpStartIdx+1 {
				delay = tcpDelay
			}
			if i > tcpStartIdx+1 {
				// If we have happy eyeballs for TCP, dials after the second position
				// will be delayed by 2*tcpDelay
				if happyEyeballsTCP {
					delay = 2 * tcpDelay
				} else {
					delay = tcpDelay
				}
			}
			delay += tcpFirstDialDelay
			lastQUICOrTCPDelay = delay
		// if it's neither quic, webtransport, tcp, or websocket address
		default:
			delay = lastQUICOrTCPDelay + otherDelay
		}
		res = append(res, network.AddrDelay{Addr: addr, Delay: offset + delay})
	}
	return res
}

// score scores a multiaddress for dialing delay. Lower is better.
// The lower 16 bits of the result are the port. Low ports are ranked higher because they're
// more likely to be listen addresses.
// The addresses are ranked as:
// QUICv1 IPv6 > QUICdraft29 IPv6 > QUICv1 IPv4 > QUICdraft29 IPv4 >
// WebTransport IPv6 > WebTransport IPv4 > TCP IPv6 > TCP IPv4
//
// The sort in getAddrDelay calls this O(n*log(n)) times, so it walks the multiaddr
// once and reads the port straight out of the component bytes. ValueForProtocol
// would re-scan the address for each protocol we check and allocate a string per
// port only to parse it straight back into an int.
func score(a ma.Multiaddr) int {
	var ip4Weight, udpPort, tcpPort int
	var hasWebTransport, hasQUIC, hasQUICV1, hasTCP, hasWebRTCDirect bool

	for _, c := range a {
		switch c.Protocol().Code {
		case ma.P_IP4:
			ip4Weight = 1 << 18
		case ma.P_UDP:
			udpPort = componentPort(c)
		case ma.P_TCP:
			tcpPort = componentPort(c)
			hasTCP = true
		case ma.P_WEBTRANSPORT:
			hasWebTransport = true
		case ma.P_QUIC:
			hasQUIC = true
		case ma.P_QUIC_V1:
			hasQUICV1 = true
		case ma.P_WEBRTC_DIRECT:
			hasWebRTCDirect = true
		}
	}

	// The order of these cases matters: a WebTransport address also has a QUIC_V1
	// component, and every QUIC address also has a UDP component.
	switch {
	case hasWebTransport:
		return ip4Weight + (1 << 19) + udpPort
	case hasQUIC:
		return ip4Weight + udpPort + (1 << 17)
	case hasQUICV1:
		return ip4Weight + udpPort
	case hasTCP:
		return ip4Weight + tcpPort + (1 << 20)
	case hasWebRTCDirect:
		return 1 << 21
	}
	return (1 << 30)
}

// componentPort reads the port out of a TCP or UDP component. Both encode it
// as a big endian uint16.
func componentPort(c ma.Component) int {
	raw := c.RawValue()
	if len(raw) != 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(raw))
}

func isProtocolAddr(a ma.Multiaddr, p int) bool {
	found := false
	ma.ForEach(a, func(c ma.Component) bool {
		if c.Protocol().Code == p {
			found = true
			return false
		}
		return true
	})
	return found
}

func isQUICAddr(a ma.Multiaddr) bool {
	return isProtocolAddr(a, ma.P_QUIC) || isProtocolAddr(a, ma.P_QUIC_V1)
}

// filterAddrs filters an address slice in place
func filterAddrs(addrs []ma.Multiaddr, f func(a ma.Multiaddr) bool) (filtered, rest []ma.Multiaddr) {
	j := 0
	for i := range addrs {
		if f(addrs[i]) {
			addrs[i], addrs[j] = addrs[j], addrs[i]
			j++
		}
	}
	return addrs[:j], addrs[j:]
}
