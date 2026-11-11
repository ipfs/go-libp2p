package holepunch

import (
	"context"
	"slices"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func removeRelayAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	return slices.DeleteFunc(addrs, isRelayAddress)
}

func isRelayAddress(a ma.Multiaddr) bool {
	_, err := a.ValueForProtocol(ma.P_CIRCUIT)
	return err == nil
}

// maxHolePunchAddrs caps the peer-supplied addresses dialed per DCUtR message,
// matching autonatv2's maxPeerAddresses.
const maxHolePunchAddrs = 50

// filterHolePunchAddrs keeps only the public, IP-based addresses worth
// hole-punching to, capped at maxHolePunchAddrs. Callers run it after any
// AddrFilter, so it has the final say on what the hole puncher dials.
func filterHolePunchAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	addrs = ma.FilterAddrs(addrs, isPublicIPAddr)
	if len(addrs) > maxHolePunchAddrs {
		addrs = addrs[:maxHolePunchAddrs]
	}
	return addrs
}

// isPublicIPAddr reports whether a is a public IP address. DNS names are
// rejected: IsPublicAddr accepts them, but they resolve to a peer-controlled IP.
func isPublicIPAddr(a ma.Multiaddr) bool {
	if _, err := manet.ToIP(a); err != nil {
		return false
	}
	return manet.IsPublicAddr(a)
}

func addrsToBytes(as []ma.Multiaddr) [][]byte {
	bzs := make([][]byte, 0, len(as))
	for _, a := range as {
		bzs = append(bzs, a.Bytes())
	}
	return bzs
}

func addrsFromBytes(bzs [][]byte) []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, 0, len(bzs))
	for _, bz := range bzs {
		a, err := ma.NewMultiaddrBytes(bz)
		if err == nil {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

func getDirectConnection(h host.Host, p peer.ID) network.Conn {
	for _, c := range h.Network().ConnsToPeer(p) {
		if !isRelayAddress(c.RemoteMultiaddr()) {
			return c
		}
	}
	return nil
}

func holePunchConnect(ctx context.Context, host host.Host, pi peer.AddrInfo, isClient bool) error {
	holePunchCtx := network.WithSimultaneousConnect(ctx, isClient, "hole-punching")
	forceDirectConnCtx := network.WithForceDirectDial(holePunchCtx, "hole-punching")

	log.Debug("holepunchConnect", "source_peer", host.ID(), "destination_peer", pi.ID, "addrs", pi.Addrs)
	if err := host.Connect(forceDirectConnCtx, pi); err != nil {
		log.Debug("hole punch attempt with peer failed", "destination_peer", pi.ID, "err", err)
		return err
	}
	log.Debug("hole punch successful", "destination_peer", pi.ID)
	return nil
}
