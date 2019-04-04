package relay

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	basic "github.com/libp2p/go-libp2p/p2p/host/basic"

	autonat "github.com/libp2p/go-libp2p-autonat"
	_ "github.com/libp2p/go-libp2p-circuit"
	discovery "github.com/libp2p/go-libp2p-discovery"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	routing "github.com/libp2p/go-libp2p-routing"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

const (
	RelayRendezvous = "/libp2p/relay"
)

var (
	DesiredRelays = 3

	BootDelay = 20 * time.Second

	unspecificRelay = ma.StringCast("/p2p-circuit")
)

// AutoRelayHost is a Host that uses relays for connectivity when a NAT is detected.
type AutoRelayHost struct {
	*basic.BasicHost
	discover discovery.Discoverer
	router   routing.PeerRouting
	autonat  autonat.AutoNAT
	addrsF   basic.AddrsFactory

	disconnect chan struct{}

	mx     sync.Mutex
	relays map[peer.ID]pstore.PeerInfo
	addrs  []ma.Multiaddr
}

// Warning:  This function should not be used directly by clients activating
// autorelay. NewAutoRelayHost is used internally by the libp2p.New generator.
// To enable autorelay, use the EnableAutoRelay Option.
// https://godoc.org/github.com/libp2p/go-libp2p#EnableAutoRelay
func NewAutoRelayHost(ctx context.Context, bhost *basic.BasicHost, discover discovery.Discoverer, router routing.PeerRouting) *AutoRelayHost {
	h := &AutoRelayHost{
		BasicHost:  bhost,
		discover:   discover,
		router:     router,
		addrsF:     bhost.AddrsFactory,
		relays:     make(map[peer.ID]pstore.PeerInfo),
		disconnect: make(chan struct{}, 1),
	}
	h.autonat = autonat.NewAutoNAT(ctx, bhost, h.baseAddrs)
	bhost.AddrsFactory = h.hostAddrs
	bhost.Network().Notify(h)
	go h.background(ctx)
	return h
}

func (h *AutoRelayHost) hostAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	h.mx.Lock()
	defer h.mx.Unlock()
	if h.addrs != nil && h.autonat.Status() == autonat.NATStatusPrivate {
		return h.addrs
	} else {
		return filterUnspecificRelay(h.addrsF(addrs))
	}
}

func (h *AutoRelayHost) baseAddrs() []ma.Multiaddr {
	return filterUnspecificRelay(h.addrsF(h.AllAddrs()))
}

func (h *AutoRelayHost) background(ctx context.Context) {
	select {
	case <-time.After(autonat.AutoNATBootDelay + BootDelay):
	case <-ctx.Done():
		return
	}

	// when true, we need to identify push
	push := false

	for {
		wait := autonat.AutoNATRefreshInterval
		switch h.autonat.Status() {
		case autonat.NATStatusUnknown:
			wait = autonat.AutoNATRetryInterval

		case autonat.NATStatusPublic:
			// invalidate addrs
			h.mx.Lock()
			if h.addrs != nil {
				h.addrs = nil
				push = true
			}
			h.mx.Unlock()

			// if we had previously announced relay addrs, push our public addrs
			if push {
				push = false
				h.PushIdentify()
			}

		case autonat.NATStatusPrivate:
			push = false // clear, findRelays pushes as needed
			h.findRelays(ctx)
		}

		select {
		case <-h.disconnect:
			// invalidate addrs
			h.mx.Lock()
			if h.addrs != nil {
				h.addrs = nil
				push = true
			}
			h.mx.Unlock()
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

func (h *AutoRelayHost) findRelays(ctx context.Context) {
	h.mx.Lock()
	if len(h.relays) >= DesiredRelays {
		h.mx.Unlock()
		return
	}
	need := DesiredRelays - len(h.relays)
	h.mx.Unlock()

	limit := 20
	if need > limit/2 {
		limit = 2 * need
	}

	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	pis, err := discovery.FindPeers(dctx, h.discover, RelayRendezvous, limit)
	cancel()
	if err != nil {
		log.Debugf("error discovering relays: %s", err.Error())
		return
	}

	pis = h.selectRelays(pis)

	update := 0

	for _, pi := range pis {
		h.mx.Lock()
		if _, ok := h.relays[pi.ID]; ok {
			h.mx.Unlock()
			continue
		}
		h.mx.Unlock()

		cctx, cancel := context.WithTimeout(ctx, 60*time.Second)

		if len(pi.Addrs) == 0 {
			pi, err = h.router.FindPeer(cctx, pi.ID)
			if err != nil {
				log.Debugf("error finding relay peer %s: %s", pi.ID, err.Error())
				cancel()
				continue
			}
		}

		err = h.Connect(cctx, pi)
		cancel()
		if err != nil {
			log.Debugf("error connecting to relay %s: %s", pi.ID, err.Error())
			continue
		}

		log.Debugf("connected to relay %s", pi.ID)
		h.mx.Lock()
		h.relays[pi.ID] = pi
		h.mx.Unlock()

		// tag the connection as very important
		h.ConnManager().TagPeer(pi.ID, "relay", 42)

		update++
		need--
		if need == 0 {
			break
		}
	}

	if update > 0 || h.addrs == nil {
		h.updateAddrs()
	}
}

func (h *AutoRelayHost) selectRelays(pis []pstore.PeerInfo) []pstore.PeerInfo {
	// TODO better relay selection strategy; this just selects random relays
	//      but we should probably use ping latency as the selection metric
	shuffleRelays(pis)
	return pis
}

func (h *AutoRelayHost) updateAddrs() {
	h.doUpdateAddrs()
	h.PushIdentify()
}

// This function updates our NATed advertised addrs (h.addrs)
// The public addrs are rewritten so that they only retain the public IP part; they
// become undialable but are useful as a hint to the dialer to determine whether or not
// to dial private addrs.
// The non-public addrs are included verbatim so that peers behind the same NAT/firewall
// can still dial us directly.
// On top of those, we add the relay-specific addrs for the relays to which we are
// connected. For each non-private relay addr, we encapsulate the p2p-circuit addr
// through which we can be dialed.
func (h *AutoRelayHost) doUpdateAddrs() {
	h.mx.Lock()
	defer h.mx.Unlock()

	addrs := h.baseAddrs()
	raddrs := make([]ma.Multiaddr, 0, len(addrs)+len(h.relays))

	// remove our public addresses from the list
	for _, addr := range addrs {
		if manet.IsPublicAddr(addr) {
			continue
		}
		raddrs = append(raddrs, addr)
	}

	// add relay specific addrs to the list
	for _, pi := range h.relays {
		circuit, err := ma.NewMultiaddr(fmt.Sprintf("/p2p/%s/p2p-circuit", pi.ID.Pretty()))
		if err != nil {
			panic(err)
		}

		for _, addr := range pi.Addrs {
			if !manet.IsPrivateAddr(addr) {
				pub := addr.Encapsulate(circuit)
				raddrs = append(raddrs, pub)
			}
		}
	}

	h.addrs = raddrs
}

func filterUnspecificRelay(addrs []ma.Multiaddr) []ma.Multiaddr {
	res := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr.Equal(unspecificRelay) {
			continue
		}
		res = append(res, addr)
	}
	return res
}

func shuffleRelays(pis []pstore.PeerInfo) {
	for i := range pis {
		j := rand.Intn(i + 1)
		pis[i], pis[j] = pis[j], pis[i]
	}
}

func containsAddr(lst []ma.Multiaddr, addr ma.Multiaddr) bool {
	for _, xaddr := range lst {
		if xaddr.Equal(addr) {
			return true
		}
	}
	return false
}

// notify
func (h *AutoRelayHost) Listen(inet.Network, ma.Multiaddr)      {}
func (h *AutoRelayHost) ListenClose(inet.Network, ma.Multiaddr) {}
func (h *AutoRelayHost) Connected(inet.Network, inet.Conn)      {}

func (h *AutoRelayHost) Disconnected(_ inet.Network, c inet.Conn) {
	p := c.RemotePeer()
	h.mx.Lock()
	defer h.mx.Unlock()
	if _, ok := h.relays[p]; ok {
		delete(h.relays, p)
		select {
		case h.disconnect <- struct{}{}:
		default:
		}
	}
}

func (h *AutoRelayHost) OpenedStream(inet.Network, inet.Stream) {}
func (h *AutoRelayHost) ClosedStream(inet.Network, inet.Stream) {}

var _ host.Host = (*AutoRelayHost)(nil)
