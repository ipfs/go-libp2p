package relay

import (
	"context"
	"math/rand"
	"sync"
	"time"

	circuitv2 "github.com/libp2p/go-libp2p-circuit/v2/client"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	discovery "github.com/libp2p/go-libp2p-discovery"
	basic "github.com/libp2p/go-libp2p/p2p/host/basic"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	RelayRendezvous = "/libp2p/relay"
)

var (
	DesiredRelays = 1

	BootDelay = 20 * time.Second

	connMgrTag = "relay"

	// ReserveRefreshInterval is the interval at which we will attempt reservation refreshes for refreshes that are due.
	ReserveRefreshInterval = 1 * time.Minute

	// MinTTLRefresh is the minimum Limited Relay TTL below which we will NOT schedule the reservation for refreshes.
	MinTTLRefresh = 10 * time.Minute
)

// These are the known PL-operated V2 relays
// TODO Fill this up
var DefaultRelays = []string{}

type relayInfo struct {
	addrs      []ma.Multiaddr
	resfreshAt time.Time
}

// AutoRelay is a Host that uses relays for connectivity when a NAT is detected.
type AutoRelay struct {
	ctx      context.Context
	host     *basic.BasicHost
	discover discovery.Discoverer
	router   routing.PeerRouting
	addrsF   basic.AddrsFactory

	static []peer.AddrInfo

	disconnect chan struct{}

	mx     sync.Mutex
	relays map[peer.ID]*relayInfo
	status network.Reachability

	cachedAddrs       []ma.Multiaddr
	cachedAddrsExpiry time.Time
}

func NewAutoRelay(ctx context.Context, bhost *basic.BasicHost, discover discovery.Discoverer, router routing.PeerRouting, static []peer.AddrInfo) *AutoRelay {
	ar := &AutoRelay{
		ctx:        ctx,
		host:       bhost,
		discover:   discover,
		router:     router,
		addrsF:     bhost.AddrsFactory,
		static:     static,
		relays:     make(map[peer.ID]*relayInfo),
		disconnect: make(chan struct{}, 1),
		status:     network.ReachabilityUnknown,
	}
	bhost.AddrsFactory = ar.hostAddrs
	bhost.Network().Notify(ar)
	go ar.background(ctx)
	return ar
}

func (ar *AutoRelay) hostAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	return ar.relayAddrs(ar.addrsF(addrs))
}

func (ar *AutoRelay) background(ctx context.Context) {
	subReachability, _ := ar.host.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	defer subReachability.Close()

	// when true, we need to identify push
	push := false
	ticker := time.NewTicker(ReserveRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ar.refreshReservations()
		case ev, ok := <-subReachability.Out():
			if !ok {
				return
			}
			evt, ok := ev.(event.EvtLocalReachabilityChanged)
			if !ok {
				return
			}

			var update bool
			if evt.Reachability == network.ReachabilityPrivate {
				// TODO: this is a long-lived (2.5min task) that should get spun up in a separate thread
				// and canceled if the relay learns the nat is now public.
				update = ar.findRelays(ctx)
			}

			ar.mx.Lock()
			if update || (ar.status != evt.Reachability && evt.Reachability != network.ReachabilityUnknown) {
				push = true
			}
			ar.status = evt.Reachability
			ar.mx.Unlock()
		case <-ar.disconnect:
			push = true
		case <-ctx.Done():
			return
		}

		if push {
			ar.mx.Lock()
			ar.cachedAddrs = nil
			ar.mx.Unlock()
			push = false
			ar.host.SignalAddressChange()
		}
	}
}

func (ar *AutoRelay) refreshReservations() {
	for p, info := range ar.relays {
		if !info.resfreshAt.IsZero() && time.Now().After(info.resfreshAt) {
			ar.mx.Lock()
			// if we're no longer connected to the peer, there's nothing to do here.
			if ar.host.Network().Connectedness(p) != network.Connected {
				ar.mx.Unlock()
				continue
			}
			ar.mx.Unlock()

			// try reserving a slot again -> this is akin to a reservation refresh.
			rsvp, err := circuitv2.Reserve(ar.ctx, ar.host, peer.AddrInfo{ID: p, Addrs: info.addrs})
			if err != nil {
				log.Debugf("error refreshing slot on relay: %s", err.Error())
				continue
			}

			ar.mx.Lock()
			info := &relayInfo{
				addrs: rsvp.Addrs,
			}
			if rsvp.LimitDuration > MinTTLRefresh {
				info.resfreshAt = time.Now().Add(rsvp.LimitDuration / 2)
			}

			ar.relays[p] = info
			ar.host.ConnManager().Protect(p, connMgrTag)
			ar.mx.Unlock()
		}
	}
}

func (ar *AutoRelay) findRelays(ctx context.Context) bool {
	if ar.numRelays() >= DesiredRelays {
		return false
	}

	update := false
	for retry := 0; retry < 5; retry++ {
		if retry > 0 {
			log.Debug("no relays connected; retrying in 30s")
			select {
			case <-time.After(30 * time.Second):
			case <-ctx.Done():
				return update
			}
		}

		update = ar.findRelaysOnce(ctx) || update
		if ar.numRelays() > 0 {
			return update
		}
	}
	return update
}

func (ar *AutoRelay) findRelaysOnce(ctx context.Context) bool {
	pis, err := ar.discoverRelays(ctx)
	if err != nil {
		log.Debugf("error discovering relays: %s", err)
		return false
	}
	log.Debugf("discovered %d relays", len(pis))
	pis = ar.selectRelays(ctx, pis)
	log.Debugf("selected %d relays", len(pis))

	update := false
	for _, pi := range pis {
		update = ar.tryRelay(ctx, pi) || update
		if ar.numRelays() >= DesiredRelays {
			break
		}
	}
	return update
}

func (ar *AutoRelay) numRelays() int {
	ar.mx.Lock()
	defer ar.mx.Unlock()
	return len(ar.relays)
}

// usingRelay returns if we're currently using the given relay.
func (ar *AutoRelay) usingRelay(p peer.ID) bool {
	ar.mx.Lock()
	defer ar.mx.Unlock()
	_, ok := ar.relays[p]
	return ok
}

// addRelay adds the given relay to our set of relays.
// returns true when we add a new relay
func (ar *AutoRelay) tryRelay(ctx context.Context, pi peer.AddrInfo) bool {
	if ar.usingRelay(pi.ID) {
		return false
	}

	if !ar.connect(ctx, pi) {
		return false
	}

	rsvp, err := circuitv2.Reserve(ctx, ar.host, pi)
	if err != nil {
		log.Debugf("error reserving slot on relay: %s", err.Error())
		return false
	}

	ar.mx.Lock()
	defer ar.mx.Unlock()

	// make sure we're still connected.
	if ar.host.Network().Connectedness(pi.ID) != network.Connected {
		return false
	}

	rinfo := &relayInfo{
		addrs: rsvp.Addrs,
	}
	// If it's a limited time Relay, we need to schedule a reservation refresh before the reservation expires.
	// However, avoid scheduling a refresh if the TTL is less than 10 minutes to prevent repeated refreshes.
	if rsvp.LimitDuration > MinTTLRefresh {
		rinfo.resfreshAt = time.Now().Add(rsvp.LimitDuration / 2)
	}
	ar.relays[pi.ID] = rinfo
	ar.host.ConnManager().Protect(pi.ID, connMgrTag)

	return true
}

func (ar *AutoRelay) connect(ctx context.Context, pi peer.AddrInfo) bool {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if len(pi.Addrs) == 0 {
		if ar.router == nil {
			log.Debug("cannot find relay addrs as router is NOT configured")
			return false
		}
		var err error
		pi, err = ar.router.FindPeer(ctx, pi.ID)
		if err != nil {
			log.Debugf("error finding relay peer %s: %s", pi.ID, err.Error())
			return false
		}
	}

	err := ar.host.Connect(ctx, pi)
	if err != nil {
		log.Debugf("error connecting to relay %s: %s", pi.ID, err.Error())
		return false
	}

	// tag the connection as very important
	ar.host.ConnManager().TagPeer(pi.ID, connMgrTag, 42)
	return true
}

func (ar *AutoRelay) discoverRelays(ctx context.Context) ([]peer.AddrInfo, error) {
	if len(ar.static) > 0 {
		return ar.static, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return discovery.FindPeers(ctx, ar.discover, RelayRendezvous, discovery.Limit(1000))
}

func (ar *AutoRelay) selectRelays(ctx context.Context, pis []peer.AddrInfo) []peer.AddrInfo {
	// TODO better relay selection strategy; this just selects random relays
	//      but we should probably use ping latency as the selection metric

	shuffleRelays(pis)
	return pis
}

// This function is computes the NATed relay addrs when our status is private:
// - The public addrs are removed from the address set.
// - The non-public addrs are included verbatim so that peers behind the same NAT/firewall
//   can still dial us directly.
// - On top of those, we add the relay-specific addrs for the relays to which we are
//   connected. For each non-private relay addr, we encapsulate the p2p-circuit addr
//   through which we can be dialed.
func (ar *AutoRelay) relayAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	ar.mx.Lock()
	defer ar.mx.Unlock()

	if ar.status != network.ReachabilityPrivate {
		return addrs
	}

	if ar.cachedAddrs != nil && time.Now().Before(ar.cachedAddrsExpiry) {
		return ar.cachedAddrs
	}

	raddrs := make([]ma.Multiaddr, 0, 4*len(ar.relays)+4)

	// only keep private addrs from the original addr set
	for _, addr := range addrs {
		if manet.IsPrivateAddr(addr) {
			raddrs = append(raddrs, addr)
		}
	}

	// add relay specific addrs to the list
	for _, rinfo := range ar.relays {
		caddrs := cleanupAddressSet(rinfo.addrs)
		raddrs = append(raddrs, caddrs...)
	}

	ar.cachedAddrs = raddrs
	ar.cachedAddrsExpiry = time.Now().Add(30 * time.Second)

	return raddrs
}

func shuffleRelays(pis []peer.AddrInfo) {
	for i := range pis {
		j := rand.Intn(i + 1)
		pis[i], pis[j] = pis[j], pis[i]
	}
}

// Notifee
func (ar *AutoRelay) Listen(network.Network, ma.Multiaddr)      {}
func (ar *AutoRelay) ListenClose(network.Network, ma.Multiaddr) {}
func (ar *AutoRelay) Connected(network.Network, network.Conn)   {}

func (ar *AutoRelay) Disconnected(_ network.Network, c network.Conn) {
	p := c.RemotePeer()

	ar.mx.Lock()
	defer ar.mx.Unlock()

	if ar.host.Network().Connectedness(p) == network.Connected {
		// We have a second connection.
		return
	}

	ar.host.ConnManager().Unprotect(p, connMgrTag)
	if _, ok := ar.relays[p]; ok {
		delete(ar.relays, p)
		select {
		case ar.disconnect <- struct{}{}:
		default:
		}
	}
}

func (ar *AutoRelay) OpenedStream(network.Network, network.Stream) {}
func (ar *AutoRelay) ClosedStream(network.Network, network.Stream) {}
