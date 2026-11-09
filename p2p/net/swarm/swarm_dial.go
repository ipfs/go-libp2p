package swarm

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/canonicallog"

	ma "github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
	manet "github.com/multiformats/go-multiaddr/net"
)

// The maximum number of addresses we'll return when resolving all of a peer's
// address
const maximumResolvedAddresses = 100

const maximumDNSADDRRecursion = 4

// Diagram of dial sync:
//
//   many callers of Dial()   synched w.  dials many addrs       results to callers
//  ----------------------\    dialsync    use earliest            /--------------
//  -----------------------\              |----------\           /----------------
//  ------------------------>------------<-------     >---------<-----------------
//  -----------------------|              \----x                 \----------------
//  ----------------------|                \-----x                \---------------
//                                         any may fail          if no addr at end
//                                                             retry dialAttempt x

var (
	// ErrDialBackoff is returned by the backoff code when a given peer has
	// been dialed too frequently
	ErrDialBackoff = errors.New("dial backoff")

	// ErrDialRefusedBlackHole is returned when we are in a black holed environment
	ErrDialRefusedBlackHole = errors.New("dial refused because of black hole")

	// ErrDialToSelf is returned if we attempt to dial our own peer
	ErrDialToSelf = errors.New("dial to self attempted")

	// ErrNoTransport is returned when we don't know a transport for the
	// given multiaddr.
	ErrNoTransport = errors.New("no transport for protocol")

	// ErrAllDialsFailed is returned when connecting to a peer has ultimately failed
	ErrAllDialsFailed = errors.New("all dials failed")

	// ErrNoAddresses is returned when we fail to find any addresses for a
	// peer we're trying to dial.
	ErrNoAddresses = errors.New("no addresses")

	// ErrNoGoodAddresses is returned when we find addresses for a peer but
	// can't use any of them.
	ErrNoGoodAddresses = errors.New("no good addresses")

	// ErrGaterDisallowedConnection is returned when the gater prevents us from
	// forming a connection with a peer.
	ErrGaterDisallowedConnection = errors.New("gater disallows connection to peer")
)

// ErrQUICDraft29 wraps ErrNoTransport and provide a more meaningful error message
var ErrQUICDraft29 errQUICDraft29

type errQUICDraft29 struct{}

func (errQUICDraft29) Error() string {
	return "QUIC draft-29 has been removed, QUIC (RFC 9000) is accessible with /quic-v1"
}

func (errQUICDraft29) Unwrap() error {
	return ErrNoTransport
}

// DialAttempts governs how many times a goroutine will try to dial a given peer.
// Note: this is down to one, as we have _too many dials_ atm. To add back in,
// add loop back in Dial(.)
const DialAttempts = 1

// ConcurrentFdDials is the number of concurrent outbound dials over transports
// that consume file descriptors
const ConcurrentFdDials = 160

// DefaultPerPeerRateLimit is the number of concurrent outbound dials to make
// per peer
var DefaultPerPeerRateLimit = 8

// DialBackoff is a type for tracking peer dial backoffs. Dialbackoff is used to
// avoid over-dialing the same, dead peers. Whenever we totally time out on all
// addresses of a peer, we add the addresses to DialBackoff. Then, whenever we
// attempt to dial the peer again, we check each address for backoff. If it's on
// backoff, we don't dial the address and exit promptly. If a dial is
// successful, the peer and all its addresses are removed from backoff.
//
// * It's safe to use its zero value.
// * It's thread-safe.
// * It's *not* safe to move this type after using.
type DialBackoff struct {
	entries map[peer.ID]map[string]*backoffAddr
	lock    sync.RWMutex
}

type backoffAddr struct {
	tries int
	until time.Time
}

func (db *DialBackoff) init(ctx context.Context) {
	if db.entries == nil {
		db.entries = make(map[peer.ID]map[string]*backoffAddr)
	}
	go db.background(ctx)
}

func (db *DialBackoff) background(ctx context.Context) {
	ticker := time.NewTicker(BackoffMax)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.cleanup()
		}
	}
}

// Backoff returns whether the client should backoff from dialing
// peer p at address addr
func (db *DialBackoff) Backoff(p peer.ID, addr ma.Multiaddr) (backoff bool) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	ap, found := db.entries[p][string(addr.Bytes())]
	return found && time.Now().Before(ap.until)
}

// BackoffBase is the base amount of time to backoff (default: 5s).
var BackoffBase = time.Second * 5

// BackoffCoef is the backoff coefficient (default: 1s).
var BackoffCoef = time.Second

// BackoffMax is the maximum backoff time (default: 5m).
var BackoffMax = time.Minute * 5

// AddBackoff adds peer's address to backoff.
//
// Backoff is not exponential, it's quadratic and computed according to the
// following formula:
//
//	BackoffBase + BakoffCoef * PriorBackoffs^2
//
// Where PriorBackoffs is the number of previous backoffs.
func (db *DialBackoff) AddBackoff(p peer.ID, addr ma.Multiaddr) {
	saddr := string(addr.Bytes())
	db.lock.Lock()
	defer db.lock.Unlock()
	bp, ok := db.entries[p]
	if !ok {
		bp = make(map[string]*backoffAddr, 1)
		db.entries[p] = bp
	}
	ba, ok := bp[saddr]
	if !ok {
		bp[saddr] = &backoffAddr{
			tries: 1,
			until: time.Now().Add(BackoffBase),
		}
		return
	}

	backoffTime := min(BackoffBase+BackoffCoef*time.Duration(ba.tries*ba.tries), BackoffMax)
	ba.until = time.Now().Add(backoffTime)
	ba.tries++
}

// Clear removes a backoff record. Clients should call this after a
// successful Dial.
func (db *DialBackoff) Clear(p peer.ID) {
	db.lock.Lock()
	defer db.lock.Unlock()
	delete(db.entries, p)
}

func (db *DialBackoff) cleanup() {
	db.lock.Lock()
	defer db.lock.Unlock()
	now := time.Now()
	for p, e := range db.entries {
		good := false
		for _, backoff := range e {
			backoffTime := min(BackoffBase+BackoffCoef*time.Duration(backoff.tries*backoff.tries), BackoffMax)
			if now.Before(backoff.until.Add(backoffTime)) {
				good = true
				break
			}
		}
		if !good {
			delete(db.entries, p)
		}
	}
}

// DialPeer connects to a peer. Use network.WithForceDirectDial to force a
// direct connection.
//
// The idea is that the client of Swarm does not need to know what network
// the connection will happen over. Swarm can use whichever it choses.
// This allows us to use various transport protocols, do NAT traversal/relay,
// etc. to achieve connection.
func (s *Swarm) DialPeer(ctx context.Context, p peer.ID) (network.Conn, error) {
	// Avoid typed nil issues.
	c, err := s.dialPeer(ctx, p)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// internal dial method that returns an unwrapped conn
//
// It is gated by the swarm's dial synchronization systems: dialsync and
// dialbackoff.
func (s *Swarm) dialPeer(ctx context.Context, p peer.ID) (*Conn, error) {
	log.Debug("dialing peer", "source_peer", s.local, "destination_peer", p)
	err := p.Validate()
	if err != nil {
		return nil, err
	}

	if p == s.local {
		return nil, ErrDialToSelf
	}

	// check if we already have an open (usable) connection.
	conn := s.bestAcceptableConnToPeer(ctx, p)
	if conn != nil {
		return conn, nil
	}

	if s.gater != nil && !s.gater.InterceptPeerDial(p) {
		log.Debug("gater disallowed outbound connection to peer", "peer", p)
		return nil, &DialError{Peer: p, Cause: ErrGaterDisallowedConnection}
	}

	// apply the DialPeer timeout
	ctx, cancel := context.WithTimeout(ctx, network.GetDialPeerTimeout(ctx))
	defer cancel()

	conn, err = s.dsync.Dial(ctx, p)
	if err == nil {
		// Ensure we connected to the correct peer.
		// This was most likely already checked by the security protocol, but it doesn't hurt do it again here.
		if conn.RemotePeer() != p {
			conn.Close()
			log.Error("Handshake failed to properly authenticate peer", "authenticated", conn.RemotePeer(), "expected", p)
			return nil, fmt.Errorf("unexpected peer")
		}
		return conn, nil
	}

	log.Debug("network finished dialing peer", "local", s.local, "remote", p)

	if ctx.Err() != nil {
		// Context error trumps any dial errors as it was likely the ultimate cause.
		return nil, ctx.Err()
	}

	if s.ctx.Err() != nil {
		// Ok, so the swarm is shutting down.
		return nil, ErrSwarmClosed
	}

	return nil, err
}

// dialWorkerLoop synchronizes and executes concurrent dials to a single peer
func (s *Swarm) dialWorkerLoop(p peer.ID, reqch <-chan dialRequest) {
	w := newDialWorker(s, p, reqch, nil)
	w.loop()
}

func (s *Swarm) addrsForDial(ctx context.Context, p peer.ID) (goodAddrs []ma.Multiaddr, addrErrs []TransportError, err error) {
	peerAddrs := s.peers.Addrs(p)
	if len(peerAddrs) == 0 {
		return nil, nil, ErrNoAddresses
	}

	// Resolve dns or dnsaddrs
	resolved := s.resolveAddrs(ctx, peer.AddrInfo{ID: p, Addrs: peerAddrs})

	goodAddrs = ma.Unique(resolved)
	goodAddrs, addrErrs = s.filterKnownUndialables(p, goodAddrs)
	if forceDirect, _ := network.GetForceDirectDial(ctx); forceDirect {
		goodAddrs = ma.FilterAddrs(goodAddrs, s.nonProxyAddr)
	}

	if len(goodAddrs) == 0 {
		return nil, addrErrs, ErrNoGoodAddresses
	}

	s.peers.AddAddrs(p, goodAddrs, peerstore.TempAddrTTL)

	return goodAddrs, addrErrs, nil
}

func startsWithDNSComponent(m ma.Multiaddr) bool {
	if m == nil {
		return false
	}
	startsWithDNS := false
	// Using ForEach to avoid allocating
	ma.ForEach(m, func(c ma.Component) bool {
		switch c.Protocol().Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
			startsWithDNS = true
		}

		return false
	})
	return startsWithDNS
}

func stripP2PComponent(addrs []ma.Multiaddr) []ma.Multiaddr {
	for i, addr := range addrs {
		if id, _ := peer.IDFromP2PAddr(addr); id != "" {
			addrs[i], _ = ma.SplitLast(addr)
		}
	}
	return addrs
}

type resolver struct {
	canResolve func(ma.Multiaddr) bool
	resolve    func(ctx context.Context, maddr ma.Multiaddr, outputLimit int) ([]ma.Multiaddr, error)
}

type resolveErr struct {
	addr ma.Multiaddr
	err  error
}

func chainResolvers(ctx context.Context, addrs []ma.Multiaddr, outputLimit int, resolvers []resolver) ([]ma.Multiaddr, []resolveErr) {
	nextAddrs := make([]ma.Multiaddr, 0, len(addrs))
	errs := make([]resolveErr, 0)
	for _, r := range resolvers {
		for _, a := range addrs {
			if !r.canResolve(a) {
				nextAddrs = append(nextAddrs, a)
				continue
			}
			if len(nextAddrs) >= outputLimit {
				nextAddrs = nextAddrs[:outputLimit]
				break
			}
			next, err := r.resolve(ctx, a, outputLimit-len(nextAddrs))
			if err != nil {
				errs = append(errs, resolveErr{addr: a, err: err})
				continue
			}
			nextAddrs = append(nextAddrs, next...)
		}
		addrs, nextAddrs = nextAddrs, addrs
		nextAddrs = nextAddrs[:0]
	}
	return addrs, errs
}

// resolveAddrs resolves DNS/DNSADDR components in the given peer's addresses.
// We want to resolve the DNS components to IP addresses becase we want the
// swarm to manage ranking and dialing multiple connections, and a single DNS
// address can resolve to multiple IP addresses.
func (s *Swarm) resolveAddrs(ctx context.Context, pi peer.AddrInfo) []ma.Multiaddr {
	dnsAddrResolver := resolver{
		canResolve: startsWithDNSADDR,
		resolve: func(ctx context.Context, maddr ma.Multiaddr, outputLimit int) ([]ma.Multiaddr, error) {
			return s.multiaddrResolver.ResolveDNSAddr(ctx, pi.ID, maddr, maximumDNSADDRRecursion, outputLimit)
		},
	}

	var skipped []ma.Multiaddr
	skipResolver := resolver{
		canResolve: func(addr ma.Multiaddr) bool {
			tpt := s.TransportForDialing(addr)
			if tpt == nil {
				return false
			}
			_, ok := tpt.(transport.SkipResolver)
			return ok

		},
		resolve: func(ctx context.Context, addr ma.Multiaddr, _ int) ([]ma.Multiaddr, error) {
			tpt := s.TransportForDialing(addr)
			resolver, ok := tpt.(transport.SkipResolver)
			if !ok {
				return []ma.Multiaddr{addr}, nil
			}
			if resolver.SkipResolve(ctx, addr) {
				skipped = append(skipped, addr)
				return nil, nil
			}
			return []ma.Multiaddr{addr}, nil
		},
	}

	tptResolver := resolver{
		canResolve: func(addr ma.Multiaddr) bool {
			tpt := s.TransportForDialing(addr)
			if tpt == nil {
				return false
			}
			_, ok := tpt.(transport.Resolver)
			return ok
		},
		resolve: func(ctx context.Context, addr ma.Multiaddr, outputLimit int) ([]ma.Multiaddr, error) {
			tpt := s.TransportForDialing(addr)
			resolver, ok := tpt.(transport.Resolver)
			if !ok {
				return []ma.Multiaddr{addr}, nil
			}
			addrs, err := resolver.Resolve(ctx, addr)
			if err != nil {
				return nil, err
			}
			if len(addrs) > outputLimit {
				addrs = addrs[:outputLimit]
			}
			return addrs, nil
		},
	}

	dnsResolver := resolver{
		canResolve: startsWithDNSComponent,
		resolve:    s.multiaddrResolver.ResolveDNSComponent,
	}
	addrs, errs := chainResolvers(ctx, pi.Addrs, maximumResolvedAddresses, []resolver{dnsAddrResolver, skipResolver, tptResolver, dnsResolver})
	for _, err := range errs {
		log.Warn("Failed to resolve addr", "addr", err.addr, "err", err.err)
	}
	// Add skipped addresses back to the resolved addresses
	addrs = append(addrs, skipped...)
	return stripP2PComponent(addrs)
}

func (s *Swarm) dialNextAddr(ctx context.Context, p peer.ID, addr ma.Multiaddr, resch chan transport.DialUpdate) error {
	// check the dial backoff
	if forceDirect, _ := network.GetForceDirectDial(ctx); !forceDirect {
		if s.backf.Backoff(p, addr) {
			return ErrDialBackoff
		}
	}

	// start the dial
	s.limitedDial(ctx, p, addr, resch)

	return nil
}

func (s *Swarm) CanDial(p peer.ID, addr ma.Multiaddr) bool {
	dialable, _ := s.filterKnownUndialables(p, []ma.Multiaddr{addr})
	return len(dialable) > 0
}

func (s *Swarm) nonProxyAddr(addr ma.Multiaddr) bool {
	t := s.TransportForDialing(addr)
	if t == nil {
		// No transport can dial this address (for example, the swarm was
		// closed): it is not a candidate for a direct dial.
		return false
	}
	return !t.Proxy()
}

var quicDraft29DialMatcher = mafmt.And(mafmt.IP, mafmt.Base(ma.P_UDP), mafmt.Base(ma.P_QUIC))

// filterKnownUndialables takes a list of multiaddrs, and removes those
// that we definitely don't want to dial: addresses configured to be blocked,
// IPv6 link-local addresses, addresses without a dial-capable transport,
// addresses that we know to be our own, and addresses with a better transport
// available. This is an optimization to avoid wasting time on dials that we
// know are going to fail or for which we have a better alternative.
func (s *Swarm) filterKnownUndialables(p peer.ID, addrs []ma.Multiaddr) (goodAddrs []ma.Multiaddr, addrErrs []TransportError) {
	lisAddrs, _ := s.InterfaceListenAddresses()
	var ourAddrs []ma.Multiaddr
	for _, addr := range lisAddrs {
		// we're only sure about filtering out /ip4 and /ip6 addresses, so far
		ma.ForEach(addr, func(c ma.Component) bool {
			if c.Protocol().Code == ma.P_IP4 || c.Protocol().Code == ma.P_IP6 {
				ourAddrs = append(ourAddrs, addr)
			}
			return false
		})
	}

	addrErrs = make([]TransportError, 0, len(addrs))

	// The order of checking for transport and filtering low priority addrs is important. If we
	// can only dial /webtransport, we don't want to filter /webtransport addresses out because
	// the peer had a /quic-v1 address

	// filter addresses with no transport
	addrs = ma.FilterAddrs(addrs, func(a ma.Multiaddr) bool {
		if s.TransportForDialing(a) == nil {
			e := ErrNoTransport
			// We used to support QUIC draft-29 for a long time.
			// Provide a more useful error when attempting to dial a QUIC draft-29 address.
			if quicDraft29DialMatcher.Matches(a) {
				e = ErrQUICDraft29
			}
			addrErrs = append(addrErrs, TransportError{Address: a, Cause: e})
			return false
		}
		return true
	})

	// filter low priority addresses among the addresses we can dial
	// We don't return an error for these addresses
	addrs = filterLowPriorityAddresses(addrs)

	// remove black holed addrs
	addrs, blackHoledAddrs := s.bhd.FilterAddrs(addrs)
	for _, a := range blackHoledAddrs {
		addrErrs = append(addrErrs, TransportError{Address: a, Cause: ErrDialRefusedBlackHole})
	}

	return ma.FilterAddrs(addrs,
		// Linux and BSD treat an unspecified address when dialing as a localhost address.
		// Windows doesn't support this. We filter all such addresses out because peers
		// listening on unspecified addresses will advertise more specific addresses.
		// https://unix.stackexchange.com/a/419881
		// https://superuser.com/a/1755455
		func(addr ma.Multiaddr) bool {
			return !manet.IsIPUnspecified(addr)
		},
		func(addr ma.Multiaddr) bool {
			if ma.Contains(ourAddrs, addr) {
				addrErrs = append(addrErrs, TransportError{Address: addr, Cause: ErrDialToSelf})
				return false
			}
			return true
		},
		// TODO: Consider allowing link-local addresses
		func(addr ma.Multiaddr) bool { return !manet.IsIP6LinkLocal(addr) },
		func(addr ma.Multiaddr) bool {
			if s.gater != nil && !s.gater.InterceptAddrDial(p, addr) {
				addrErrs = append(addrErrs, TransportError{Address: addr, Cause: ErrGaterDisallowedConnection})
				return false
			}
			return true
		},
	), addrErrs
}

// limitedDial will start a dial to the given peer when
// it is able, respecting the various different types of rate
// limiting that occur without using extra goroutines per addr
func (s *Swarm) limitedDial(ctx context.Context, p peer.ID, a ma.Multiaddr, resp chan transport.DialUpdate) {
	timeout := s.dialTimeout
	if manet.IsPrivateAddr(a) && s.dialTimeoutLocal < s.dialTimeout {
		timeout = s.dialTimeoutLocal
	}
	s.limiter.AddDialJob(&dialJob{
		addr:    a,
		peer:    p,
		resp:    resp,
		ctx:     ctx,
		timeout: timeout,
	})
}

// dialAddr is the actual dial for an addr, indirectly invoked through the limiter
func (s *Swarm) dialAddr(ctx context.Context, p peer.ID, addr ma.Multiaddr, updCh chan<- transport.DialUpdate) (transport.CapableConn, error) {
	// Just to double check. Costs nothing.
	if s.local == p {
		return nil, ErrDialToSelf
	}
	// Check before we start work
	if err := ctx.Err(); err != nil {
		log.Debug("swarm not dialing. Context cancelled", "source_peer", s.local, "err", err, "destination_peer", p, "addr", addr)
		return nil, err
	}
	log.Debug("swarm dialing peer", "source_peer", s.local, "destination_peer", p, "addr", addr)

	tpt := s.TransportForDialing(addr)
	if tpt == nil {
		return nil, ErrNoTransport
	}

	start := time.Now()
	var connC transport.CapableConn
	var err error
	if du, ok := tpt.(transport.DialUpdater); ok {
		connC, err = du.DialWithUpdates(ctx, addr, p, updCh)
	} else {
		connC, err = tpt.Dial(ctx, addr, p)
	}

	// We're recording any error as a failure here.
	// Notably, this also applies to cancellations (i.e. if another dial attempt was faster).
	// This is ok since the black hole detector uses a very low threshold (5%).
	s.bhd.RecordResult(addr, err == nil)

	if err != nil {
		if s.metricsTracer != nil {
			s.metricsTracer.FailedDialing(addr, err, context.Cause(ctx))
		}
		return nil, err
	}
	canonicallog.LogPeerStatus(100, connC.RemotePeer(), connC.RemoteMultiaddr(), "connection_status", "established", "dir", "outbound")
	if s.metricsTracer != nil {
		connWithMetrics := wrapWithMetrics(connC, s.metricsTracer, start, network.DirOutbound)
		connWithMetrics.completedHandshake()
		connC = connWithMetrics
	}

	// Trust the transport? Yeah... right.
	if connC.RemotePeer() != p {
		connC.Close()
		err = fmt.Errorf("BUG in transport %T: tried to dial %s, dialed %s", tpt, p, connC.RemotePeer())
		log.Error("BUG in transport: peer mismatch", "transport_type", fmt.Sprintf("%T", tpt), "expected_peer", p, "observed_peer", connC.RemotePeer())
		return nil, err
	}

	// success! we got one!
	return connC, nil
}

// TODO We should have a `IsFdConsuming() bool` method on the `Transport` interface in go-libp2p/core/transport.
// This function checks if any of the transport protocols in the address requires a file descriptor.
// For now:
// A Non-circuit address which has the TCP/UNIX protocol is deemed FD consuming.
// For a circuit-relay address, we look at the address of the relay server/proxy
// and use the same logic as above to decide.
func isFdConsumingAddr(addr ma.Multiaddr) bool {
	first, _ := ma.SplitFunc(addr, func(c ma.Component) bool {
		return c.Protocol().Code == ma.P_CIRCUIT
	})

	// for safety
	if first == nil {
		return true
	}

	_, err1 := first.ValueForProtocol(ma.P_TCP)
	_, err2 := first.ValueForProtocol(ma.P_UNIX)
	return err1 == nil || err2 == nil
}

func isRelayAddr(addr ma.Multiaddr) bool {
	_, err := addr.ValueForProtocol(ma.P_CIRCUIT)
	return err == nil
}

// filterLowPriorityAddresses removes addresses inplace for which we have a better alternative
//  1. If a /quic-v1 address is present, filter out /quic and /webtransport address on the same 2-tuple:
//     QUIC v1 is preferred over the deprecated QUIC draft-29, and given the choice, we prefer using
//     raw QUIC over using WebTransport.
//  2. If a /tcp address is present, filter out /ws or /wss addresses on the same 2-tuple:
//     We prefer using raw TCP over using WebSocket.
func filterLowPriorityAddresses(addrs []ma.Multiaddr) []ma.Multiaddr {
	// make a map of QUIC v1 and TCP AddrPorts.
	quicV1Addr := make(map[netip.AddrPort]struct{})
	tcpAddr := make(map[netip.AddrPort]struct{})
	for _, a := range addrs {
		switch {
		case isProtocolAddr(a, ma.P_WEBTRANSPORT):
		case isProtocolAddr(a, ma.P_QUIC_V1):
			ap, err := addrPort(a, ma.P_UDP)
			if err != nil {
				continue
			}
			quicV1Addr[ap] = struct{}{}
		case isProtocolAddr(a, ma.P_WS) || isProtocolAddr(a, ma.P_WSS):
		case isProtocolAddr(a, ma.P_TCP):
			ap, err := addrPort(a, ma.P_TCP)
			if err != nil {
				continue
			}
			tcpAddr[ap] = struct{}{}
		}
	}

	i := 0
	for _, a := range addrs {
		switch {
		case isProtocolAddr(a, ma.P_WEBTRANSPORT) || isProtocolAddr(a, ma.P_QUIC):
			ap, err := addrPort(a, ma.P_UDP)
			if err != nil {
				break
			}
			if _, ok := quicV1Addr[ap]; ok {
				continue
			}
		case isProtocolAddr(a, ma.P_WS) || isProtocolAddr(a, ma.P_WSS):
			ap, err := addrPort(a, ma.P_TCP)
			if err != nil {
				break
			}
			if _, ok := tcpAddr[ap]; ok {
				continue
			}
		}
		addrs[i] = a
		i++
	}
	return addrs[:i]
}

// addrPort returns the ip and port for a. p should be either ma.P_TCP or ma.P_UDP.
// a must be an (ip, TCP) or (ip, udp) address.
func addrPort(a ma.Multiaddr, p int) (netip.AddrPort, error) {
	ip, err := manet.ToIP(a)
	if err != nil {
		return netip.AddrPort{}, err
	}
	port, err := a.ValueForProtocol(p)
	if err != nil {
		return netip.AddrPort{}, err
	}
	pi, err := strconv.Atoi(port)
	if err != nil {
		return netip.AddrPort{}, err
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("failed to parse IP %s", ip)
	}
	return netip.AddrPortFrom(addr, uint16(pi)), nil
}
