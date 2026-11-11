package swarm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slices"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/transport"

	logging "github.com/libp2p/go-libp2p/gologshim"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

const (
	defaultDialTimeout = 15 * time.Second

	// defaultDialTimeoutLocal is the maximum duration a Dial to local network address
	// is allowed to take.
	// This includes the time between dialing the raw network connection,
	// protocol selection as well the handshake, if applicable.
	defaultDialTimeoutLocal = 5 * time.Second

	defaultNewStreamTimeout = 15 * time.Second
)

var log = logging.Logger("swarm2")

// ErrSwarmClosed is returned when one attempts to operate on a closed swarm.
var ErrSwarmClosed = errors.New("swarm closed")

// ErrAddrFiltered is returned when trying to register a connection to a
// filtered address. You shouldn't see this error unless some underlying
// transport is misbehaving.
var ErrAddrFiltered = errors.New("address filtered")

// ErrDialTimeout is returned when one a dial times out due to the global timeout
var ErrDialTimeout = errors.New("dial timed out")

type Option func(*Swarm) error

// WithConnectionGater sets a connection gater
func WithConnectionGater(gater connmgr.ConnectionGater) Option {
	return func(s *Swarm) error {
		s.gater = gater
		return nil
	}
}

// WithMultiaddrResolver sets a custom multiaddress resolver
func WithMultiaddrResolver(resolver network.MultiaddrDNSResolver) Option {
	return func(s *Swarm) error {
		s.multiaddrResolver = resolver
		return nil
	}
}

// WithMetrics sets a metrics reporter
func WithMetrics(reporter metrics.Reporter) Option {
	return func(s *Swarm) error {
		s.bwc = reporter
		return nil
	}
}

func WithMetricsTracer(t MetricsTracer) Option {
	return func(s *Swarm) error {
		s.metricsTracer = t
		return nil
	}
}

func WithDialTimeout(t time.Duration) Option {
	return func(s *Swarm) error {
		s.dialTimeout = t
		return nil
	}
}

func WithDialTimeoutLocal(t time.Duration) Option {
	return func(s *Swarm) error {
		s.dialTimeoutLocal = t
		return nil
	}
}

func WithResourceManager(m network.ResourceManager) Option {
	return func(s *Swarm) error {
		s.rcmgr = m
		return nil
	}
}

// WithDialRanker configures swarm to use d as the DialRanker
func WithDialRanker(d network.DialRanker) Option {
	return func(s *Swarm) error {
		if d == nil {
			return errors.New("swarm: dial ranker cannot be nil")
		}
		s.dialRanker = d
		return nil
	}
}

// WithUDPBlackHoleSuccessCounter configures swarm to use the provided config for UDP black hole detection
// n is the size of the sliding window used to evaluate black hole state
// min is the minimum number of successes out of n required to not block requests
func WithUDPBlackHoleSuccessCounter(f *BlackHoleSuccessCounter) Option {
	return func(s *Swarm) error {
		s.udpBHF = f
		return nil
	}
}

// WithIPv6BlackHoleSuccessCounter configures swarm to use the provided config for IPv6 black hole detection
// n is the size of the sliding window used to evaluate black hole state
// min is the minimum number of successes out of n required to not block requests
func WithIPv6BlackHoleSuccessCounter(f *BlackHoleSuccessCounter) Option {
	return func(s *Swarm) error {
		s.ipv6BHF = f
		return nil
	}
}

// WithReadOnlyBlackHoleDetector configures the swarm to use the black hole detector in
// read only mode. In Read Only mode dial requests are refused in unknown state and
// no updates to the detector state are made. This is useful for services like AutoNAT that
// care about accurately providing reachability info.
func WithReadOnlyBlackHoleDetector() Option {
	return func(s *Swarm) error {
		s.readOnlyBHD = true
		return nil
	}
}

// Swarm is a connection muxer, allowing connections to other peers to
// be opened and closed, while still using the same Chan for all
// communication. The Chan sends/receives Messages, which note the
// destination or source Peer.
type Swarm struct {
	nextConnID   atomic.Uint64
	nextStreamID atomic.Uint64

	// Close refcount. This allows us to fully wait for the swarm to be torn
	// down before continuing.
	refs sync.WaitGroup

	emitter event.Emitter

	rcmgr network.ResourceManager

	local peer.ID
	peers peerstore.Peerstore

	dialTimeout      time.Duration
	dialTimeoutLocal time.Duration

	conns struct {
		sync.RWMutex
		m map[peer.ID][]*Conn
	}

	listeners struct {
		sync.RWMutex

		ifaceListenAddres []ma.Multiaddr
		cacheEOL          time.Time

		m map[transport.Listener]struct{}
	}

	notifs struct {
		sync.RWMutex
		m map[network.Notifiee]struct{}
	}

	directConnNotifs struct {
		sync.Mutex
		m map[peer.ID][]chan struct{}
	}

	transports struct {
		sync.RWMutex
		m map[int]transport.Transport
	}

	multiaddrResolver network.MultiaddrDNSResolver

	// stream handlers
	streamh atomic.Pointer[network.StreamHandler]

	// dialing helpers
	dsync   *dialSync
	backf   DialBackoff
	limiter *dialLimiter
	gater   connmgr.ConnectionGater

	closeOnce sync.Once
	ctx       context.Context // is canceled when Close is called
	ctxCancel context.CancelFunc

	bwc           metrics.Reporter
	metricsTracer MetricsTracer

	dialRanker network.DialRanker

	connectionEventsEmitter *connectionEventsEmitter
	udpBHF                  *BlackHoleSuccessCounter
	ipv6BHF                 *BlackHoleSuccessCounter
	bhd                     *blackHoleDetector
	readOnlyBHD             bool
}

// NewSwarm constructs a Swarm.
func NewSwarm(local peer.ID, peers peerstore.Peerstore, eventBus event.Bus, opts ...Option) (*Swarm, error) {
	emitter, err := eventBus.Emitter(new(event.EvtPeerConnectednessChanged))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Swarm{
		local:             local,
		peers:             peers,
		emitter:           emitter,
		ctx:               ctx,
		ctxCancel:         cancel,
		dialTimeout:       defaultDialTimeout,
		dialTimeoutLocal:  defaultDialTimeoutLocal,
		multiaddrResolver: ResolverFromMaDNS{madns.DefaultResolver},
		dialRanker:        DefaultDialRanker,

		// A black hole is a binary property. On a network if UDP dials are blocked or there is
		// no IPv6 connectivity, all dials will fail. So a low success rate of 5 out 100 dials
		// is good enough.
		udpBHF:  &BlackHoleSuccessCounter{N: 100, MinSuccesses: 5, Name: "UDP"},
		ipv6BHF: &BlackHoleSuccessCounter{N: 100, MinSuccesses: 5, Name: "IPv6"},
	}

	s.conns.m = make(map[peer.ID][]*Conn)
	s.listeners.m = make(map[transport.Listener]struct{})
	s.transports.m = make(map[int]transport.Transport)
	s.notifs.m = make(map[network.Notifiee]struct{})
	s.directConnNotifs.m = make(map[peer.ID][]chan struct{})
	s.connectionEventsEmitter = newConnectionEventsEmitter(
		s.Connectedness, emitter,
		func(c *Conn) { s.notifyAll(func(f network.Notifiee) { f.Connected(s, c) }) },
		func(c *Conn) { s.notifyAll(func(f network.Notifiee) { f.Disconnected(s, c) }) },
	)

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if s.rcmgr == nil {
		s.rcmgr = &network.NullResourceManager{}
	}

	s.dsync = newDialSync(s.dialWorkerLoop)

	s.limiter = newDialLimiter(s.dialAddr)
	s.backf.init(s.ctx)

	s.bhd = &blackHoleDetector{
		udp:      s.udpBHF,
		ipv6:     s.ipv6BHF,
		mt:       s.metricsTracer,
		readOnly: s.readOnlyBHD,
	}
	return s, nil
}

func (s *Swarm) Close() error {
	s.closeOnce.Do(s.close)
	return nil
}

// Done returns a channel that is closed when the swarm is closed.
func (s *Swarm) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Swarm) close() {
	s.ctxCancel()

	// Prevents new connections and/or listeners from being added to the swarm.
	s.listeners.Lock()
	listeners := s.listeners.m
	s.listeners.m = nil
	s.listeners.Unlock()

	s.conns.Lock()
	conns := s.conns.m
	s.conns.m = nil
	s.conns.Unlock()

	// Lots of goroutines but we might as well do this in parallel. We want to shut down as fast as
	// possible.
	s.refs.Add(len(listeners))
	for l := range listeners {
		go func(l transport.Listener) {
			defer s.refs.Done()
			if err := l.Close(); err != nil && err != transport.ErrListenerClosed {
				log.Error("error when shutting down listener", "err", err)
			}
		}(l)
	}

	for _, cs := range conns {
		for _, c := range cs {
			go func(c *Conn) {
				if err := c.Close(); err != nil {
					log.Error("error when shutting down connection", "err", err)
				}
			}(c)
		}
	}

	// Wait for everything to finish.
	// We must wait for all the connection notifications to complete before
	// closing the events emitter.
	s.refs.Wait()
	s.connectionEventsEmitter.Close()
	s.emitter.Close()

	// Now close out any transports (if necessary). Do this after closing
	// all connections/listeners.
	s.transports.Lock()
	transports := s.transports.m
	s.transports.m = nil
	s.transports.Unlock()

	// Dedup transports that may be listening on multiple protocols
	transportsToClose := make(map[transport.Transport]struct{}, len(transports))
	for _, t := range transports {
		transportsToClose[t] = struct{}{}
	}

	var wg sync.WaitGroup
	for t := range transportsToClose {
		if closer, ok := t.(io.Closer); ok {
			wg.Add(1)
			go func(c io.Closer) {
				defer wg.Done()
				if err := c.Close(); err != nil {
					log.Error("error when closing down transport", "transport_type", fmt.Sprintf("%T", c), "err", err)
				}
			}(closer)
		}
	}
	wg.Wait()
}

func (s *Swarm) addConn(tc transport.CapableConn, dir network.Direction) (*Conn, error) {
	var (
		p    = tc.RemotePeer()
		addr = tc.RemoteMultiaddr()
	)

	// create the Stat object, initializing with the underlying connection Stat if available
	var stat network.ConnStats
	if cs, ok := tc.(network.ConnStat); ok {
		stat = cs.Stat()
	}
	stat.Direction = dir
	stat.Opened = time.Now()
	isLimited := stat.Limited

	// Wrap and register the connection.
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		conn:   tc,
		swarm:  s,
		stat:   stat,
		id:     s.nextConnID.Add(1),
		ctx:    ctx,
		cancel: cancel,
	}

	// we ONLY check upgraded connections here so we can send them a Disconnect message.
	// If we do this in the Upgrader, we will not be able to do this.
	if s.gater != nil {
		if allow, _ := s.gater.InterceptUpgraded(c); !allow {
			err := tc.CloseWithError(network.ConnGated)
			if err != nil {
				log.Warn("failed to close connection with peer and addr", "peer", p, "addr", addr, "err", err)
			}
			return nil, ErrGaterDisallowedConnection
		}
	}

	// Add the public key.
	if pk := tc.RemotePublicKey(); pk != nil {
		s.peers.AddPubKey(p, pk)
	}

	// Clear any backoffs
	s.backf.Clear(p)

	// Finally, add the peer.
	s.conns.Lock()
	// Check if we're still online
	if s.conns.m == nil {
		s.conns.Unlock()
		tc.Close()
		return nil, ErrSwarmClosed
	}

	c.streams.m = make(map[*Stream]struct{})
	s.conns.m[p] = append(s.conns.m[p], c)
	// Add two swarm refs:
	// * One will be decremented after the close notifications fire in Conn.doClose
	// * The other will be decremented when Conn.start exits.
	s.refs.Add(2)
	s.conns.Unlock()

	if !isLimited {
		// Notify goroutines waiting for a direct connection
		// do this before connected events, as there's no reason to stall this
		// notification for the events.
		//
		// Go routines interested in waiting for direct connection first acquire this lock
		// and then acquire s.conns.RLock. Do not acquire this lock before conns.Unlock to
		// prevent deadlock.
		s.directConnNotifs.Lock()
		for _, ch := range s.directConnNotifs.m[p] {
			close(ch)
		}
		delete(s.directConnNotifs.m, p)
		s.directConnNotifs.Unlock()
	}

	// AddConn dispatches PeerConnectednessChanged and Notifiee.Connected before
	// c.start() spawns the AcceptStream loop, so handlers see the conn before
	// any inbound stream arrives.
	s.connectionEventsEmitter.AddConn(c)

	c.start()
	return c, nil
}

// Peerstore returns this swarms internal Peerstore.
func (s *Swarm) Peerstore() peerstore.Peerstore {
	return s.peers
}

// SetStreamHandler assigns the handler for new streams.
func (s *Swarm) SetStreamHandler(handler network.StreamHandler) {
	s.streamh.Store(&handler)
}

// StreamHandler gets the handler for new streams.
func (s *Swarm) StreamHandler() network.StreamHandler {
	handler := s.streamh.Load()
	if handler == nil {
		return nil
	}
	return *handler
}

// NewStream creates a new stream on any available connection to peer, dialing
// if necessary.
// Use network.WithAllowLimitedConn to open a stream over a limited(relayed)
// connection.
func (s *Swarm) NewStream(ctx context.Context, p peer.ID) (network.Stream, error) {
	log.Debug("opening stream to peer", "source_peer", s.local, "destination_peer", p)

	// Algorithm:
	// 1. Find the best connection, otherwise, dial.
	// 2. If the best connection is limited, wait for a direct conn via conn
	//    reversal or hole punching.
	// 3. Try opening a stream.
	// 4. If the underlying connection is, in fact, closed, close the outer
	//    connection and try again. We do this in case we have a closed
	//    connection but don't notice it until we actually try to open a
	//    stream.
	//
	// TODO: Try all connections even if we get an error opening a stream on
	// a non-closed connection.
	numDials := 0
	for {
		c := s.bestConnToPeer(p)
		if c == nil {
			if nodial, _ := network.GetNoDial(ctx); !nodial {
				numDials++
				if numDials > DialAttempts {
					return nil, errors.New("max dial attempts exceeded")
				}
				var err error
				c, err = s.dialPeer(ctx, p)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, network.ErrNoConn
			}
		}

		limitedAllowed, _ := network.GetAllowLimitedConn(ctx)
		if !limitedAllowed && c.Stat().Limited {
			var err error
			c, err = s.waitForDirectConn(ctx, p)
			if err != nil {
				log.Debug("failed to get direct connection to a limited peer", "destination_peer", p, "err", err)
				return nil, err
			}
		}

		str, err := c.NewStream(ctx)
		if err != nil {
			if c.conn.IsClosed() {
				continue
			}
			return nil, err
		}
		return str, nil
	}
}

// waitForDirectConn waits for a direct connection established through hole punching or connection reversal.
func (s *Swarm) waitForDirectConn(ctx context.Context, p peer.ID) (*Conn, error) {
	s.directConnNotifs.Lock()
	c := s.bestConnToPeer(p)
	if c == nil {
		s.directConnNotifs.Unlock()
		return nil, network.ErrNoConn
	} else if !c.Stat().Limited {
		s.directConnNotifs.Unlock()
		return c, nil
	}

	// Wait for limited connection to upgrade to a direct connection either by
	// connection reversal or hole punching.
	ch := make(chan struct{})
	s.directConnNotifs.m[p] = append(s.directConnNotifs.m[p], ch)
	s.directConnNotifs.Unlock()

	// apply the DialPeer timeout
	ctx, cancel := context.WithTimeout(ctx, network.GetDialPeerTimeout(ctx))
	defer cancel()

	// Wait for notification.
	select {
	case <-ctx.Done():
		// Remove ourselves from the notification list
		s.directConnNotifs.Lock()
		defer s.directConnNotifs.Unlock()

		s.directConnNotifs.m[p] = slices.DeleteFunc(
			s.directConnNotifs.m[p],
			func(c chan struct{}) bool { return c == ch },
		)
		if len(s.directConnNotifs.m[p]) == 0 {
			delete(s.directConnNotifs.m, p)
		}
		return nil, ctx.Err()
	case <-ch:
		// We do not need to remove ourselves from the list here as the notifier
		// clears the map entry
		c := s.bestConnToPeer(p)
		if c == nil {
			return nil, network.ErrNoConn
		}
		if c.Stat().Limited {
			return nil, network.ErrLimitedConn
		}
		return c, nil
	}
}

// ConnsToPeer returns all the live connections to peer.
func (s *Swarm) ConnsToPeer(p peer.ID) []network.Conn {
	// TODO: Consider sorting the connection list best to worst. Currently,
	// it's sorted oldest to newest.
	s.conns.RLock()
	defer s.conns.RUnlock()
	conns := s.conns.m[p]
	output := make([]network.Conn, len(conns))
	for i, c := range conns {
		output[i] = c
	}
	return output
}

func isBetterConn(a, b *Conn) bool {
	// If one is limited and not the other, prefer the unlimited connection.
	aLimited := a.Stat().Limited
	bLimited := b.Stat().Limited
	if aLimited != bLimited {
		return !aLimited
	}

	// If one is direct and not the other, prefer the direct connection.
	aDirect := isDirectConn(a)
	bDirect := isDirectConn(b)
	if aDirect != bDirect {
		return aDirect
	}

	// Otherwise, prefer the connection with more open streams.
	a.streams.Lock()
	aLen := len(a.streams.m)
	a.streams.Unlock()

	b.streams.Lock()
	bLen := len(b.streams.m)
	b.streams.Unlock()

	if aLen != bLen {
		return aLen > bLen
	}

	// finally, pick the last connection.
	return true
}

// bestConnToPeer returns the best connection to peer.
func (s *Swarm) bestConnToPeer(p peer.ID) *Conn {
	// TODO: Prefer some transports over others.
	// For now, prefers direct connections over Relayed connections.
	// For tie-breaking, select the newest non-closed connection with the most streams.
	s.conns.RLock()
	defer s.conns.RUnlock()

	var best *Conn
	for _, c := range s.conns.m[p] {
		if c.conn.IsClosed() {
			// We *will* garbage collect this soon anyways.
			continue
		}
		if best == nil || isBetterConn(c, best) {
			best = c
		}
	}
	return best
}

// bestAcceptableConnToPeer returns the best acceptable connection, considering the passed in ctx.
// If network.WithForceDirectDial is used, it only returns a direct connections, ignoring
// any limited (relayed) connections to the peer.
func (s *Swarm) bestAcceptableConnToPeer(ctx context.Context, p peer.ID) *Conn {
	conn := s.bestConnToPeer(p)

	forceDirect, _ := network.GetForceDirectDial(ctx)
	if forceDirect && !isDirectConn(conn) {
		return nil
	}
	return conn
}

func isDirectConn(c *Conn) bool {
	return c != nil && !c.conn.Transport().Proxy()
}

// Connectedness returns our "connectedness" state with the given peer.
//
// To check if we have an open connection, use `s.Connectedness(p) ==
// network.Connected`.
func (s *Swarm) Connectedness(p peer.ID) network.Connectedness {
	s.conns.RLock()
	defer s.conns.RUnlock()

	return s.connectednessUnlocked(p)
}

// connectednessUnlocked returns the connectedness of a peer.
func (s *Swarm) connectednessUnlocked(p peer.ID) network.Connectedness {
	var haveLimited bool
	for _, c := range s.conns.m[p] {
		if c.IsClosed() {
			// These will be garbage collected soon
			continue
		}
		if c.Stat().Limited {
			haveLimited = true
		} else {
			return network.Connected
		}
	}
	if haveLimited {
		return network.Limited
	}
	return network.NotConnected
}

// Conns returns a slice of all connections.
func (s *Swarm) Conns() []network.Conn {
	s.conns.RLock()
	defer s.conns.RUnlock()

	conns := make([]network.Conn, 0, len(s.conns.m))
	for _, cs := range s.conns.m {
		for _, c := range cs {
			conns = append(conns, c)
		}
	}
	return conns
}

// ClosePeer closes all connections to the given peer.
func (s *Swarm) ClosePeer(p peer.ID) error {
	conns := s.ConnsToPeer(p)
	switch len(conns) {
	case 0:
		return nil
	case 1:
		return conns[0].Close()
	default:
		errCh := make(chan error)
		for _, c := range conns {
			go func(c network.Conn) {
				errCh <- c.Close()
			}(c)
		}

		var errs []string
		for range conns {
			err := <-errCh
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("when disconnecting from peer %s: %s", p, strings.Join(errs, ", "))
		}
		return nil
	}
}

// Peers returns a copy of the set of peers swarm is connected to.
func (s *Swarm) Peers() []peer.ID {
	s.conns.RLock()
	defer s.conns.RUnlock()
	peers := make([]peer.ID, 0, len(s.conns.m))
	for p := range s.conns.m {
		peers = append(peers, p)
	}

	return peers
}

// LocalPeer returns the local peer swarm is associated to.
func (s *Swarm) LocalPeer() peer.ID {
	return s.local
}

// Backoff returns the DialBackoff object for this swarm.
func (s *Swarm) Backoff() *DialBackoff {
	return &s.backf
}

// notifyAll sends a signal to all Notifiees
func (s *Swarm) notifyAll(notify func(network.Notifiee)) {
	s.notifs.RLock()
	for f := range s.notifs.m {
		notify(f)
	}
	s.notifs.RUnlock()
}

// Notify signs up Notifiee to receive signals when events happen
func (s *Swarm) Notify(f network.Notifiee) {
	s.notifs.Lock()
	s.notifs.m[f] = struct{}{}
	s.notifs.Unlock()
}

// StopNotify unregisters Notifiee fromr receiving signals
func (s *Swarm) StopNotify(f network.Notifiee) {
	s.notifs.Lock()
	delete(s.notifs.m, f)
	s.notifs.Unlock()
}

func (s *Swarm) removeConn(c *Conn) {
	p := c.RemotePeer()

	s.conns.Lock()
	defer s.conns.Unlock()

	cs := s.conns.m[p]
	for i, ci := range cs {
		if ci == c {
			// NOTE: We're intentionally preserving order.
			// This way, connections to a peer are always
			// sorted oldest to newest.
			copy(cs[i:], cs[i+1:])
			cs[len(cs)-1] = nil
			s.conns.m[p] = cs[:len(cs)-1]
			break
		}
	}
	if len(s.conns.m[p]) == 0 {
		delete(s.conns.m, p)
	}
}

// String returns a string representation of Network.
func (s *Swarm) String() string {
	return fmt.Sprintf("<Swarm %s>", s.LocalPeer())
}

func (s *Swarm) ResourceManager() network.ResourceManager {
	return s.rcmgr
}

// Swarm is a Network.
var (
	_ network.Network            = (*Swarm)(nil)
	_ transport.TransportNetwork = (*Swarm)(nil)
)

type connWithMetrics struct {
	transport.CapableConn
	opened        time.Time
	dir           network.Direction
	metricsTracer MetricsTracer
	once          sync.Once
	closeErr      error
}

func wrapWithMetrics(capableConn transport.CapableConn, metricsTracer MetricsTracer, opened time.Time, dir network.Direction) *connWithMetrics {
	c := &connWithMetrics{CapableConn: capableConn, opened: opened, dir: dir, metricsTracer: metricsTracer}
	c.metricsTracer.OpenedConnection(c.dir, capableConn.RemotePublicKey(), capableConn.ConnState(), capableConn.LocalMultiaddr())
	return c
}

func (c *connWithMetrics) As(target any) bool {
	return c.CapableConn.As(target)
}

func (c *connWithMetrics) completedHandshake() {
	c.metricsTracer.CompletedHandshake(time.Since(c.opened), c.ConnState(), c.LocalMultiaddr())
}

func (c *connWithMetrics) Close() error {
	c.once.Do(func() {
		c.metricsTracer.ClosedConnection(c.dir, time.Since(c.opened), c.ConnState(), c.LocalMultiaddr())
		c.closeErr = c.CapableConn.Close()
	})
	return c.closeErr
}

func (c *connWithMetrics) CloseWithError(errCode network.ConnErrorCode) error {
	c.once.Do(func() {
		c.metricsTracer.ClosedConnection(c.dir, time.Since(c.opened), c.ConnState(), c.LocalMultiaddr())
		c.closeErr = c.CapableConn.CloseWithError(errCode)
	})
	return c.closeErr
}

func (c *connWithMetrics) Stat() network.ConnStats {
	if cs, ok := c.CapableConn.(network.ConnStat); ok {
		return cs.Stat()
	}
	return network.ConnStats{}
}

var _ network.ConnStat = &connWithMetrics{}

type ResolverFromMaDNS struct {
	*madns.Resolver
}

var _ network.MultiaddrDNSResolver = ResolverFromMaDNS{}

func startsWithDNSADDR(m ma.Multiaddr) bool {
	if m == nil {
		return false
	}

	startsWithDNSADDR := false
	// Using ForEach to avoid allocating
	ma.ForEach(m, func(c ma.Component) bool {
		startsWithDNSADDR = c.Protocol().Code == ma.P_DNSADDR
		return false
	})
	return startsWithDNSADDR
}

// ResolveDNSAddr implements MultiaddrDNSResolver
func (r ResolverFromMaDNS) ResolveDNSAddr(ctx context.Context, expectedPeerID peer.ID, maddr ma.Multiaddr, recursionLimit int, outputLimit int) ([]ma.Multiaddr, error) {
	if outputLimit <= 0 {
		return nil, nil
	}
	if recursionLimit <= 0 {
		return []ma.Multiaddr{maddr}, nil
	}
	var resolved, toResolve []ma.Multiaddr
	addrs, err := r.Resolve(ctx, maddr)
	if err != nil {
		return nil, err
	}
	if len(addrs) > outputLimit {
		addrs = addrs[:outputLimit]
	}

	for _, addr := range addrs {
		if startsWithDNSADDR(addr) {
			toResolve = append(toResolve, addr)
		} else {
			resolved = append(resolved, addr)
		}
	}

	for i, addr := range toResolve {
		// Set the nextOutputLimit to:
		//   outputLimit
		//   - len(resolved)          // What we already have resolved
		//   - (len(toResolve) - i)   // How many addresses we have left to resolve
		//   + 1                      // The current address we are resolving
		// This assumes that each DNSADDR address will resolve to at least one multiaddr.
		// This assumption lets us bound the space we reserve for resolving.
		nextOutputLimit := outputLimit - len(resolved) - (len(toResolve) - i) + 1
		resolvedAddrs, err := r.ResolveDNSAddr(ctx, expectedPeerID, addr, recursionLimit-1, nextOutputLimit)
		if err != nil {
			log.Warn("failed to resolve dnsaddr", "addr", addr, "err", err)
			// Dropping this address
			continue
		}
		resolved = append(resolved, resolvedAddrs...)
	}

	if len(resolved) > outputLimit {
		resolved = resolved[:outputLimit]
	}

	// If the address contains a peer id, make sure it matches our expectedPeerID
	if expectedPeerID != "" {
		removeMismatchPeerID := func(a ma.Multiaddr) bool {
			id, err := peer.IDFromP2PAddr(a)
			if err == peer.ErrInvalidAddr {
				// This multiaddr didn't contain a peer id, assume it's for this peer.
				// Handshake will fail later if it's not.
				return false
			} else if err != nil {
				// This multiaddr is invalid, drop it.
				return true
			}

			return id != expectedPeerID
		}
		resolved = slices.DeleteFunc(resolved, removeMismatchPeerID)
	}

	return resolved, nil
}

// ResolveDNSComponent implements MultiaddrDNSResolver
func (r ResolverFromMaDNS) ResolveDNSComponent(ctx context.Context, maddr ma.Multiaddr, outputLimit int) ([]ma.Multiaddr, error) {
	addrs, err := r.Resolve(ctx, maddr)
	if err != nil {
		return nil, err
	}
	if len(addrs) > outputLimit {
		addrs = addrs[:outputLimit]
	}
	return addrs, nil
}

// AddCertHashes adds certificate hashes to relevant transport addresses, if there
// are no certhashes already present on the method. It mutates `listenAddrs`.
// This method is useful for adding certhashes to public addresses discovered
// via identify, nat mapping, or provided by the user.
func (s *Swarm) AddCertHashes(listenAddrs []ma.Multiaddr) []ma.Multiaddr {
	type addCertHasher interface {
		AddCertHashes(m ma.Multiaddr) (ma.Multiaddr, bool)
	}

	for i, addr := range listenAddrs {
		t := s.TransportForListening(addr)
		if t == nil {
			continue
		}
		tpt, ok := t.(addCertHasher)
		if !ok {
			continue
		}
		addrWithCerthash, added := tpt.AddCertHashes(addr)
		if !added {
			log.Warn("Couldn't add certhashes to multiaddr", "addr", addr)
			continue
		}
		listenAddrs[i] = addrWithCerthash
	}
	return listenAddrs
}
