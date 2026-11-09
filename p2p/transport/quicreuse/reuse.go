package quicreuse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-netroute"
	"github.com/quic-go/quic-go"
)

type RefCountedQUICTransport interface {
	LocalAddr() net.Addr

	// Used to send packets directly around QUIC. Useful for hole punching.
	WriteTo([]byte, net.Addr) (int, error)

	Close() error

	// count transport reference
	DecreaseCount()
	IncreaseCount()

	Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *quic.Config) (*quic.Conn, error)
	Listen(tlsConf *tls.Config, conf *quic.Config) (QUICListener, error)
}

type singleOwnerTransport struct {
	Transport QUICTransport

	// Used to write packets directly around QUIC.
	packetConn net.PacketConn
}

var _ QUICTransport = &singleOwnerTransport{}
var _ RefCountedQUICTransport = (*singleOwnerTransport)(nil)

func (c *singleOwnerTransport) IncreaseCount() {}
func (c *singleOwnerTransport) DecreaseCount() { c.Transport.Close() }
func (c *singleOwnerTransport) LocalAddr() net.Addr {
	return c.packetConn.LocalAddr()
}

func (c *singleOwnerTransport) Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *quic.Config) (*quic.Conn, error) {
	return c.Transport.Dial(ctx, addr, tlsConf, conf)
}

func (c *singleOwnerTransport) ReadNonQUICPacket(ctx context.Context, b []byte) (int, net.Addr, error) {
	return c.Transport.ReadNonQUICPacket(ctx, b)
}

func (c *singleOwnerTransport) Close() error {
	return errors.Join(c.Transport.Close(), c.packetConn.Close())
}

func (c *singleOwnerTransport) WriteTo(b []byte, addr net.Addr) (int, error) {
	return c.Transport.WriteTo(b, addr)
}

func (c *singleOwnerTransport) Listen(tlsConf *tls.Config, conf *quic.Config) (QUICListener, error) {
	return c.Transport.Listen(tlsConf, conf)
}

// Constant. Defined as variables to simplify testing.
var (
	garbageCollectInterval = 30 * time.Second
	maxUnusedDuration      = 10 * time.Second
)

type refcountedTransport struct {
	QUICTransport

	// Used to write packets directly around QUIC.
	packetConn net.PacketConn

	mutex       sync.Mutex
	refCount    int
	unusedSince time.Time

	// Only set for transports we are borrowing.
	// If set, we will _never_ close the underlying transport. We only close this
	// channel to signal to the owner that we are done with it.
	borrowDoneSignal chan struct{}

	// Store associations as association -> set of listener objects
	associations map[any]map[*listener]struct{}
}

type connContextFunc = func(context.Context, *quic.ClientInfo) (context.Context, error)

// associateForListener associates an arbitrary value with this transport for a specific listener.
// This lets us "tag" the refcountedTransport when listening so we can use it
// later for dialing. The listener parameter allows proper cleanup when the listener closes.
// Necessary for holepunching and learning about our own observed listening address.
func (c *refcountedTransport) associateForListener(a any, ln *listener) {
	if a == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.associations == nil {
		c.associations = make(map[any]map[*listener]struct{})
	}
	if c.associations[a] == nil {
		c.associations[a] = make(map[*listener]struct{})
	}
	c.associations[a][ln] = struct{}{}
}

// RemoveAssociationsForListener removes ALL associations added by a specific listener
func (c *refcountedTransport) RemoveAssociationsForListener(ln *listener) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Remove this listener from all associations
	for association, listeners := range c.associations {
		delete(listeners, ln)
		// If no listeners remain for this association, remove the association entirely
		if len(listeners) == 0 {
			delete(c.associations, association)
		}
	}
}

// hasAssociation returns true if the transport has the given association.
// If it is a nil association, it will always return true.
func (c *refcountedTransport) hasAssociation(a any) bool {
	if a == nil {
		return true
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	listeners, ok := c.associations[a]
	return ok && len(listeners) > 0
}

func (c *refcountedTransport) IncreaseCount() {
	c.mutex.Lock()
	c.refCount++
	c.unusedSince = time.Time{}
	c.mutex.Unlock()
}

func (c *refcountedTransport) Close() error {
	if c.borrowDoneSignal != nil {
		close(c.borrowDoneSignal)
		return nil
	}

	return errors.Join(c.QUICTransport.Close(), c.packetConn.Close())
}

func (c *refcountedTransport) WriteTo(b []byte, addr net.Addr) (int, error) {
	return c.QUICTransport.WriteTo(b, addr)
}

func (c *refcountedTransport) LocalAddr() net.Addr {
	return c.packetConn.LocalAddr()
}

func (c *refcountedTransport) Listen(tlsConf *tls.Config, conf *quic.Config) (QUICListener, error) {
	return c.QUICTransport.Listen(tlsConf, conf)
}

func (c *refcountedTransport) DecreaseCount() {
	c.mutex.Lock()
	c.refCount--
	if c.refCount == 0 {
		c.unusedSince = time.Now()
	}
	c.mutex.Unlock()
}

func (c *refcountedTransport) ShouldGarbageCollect(now time.Time) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return !c.unusedSince.IsZero() && c.unusedSince.Add(maxUnusedDuration).Before(now)
}

type reuse struct {
	mutex sync.Mutex

	closeChan  chan struct{}
	gcStopChan chan struct{}

	listenUDP listenUDP

	sourceIPSelectorFn func() (SourceIPSelector, error)

	routes  SourceIPSelector
	unicast map[string] /* IP.String() */ map[int] /* port */ *refcountedTransport
	// globalListeners contains transports that are listening on 0.0.0.0 / ::
	globalListeners map[int]*refcountedTransport
	// globalDialers contains transports that we've dialed out from. These transports are listening on 0.0.0.0 / ::
	// On Dial, transports are reused from this map if no transport is available in the globalListeners
	// On Listen, transports are reused from this map if the requested port is 0, and then moved to globalListeners
	globalDialers map[int]*refcountedTransport

	statelessResetKey   *quic.StatelessResetKey
	tokenGeneratorKey   *quic.TokenGeneratorKey
	connContext         connContextFunc
	verifySourceAddress func(addr net.Addr) bool
}

func newReuse(srk *quic.StatelessResetKey, tokenKey *quic.TokenGeneratorKey, listenUDP listenUDP, sourceIPSelectorFn func() (SourceIPSelector, error),
	connContext connContextFunc, verifySourceAddress func(addr net.Addr) bool) *reuse {
	r := &reuse{
		unicast:             make(map[string]map[int]*refcountedTransport),
		globalListeners:     make(map[int]*refcountedTransport),
		globalDialers:       make(map[int]*refcountedTransport),
		closeChan:           make(chan struct{}),
		gcStopChan:          make(chan struct{}),
		listenUDP:           listenUDP,
		sourceIPSelectorFn:  sourceIPSelectorFn,
		statelessResetKey:   srk,
		tokenGeneratorKey:   tokenKey,
		connContext:         connContext,
		verifySourceAddress: verifySourceAddress,
	}
	go r.gc()
	return r
}

func (r *reuse) gc() {
	defer func() {
		r.mutex.Lock()
		for _, tr := range r.globalListeners {
			tr.Close()
		}
		for _, tr := range r.globalDialers {
			tr.Close()
		}
		for _, trs := range r.unicast {
			for _, tr := range trs {
				tr.Close()
			}
		}
		r.mutex.Unlock()
		close(r.gcStopChan)
	}()
	ticker := time.NewTicker(garbageCollectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.closeChan:
			return
		case <-ticker.C:
			now := time.Now()
			r.mutex.Lock()
			for key, tr := range r.globalListeners {
				if tr.ShouldGarbageCollect(now) {
					tr.Close()
					delete(r.globalListeners, key)
				}
			}
			for key, tr := range r.globalDialers {
				if tr.ShouldGarbageCollect(now) {
					tr.Close()
					delete(r.globalDialers, key)
				}
			}
			for ukey, trs := range r.unicast {
				for key, tr := range trs {
					if tr.ShouldGarbageCollect(now) {
						tr.Close()
						delete(trs, key)
					}
				}
				if len(trs) == 0 {
					delete(r.unicast, ukey)
					// If we've dropped all transports with a unicast binding,
					// assume our routes may have changed.
					if len(r.unicast) == 0 {
						r.routes = nil
					} else {
						// Ignore the error, there's nothing we can do about
						// it.
						r.routes, _ = r.sourceIPSelectorFn()
					}
				}
			}
			r.mutex.Unlock()
		}
	}
}

func (r *reuse) TransportWithAssociationForDial(association any, network string, raddr *net.UDPAddr) (*refcountedTransport, error) {
	var ip *net.IP

	// Only bother looking up the source address if we actually _have_ non 0.0.0.0 listeners.
	// Otherwise, save some time.

	r.mutex.Lock()
	router := r.routes
	r.mutex.Unlock()

	if router != nil {
		src, err := router.PreferredSourceIPForDestination(raddr)
		if err == nil && !src.IsUnspecified() {
			ip = &src
		}
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	tr, err := r.transportForDialLocked(association, network, ip)
	if err != nil {
		return nil, err
	}
	tr.IncreaseCount()
	return tr, nil
}

func (r *reuse) transportForDialLocked(association any, network string, source *net.IP) (*refcountedTransport, error) {
	if source != nil {
		// We already have at least one suitable transport...
		if trs, ok := r.unicast[source.String()]; ok {
			// Prefer a transport that has the given association. We want to
			// reuse the transport the association used for listening.
			for _, tr := range trs {
				if tr.hasAssociation(association) {
					return tr, nil
				}
			}
			// We don't have a transport with the association, use any one
			for _, tr := range trs {
				return tr, nil
			}
		}
	}

	// Use a transport listening on 0.0.0.0 (or ::).
	// Again, prefer a transport that has the given association.
	for _, tr := range r.globalListeners {
		if tr.hasAssociation(association) {
			return tr, nil
		}
	}
	// We don't have a transport with the association, use any one
	for _, tr := range r.globalListeners {
		return tr, nil
	}

	// Use a transport we've previously dialed from
	for _, tr := range r.globalDialers {
		return tr, nil
	}

	// We don't have a transport that we can use for dialing.
	// Dial a new connection from a random port.
	var addr *net.UDPAddr
	switch network {
	case "udp4":
		addr = &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	case "udp6":
		addr = &net.UDPAddr{IP: net.IPv6zero, Port: 0}
	}
	conn, err := r.listenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	tr := r.newTransport(conn)
	r.globalDialers[conn.LocalAddr().(*net.UDPAddr).Port] = tr
	return tr, nil
}

func (r *reuse) AddTransport(tr *refcountedTransport, laddr *net.UDPAddr) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !laddr.IP.IsUnspecified() {
		return errors.New("adding transport for specific IP not supported")
	}
	if _, ok := r.globalDialers[laddr.Port]; ok {
		return fmt.Errorf("already have global dialer for port %d", laddr.Port)
	}
	r.globalDialers[laddr.Port] = tr
	return nil
}

func (r *reuse) TransportForListen(network string, laddr *net.UDPAddr) (*refcountedTransport, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if we can reuse a transport we have already dialed out from.
	// We reuse a transport from globalDialers when the requested port is 0 or the requested
	// port is already in the globalDialers.
	// If we are reusing a transport from globalDialers, we move the globalDialers entry to
	// globalListeners
	if laddr.IP.IsUnspecified() {
		var rTr *refcountedTransport
		var localAddr *net.UDPAddr

		if laddr.Port == 0 {
			// the requested port is 0, we can reuse any transport
			for _, tr := range r.globalDialers {
				rTr = tr
				localAddr = rTr.LocalAddr().(*net.UDPAddr)
				delete(r.globalDialers, localAddr.Port)
				break
			}
		} else if _, ok := r.globalDialers[laddr.Port]; ok {
			rTr = r.globalDialers[laddr.Port]
			localAddr = rTr.LocalAddr().(*net.UDPAddr)
			delete(r.globalDialers, localAddr.Port)
		}
		// found a match
		if rTr != nil {
			rTr.IncreaseCount()
			r.globalListeners[localAddr.Port] = rTr
			return rTr, nil
		}
	}

	conn, err := r.listenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	tr := r.newTransport(conn)
	tr.IncreaseCount()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	// Deal with listen on a global address
	if localAddr.IP.IsUnspecified() {
		// The kernel already checked that the laddr is not already listen
		// so we need not check here (when we create ListenUDP).
		r.globalListeners[localAddr.Port] = tr
		return tr, nil
	}

	// Deal with listen on a unicast address
	if _, ok := r.unicast[localAddr.IP.String()]; !ok {
		r.unicast[localAddr.IP.String()] = make(map[int]*refcountedTransport)
		// Assume the system's routes may have changed if we're adding a new listener.
		// Ignore the error, there's nothing we can do.
		r.routes, _ = r.sourceIPSelectorFn()
	}

	// The kernel already checked that the laddr is not already listen
	// so we need not check here (when we create ListenUDP).
	r.unicast[localAddr.IP.String()][localAddr.Port] = tr
	return tr, nil
}

func (r *reuse) newTransport(conn net.PacketConn) *refcountedTransport {
	return &refcountedTransport{
		QUICTransport: &wrappedQUICTransport{
			Transport: newQUICTransport(
				conn,
				r.tokenGeneratorKey,
				r.statelessResetKey,
				r.connContext,
				r.verifySourceAddress,
			),
		},
		packetConn: conn,
	}
}

func (r *reuse) Close() error {
	close(r.closeChan)
	<-r.gcStopChan
	return nil
}

type SourceIPSelector interface {
	PreferredSourceIPForDestination(dst *net.UDPAddr) (net.IP, error)
}

type netrouteSourceIPSelector struct {
	routes netroute.Router
}

func (s *netrouteSourceIPSelector) PreferredSourceIPForDestination(dst *net.UDPAddr) (net.IP, error) {
	// newSourceIPSelector will not build one of these without a route table, so
	// this only guards a selector constructed directly. Cheap, and the
	// alternative is a panic on the dial path.
	if s == nil || s.routes == nil {
		return nil, errors.New("quicreuse: no route table available")
	}
	_, _, src, err := s.routes.Route(dst.IP)
	return src, err
}
