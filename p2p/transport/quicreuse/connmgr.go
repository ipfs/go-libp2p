// Package quicreuse provides `quicreuse.ConnManager`, which provides functionality
// for reusing QUIC transports for various purposes, like listening & dialing, having
// multiple QUIC listeners on the same address with different ALPNs, and sharing the
// same address with non QUIC transports like WebRTC.

package quicreuse

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/libp2p/go-libp2p/gologshim"
	"github.com/libp2p/go-netroute"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"golang.org/x/time/rate"
)

var log = gologshim.Logger("quicreuse")

type QUICListener interface {
	Accept(ctx context.Context) (*quic.Conn, error)
	Close() error
	Addr() net.Addr
}

var _ QUICListener = &quic.Listener{}

type QUICTransport interface {
	Listen(tlsConf *tls.Config, conf *quic.Config) (QUICListener, error)
	Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *quic.Config) (*quic.Conn, error)
	WriteTo(b []byte, addr net.Addr) (int, error)
	ReadNonQUICPacket(ctx context.Context, b []byte) (int, net.Addr, error)
	io.Closer
}

// ConnManager enables QUIC and WebTransport transports to listen on the same port, reusing
// listen addresses for dialing, and provides a PacketConn for sharing the listen address
// with other protocols like WebRTC.
// Reusing the listen address for dialing helps with address discovery and hole punching. For details
// of the reuse logic see `ListenQUICAndAssociate` and `DialQUIC`.
// If reuseport is disabled using the `DisableReuseport` option, listen addresses are not used for
// dialing.
type ConnManager struct {
	reuseUDP4       *reuse
	reuseUDP6       *reuse
	enableReuseport bool

	listenUDP          listenUDP
	sourceIPSelectorFn func() (SourceIPSelector, error)

	enableMetrics bool
	registerer    prometheus.Registerer

	serverConfig *quic.Config
	clientConfig *quic.Config

	quicListenersMu sync.Mutex
	quicListeners   map[string]quicListenerEntry

	srk         quic.StatelessResetKey
	tokenKey    quic.TokenGeneratorKey
	connContext connContextFunc

	verifySourceAddress func(addr net.Addr) bool

	qlogTracerDir string
}

type quicListenerEntry struct {
	refCount int
	ln       *quicListener
}

func defaultListenUDP(network string, laddr *net.UDPAddr) (net.PacketConn, error) {
	return net.ListenUDP(network, laddr)
}

// newSourceIPSelector wraps a route table, returning a nil SourceIPSelector
// when there is no table to wrap.
//
// The nil has to be an untyped nil so it reaches callers as a nil interface:
// returning a nil *netrouteSourceIPSelector would still satisfy the `router !=
// nil` check in TransportWithAssociationForDial and change nothing.
func newSourceIPSelector(r netroute.Router, err error) (SourceIPSelector, error) {
	if err != nil || r == nil {
		return nil, err
	}
	return &netrouteSourceIPSelector{routes: r}, nil
}

func defaultSourceIPSelectorFn() (SourceIPSelector, error) {
	return newSourceIPSelector(netroute.New())
}

const (
	unverifiedAddressNewConnectionRPS   = 1000
	unverifiedAddressNewConnectionBurst = 1000
)

// NewConnManager returns a new ConnManager
func NewConnManager(statelessResetKey quic.StatelessResetKey, tokenKey quic.TokenGeneratorKey, opts ...Option) (*ConnManager, error) {
	cm := &ConnManager{
		enableReuseport:    true,
		quicListeners:      make(map[string]quicListenerEntry),
		srk:                statelessResetKey,
		tokenKey:           tokenKey,
		registerer:         prometheus.DefaultRegisterer,
		listenUDP:          defaultListenUDP,
		sourceIPSelectorFn: defaultSourceIPSelectorFn,
	}
	for _, o := range opts {
		if err := o(cm); err != nil {
			return nil, err
		}
	}

	quicConf := quicConfig.Clone()
	// quic-go takes care of disabling this if QLOGDIR Environment variable isn't present
	quicConf.Tracer = qlog.DefaultConnectionTracer
	serverConfig := quicConf.Clone()

	cm.clientConfig = quicConf
	cm.serverConfig = serverConfig

	// Verify source addresses when under high load.
	// This is ensures that the number of spoofed/unverified addresses that are passed to downstream rate limiters
	// are limited, which enables IP address based rate limiting.
	sourceAddrRateLimiter := rate.NewLimiter(unverifiedAddressNewConnectionRPS, unverifiedAddressNewConnectionBurst)
	vsa := cm.verifySourceAddress
	cm.verifySourceAddress = func(addr net.Addr) bool {
		if sourceAddrRateLimiter.Allow() {
			if vsa != nil {
				return vsa(addr)
			}
			return false
		}
		return true
	}
	if cm.enableReuseport {
		cm.reuseUDP4 = newReuse(&statelessResetKey, &tokenKey, cm.listenUDP, cm.sourceIPSelectorFn, cm.connContext, cm.verifySourceAddress)
		cm.reuseUDP6 = newReuse(&statelessResetKey, &tokenKey, cm.listenUDP, cm.sourceIPSelectorFn, cm.connContext, cm.verifySourceAddress)
	}
	return cm, nil
}

func (c *ConnManager) getReuse(network string) (*reuse, error) {
	switch network {
	case "udp4":
		return c.reuseUDP4, nil
	case "udp6":
		return c.reuseUDP6, nil
	default:
		return nil, errors.New("invalid network: must be either udp4 or udp6")
	}
}

// LendTransport is an advanced method used to lend an existing QUICTransport
// to the ConnManager. The ConnManager will close the returned channel when it
// is done with the transport, so that the owner may safely close the transport.
func (c *ConnManager) LendTransport(network string, tr QUICTransport, conn net.PacketConn) (<-chan struct{}, error) {
	c.quicListenersMu.Lock()
	defer c.quicListenersMu.Unlock()

	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, errors.New("expected a conn.LocalAddr() to return a *net.UDPAddr")
	}

	refCountedTr := &refcountedTransport{
		QUICTransport:    tr,
		packetConn:       conn,
		borrowDoneSignal: make(chan struct{}),
	}

	var reuse *reuse
	reuse, err := c.getReuse(network)
	if err != nil {
		return nil, err
	}
	return refCountedTr.borrowDoneSignal, reuse.AddTransport(refCountedTr, localAddr)
}

// ListenQUIC listens for quic connections with the provided `tlsConf.NextProtos` ALPNs on `addr`. The same addr can be shared between
// different ALPNs.
func (c *ConnManager) ListenQUIC(addr ma.Multiaddr, tlsConf *tls.Config, allowWindowIncrease func(conn *quic.Conn, delta uint64) bool) (Listener, error) {
	return c.ListenQUICAndAssociate(nil, addr, tlsConf, allowWindowIncrease)
}

// ListenQUICAndAssociate listens for quic connections with the provided `tlsConf.NextProtos` ALPNs on `addr`. The same addr can be shared between
// different ALPNs.
// The QUIC Transport used for listening is tagged with the `association`. Any subsequent `TransportWithAssociationForDial`,
// or `DialQUIC` calls with the same `association` will reuse the QUIC Transport used by this method.
// A common use of associations is to ensure /quic dials use the quic listening address and /webtransport dials use the
// WebTransport listening address.
func (c *ConnManager) ListenQUICAndAssociate(association any, addr ma.Multiaddr, tlsConf *tls.Config, allowWindowIncrease func(conn *quic.Conn, delta uint64) bool) (Listener, error) {
	netw, host, err := manet.DialArgs(addr)
	if err != nil {
		return nil, err
	}
	laddr, err := net.ResolveUDPAddr(netw, host)
	if err != nil {
		return nil, err
	}

	c.quicListenersMu.Lock()
	defer c.quicListenersMu.Unlock()

	key := laddr.String()
	entry, ok := c.quicListeners[key]
	if !ok {
		tr, err := c.transportForListen(netw, laddr)
		if err != nil {
			return nil, err
		}
		ln, err := newQuicListener(tr, c.serverConfig)
		if err != nil {
			return nil, err
		}
		key = tr.LocalAddr().String()
		entry = quicListenerEntry{ln: ln}
	}
	if c.enableReuseport && association != nil {
		if _, ok := entry.ln.transport.(*refcountedTransport); !ok {
			log.Warn("reuseport is enabled, association is non-nil, but the transport is not a refcountedTransport.")
		}
	}
	l, err := entry.ln.Add(association, tlsConf, allowWindowIncrease, func() {
		c.onListenerClosed(key)
	})
	if err != nil {
		if entry.refCount <= 0 {
			entry.ln.Close()
		}
		return nil, err
	}
	entry.refCount++
	c.quicListeners[key] = entry
	return l, nil
}

func (c *ConnManager) onListenerClosed(key string) {
	c.quicListenersMu.Lock()
	defer c.quicListenersMu.Unlock()

	entry := c.quicListeners[key]
	entry.refCount = entry.refCount - 1
	if entry.refCount <= 0 {
		delete(c.quicListeners, key)
		entry.ln.Close()
	} else {
		c.quicListeners[key] = entry
	}
}

// SharedNonQUICPacketConn returns a `net.PacketConn` for `laddr` for non QUIC uses.
func (c *ConnManager) SharedNonQUICPacketConn(_ string, laddr *net.UDPAddr) (net.PacketConn, error) {
	c.quicListenersMu.Lock()
	defer c.quicListenersMu.Unlock()
	key := laddr.String()
	entry, ok := c.quicListeners[key]
	if !ok {
		return nil, errors.New("expected to be able to share with a QUIC listener, but no QUIC listener found. The QUIC listener should start first")
	}
	t := entry.ln.transport
	if t, ok := t.(*refcountedTransport); ok {
		t.IncreaseCount()
		ctx, cancel := context.WithCancel(context.Background())
		return &nonQUICPacketConn{
			ctx:             ctx,
			ctxCancel:       cancel,
			owningTransport: t,
			tr:              t.QUICTransport,
		}, nil
	}
	return nil, errors.New("expected to be able to share with a QUIC listener, but the QUIC listener is not using a refcountedTransport. `DisableReuseport` should not be set")
}

func (c *ConnManager) transportForListen(network string, laddr *net.UDPAddr) (RefCountedQUICTransport, error) {
	if c.enableReuseport {
		reuse, err := c.getReuse(network)
		if err != nil {
			return nil, err
		}
		tr, err := reuse.TransportForListen(network, laddr)
		if err != nil {
			return nil, err
		}
		return tr, nil
	}

	conn, err := c.listenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	return c.newSingleOwnerTransport(conn), nil
}

type associationKey struct{}

// WithAssociation returns a new context with the given association. Used in
// DialQUIC to prefer a transport that has the given association.
func WithAssociation(ctx context.Context, association any) context.Context {
	return context.WithValue(ctx, associationKey{}, association)
}

// DialQUIC dials `raddr`. Use `WithAssociation` to select a specific transport that was previously used for listening.
// see the documentation for `ListenQUICAndAssociate` for details on associate.
// The priority order for reusing the transport is as follows:
// - Listening transport with the same association
// - Any other listening transport
// - Any transport previously used for dialing
// If none of these are available, it'll create a new transport.
func (c *ConnManager) DialQUIC(ctx context.Context, raddr ma.Multiaddr, tlsConf *tls.Config, allowWindowIncrease func(conn *quic.Conn, delta uint64) bool) (*quic.Conn, error) {
	naddr, v, err := FromQuicMultiaddr(raddr)
	if err != nil {
		return nil, err
	}
	netw, _, err := manet.DialArgs(raddr)
	if err != nil {
		return nil, err
	}

	quicConf := c.clientConfig.Clone()
	quicConf.AllowConnectionWindowIncrease = allowWindowIncrease

	if v == quic.Version1 {
		// The endpoint has explicit support for QUIC v1, so we'll only use that version.
		quicConf.Versions = []quic.Version{quic.Version1}
	} else {
		return nil, errors.New("unknown QUIC version")
	}

	var tr RefCountedQUICTransport
	association := ctx.Value(associationKey{})
	tr, err = c.TransportWithAssociationForDial(association, netw, naddr)
	if err != nil {
		return nil, err
	}
	conn, err := tr.Dial(ctx, naddr, tlsConf, quicConf)
	if err != nil {
		tr.DecreaseCount()
		return nil, err
	}
	return conn, nil
}

// TransportForDial returns a transport for dialing `raddr`.
// If reuseport is enabled, it attempts to reuse the QUIC Transport used for
// previous listens or dials.
func (c *ConnManager) TransportForDial(network string, raddr *net.UDPAddr) (RefCountedQUICTransport, error) {
	return c.TransportWithAssociationForDial(nil, network, raddr)
}

// TransportWithAssociationForDial returns a transport for dialing `raddr`.
// If reuseport is enabled, it attempts to reuse the QUIC Transport previously used for listening with `ListenQuicAndAssociate`
// with the same `association`. If it fails to do so, it uses any other previously used transport.
func (c *ConnManager) TransportWithAssociationForDial(association any, network string, raddr *net.UDPAddr) (RefCountedQUICTransport, error) {
	if c.enableReuseport {
		reuse, err := c.getReuse(network)
		if err != nil {
			return nil, err
		}
		return reuse.TransportWithAssociationForDial(association, network, raddr)
	}

	var laddr *net.UDPAddr
	switch network {
	case "udp4":
		laddr = &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	case "udp6":
		laddr = &net.UDPAddr{IP: net.IPv6zero, Port: 0}
	}
	conn, err := c.listenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	return c.newSingleOwnerTransport(conn), nil
}

func (c *ConnManager) newSingleOwnerTransport(conn net.PacketConn) *singleOwnerTransport {
	return &singleOwnerTransport{
		Transport: &wrappedQUICTransport{
			Transport: newQUICTransport(
				conn,
				&c.tokenKey,
				&c.srk,
				c.connContext,
				c.verifySourceAddress,
			),
		},
		packetConn: conn}
}

// Protocols returns the supported QUIC protocols. The only supported protocol at the moment is /quic-v1.
func (c *ConnManager) Protocols() []int {
	return []int{ma.P_QUIC_V1}
}

func (c *ConnManager) Close() error {
	if !c.enableReuseport {
		return nil
	}
	if err := c.reuseUDP6.Close(); err != nil {
		return err
	}
	return c.reuseUDP4.Close()
}

func (c *ConnManager) ClientConfig() *quic.Config {
	return c.clientConfig
}

// wrappedQUICTransport wraps a `quic.Transport` to confirm to `QUICTransport`
type wrappedQUICTransport struct {
	*quic.Transport
}

var _ QUICTransport = (*wrappedQUICTransport)(nil)

func (t *wrappedQUICTransport) Listen(tlsConf *tls.Config, conf *quic.Config) (QUICListener, error) {
	return t.Transport.Listen(tlsConf, conf)
}

func newQUICTransport(
	conn net.PacketConn,
	tokenGeneratorKey *quic.TokenGeneratorKey,
	statelessResetKey *quic.StatelessResetKey,
	connContext connContextFunc,
	verifySourceAddress func(addr net.Addr) bool,
) *quic.Transport {
	return &quic.Transport{
		Conn:                conn,
		TokenGeneratorKey:   tokenGeneratorKey,
		StatelessResetKey:   statelessResetKey,
		ConnContext:         connContext,
		VerifySourceAddress: verifySourceAddress,
	}
}
