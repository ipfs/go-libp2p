// Package transport provides the Transport interface, which represents
// the devices and network protocols used to send and receive data.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// A CapableConn represents a connection that has offers the basic
// capabilities required by libp2p: stream multiplexing, encryption and
// peer authentication.
//
// These capabilities may be natively provided by the transport, or they
// may be shimmed via the "connection upgrade" process, which converts a
// "raw" network connection into one that supports such capabilities by
// layering an encryption channel and a stream multiplexer.
//
// CapableConn provides accessors for the local and remote multiaddrs used to
// establish the connection and an accessor for the underlying Transport.
type CapableConn interface {
	network.MuxedConn
	network.ConnSecurity
	network.ConnMultiaddrs
	network.ConnScoper

	// Transport returns the transport to which this connection belongs.
	Transport() Transport
}

// Transport represents any device by which you can connect to and accept
// connections from other peers.
//
// The Transport interface allows you to open connections to other peers
// by dialing them, and also lets you listen for incoming connections.
//
// Connections returned by Dial and passed into Listeners are of type
// CapableConn, which means that they have been upgraded to support
// stream multiplexing and connection security (encryption and authentication).
//
// If a transport implements `io.Closer` (optional), libp2p will call `Close` on
// shutdown. NOTE: `Dial` and `Listen` may be called after or concurrently with
// `Close`.
//
// In addition to the Transport interface, transports may implement
// Resolver or SkipResolver interface. When wrapping/embedding a transport, you should
// ensure that the Resolver/SkipResolver interface is handled correctly.
//
// For a conceptual overview, see https://libp2p.io/docs/transports-overview/
type Transport interface {
	// Dial dials a remote peer. It should try to reuse local listener
	// addresses if possible, but it may choose not to.
	Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (CapableConn, error)

	// CanDial returns true if this transport knows how to dial the given
	// multiaddr.
	//
	// Returning true does not guarantee that dialing this multiaddr will
	// succeed. This function should *only* be used to preemptively filter
	// out addresses that we can't dial.
	CanDial(addr ma.Multiaddr) bool

	// Listen listens on the passed multiaddr.
	Listen(laddr ma.Multiaddr) (Listener, error)

	// Protocol returns the set of protocols handled by this transport.
	//
	// See the Network interface for an explanation of how this is used.
	Protocols() []int

	// Proxy returns true if this is a proxy transport.
	//
	// See the Network interface for an explanation of how this is used.
	// TODO: Make this a part of the go-multiaddr protocol instead?
	Proxy() bool
}

// Resolver can be optionally implemented by transports that want to resolve or transform the
// multiaddr.
type Resolver interface {
	Resolve(ctx context.Context, maddr ma.Multiaddr) ([]ma.Multiaddr, error)
}

// SkipResolver can be optionally implemented by transports that don't want to
// resolve or transform the multiaddr. Useful for transports that indirectly
// wrap other transports (e.g. p2p-circuit). This lets the inner transport
// specify how a multiaddr is resolved later.
type SkipResolver interface {
	SkipResolve(ctx context.Context, maddr ma.Multiaddr) bool
}

// Listener is an interface closely resembling the net.Listener interface. The
// only real difference is that Accept() returns Conn's of the type in this
// package, and also exposes a Multiaddr method as opposed to a regular Addr
// method
type Listener interface {
	Accept() (CapableConn, error)
	Close() error
	Addr() net.Addr
	Multiaddr() ma.Multiaddr
}

// ErrListenerClosed is returned by Listener.Accept when the listener is gracefully closed.
var ErrListenerClosed = errors.New("listener closed")

// TransportNetwork is an inet.Network with methods for managing transports.
type TransportNetwork interface {
	network.Network

	// AddTransport adds a transport to this Network.
	//
	// When dialing, this Network will iterate over the protocols in the
	// remote multiaddr and pick the first protocol registered with a proxy
	// transport, if any. Otherwise, it'll pick the transport registered to
	// handle the last protocol in the multiaddr.
	//
	// When listening, this Network will iterate over the protocols in the
	// local multiaddr and pick the *last* protocol registered with a proxy
	// transport, if any. Otherwise, it'll pick the transport registered to
	// handle the last protocol in the multiaddr.
	AddTransport(t Transport) error
}

// GatedMaListener is listener that listens for raw(unsecured and non-multiplexed) incoming connections,
// gates them with a `connmgr.ConnGater`and creates a resource management scope for them.
// It can be upgraded to a full libp2p transport listener by the Upgrader.
//
// Compared to manet.Listener, this listener creates the resource management scope for the accepted connection.
type GatedMaListener interface {
	// Accept waits for and returns the next connection to the listener.
	Accept() (manet.Conn, network.ConnManagementScope, error)

	// Close closes the listener.
	// Any blocked Accept operations will be unblocked and return errors.
	Close() error

	// Multiaddr returns the listener's (local) Multiaddr.
	Multiaddr() ma.Multiaddr

	// Addr returns the net.Listener's network address.
	Addr() net.Addr
}

// Upgrader is a multistream upgrader that can upgrade an underlying connection
// to a full transport connection (secure and multiplexed).
type Upgrader interface {
	// UpgradeListener upgrades the passed multiaddr-net listener into a full libp2p-transport listener.
	//
	// Deprecated: Use UpgradeGatedMaListener(upgrader.GateMaListener(manet.Listener)) instead.
	UpgradeListener(Transport, manet.Listener) Listener

	// GateMaListener creates a GatedMaListener from a manet.Listener. It gates the accepted connection
	// and creates a resource scope for it.
	GateMaListener(manet.Listener) GatedMaListener

	// UpgradeGatedMaListener upgrades the passed GatedMaListener into a full libp2p-transport listener.
	UpgradeGatedMaListener(Transport, GatedMaListener) Listener

	// Upgrade upgrades the multiaddr/net connection into a full libp2p-transport connection.
	Upgrade(ctx context.Context, t Transport, maconn manet.Conn, dir network.Direction, p peer.ID, scope network.ConnManagementScope) (CapableConn, error)
}

// DialUpdater provides updates on in progress dials.
type DialUpdater interface {
	// DialWithUpdates dials a remote peer and provides updates on the passed channel.
	DialWithUpdates(context.Context, ma.Multiaddr, peer.ID, chan<- DialUpdate) (CapableConn, error)
}

// DialUpdateKind indicates the type of DialUpdate event.
type DialUpdateKind int

const (
	// UpdateKindDialFailed indicates dial failed.
	UpdateKindDialFailed DialUpdateKind = iota
	// UpdateKindDialSuccessful indicates dial succeeded.
	UpdateKindDialSuccessful
	// UpdateKindHandshakeProgressed indicates successful completion of the TCP 3-way
	// handshake
	UpdateKindHandshakeProgressed
)

func (k DialUpdateKind) String() string {
	switch k {
	case UpdateKindDialFailed:
		return "DialFailed"
	case UpdateKindDialSuccessful:
		return "DialSuccessful"
	case UpdateKindHandshakeProgressed:
		return "UpdateKindHandshakeProgressed"
	default:
		return fmt.Sprintf("DialUpdateKind<Unknown-%d>", k)
	}
}

// DialUpdate is used by DialUpdater to provide dial updates.
type DialUpdate struct {
	// Kind is the kind of update event.
	Kind DialUpdateKind
	// Addr is the peer's address.
	Addr ma.Multiaddr
	// Conn is the resulting connection on success.
	Conn CapableConn
	// Err is the reason for dial failure.
	Err error
}
