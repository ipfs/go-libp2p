package network

import (
	"context"
	"fmt"
	"io"

	ic "github.com/libp2p/go-libp2p/core/crypto"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	ma "github.com/multiformats/go-multiaddr"
)

type ConnErrorCode uint32

type ConnError struct {
	Remote         bool
	ErrorCode      ConnErrorCode
	TransportError error
}

func (c *ConnError) Error() string {
	side := "local"
	if c.Remote {
		side = "remote"
	}
	if c.TransportError != nil {
		return fmt.Sprintf("connection closed (%s): code: 0x%x: transport error: %s", side, c.ErrorCode, c.TransportError)
	}
	return fmt.Sprintf("connection closed (%s): code: 0x%x", side, c.ErrorCode)
}

func (c *ConnError) Is(target error) bool {
	if tce, ok := target.(*ConnError); ok {
		return tce.ErrorCode == c.ErrorCode && tce.Remote == c.Remote
	}
	return false
}

func (c *ConnError) Unwrap() []error {
	return []error{ErrReset, c.TransportError}
}

const (
	ConnNoError                   ConnErrorCode = 0
	ConnProtocolNegotiationFailed ConnErrorCode = 0x1000
	ConnResourceLimitExceeded     ConnErrorCode = 0x1001
	ConnRateLimited               ConnErrorCode = 0x1002
	ConnProtocolViolation         ConnErrorCode = 0x1003
	ConnSupplanted                ConnErrorCode = 0x1004
	ConnGarbageCollected          ConnErrorCode = 0x1005
	ConnShutdown                  ConnErrorCode = 0x1006
	ConnGated                     ConnErrorCode = 0x1007
	ConnCodeOutOfRange            ConnErrorCode = 0x1008
)

// Conn is a connection to a remote peer. It multiplexes streams.
// Usually there is no need to use a Conn directly, but it may
// be useful to get information about the peer on the other side:
//
//	stream.Conn().RemotePeer()
type Conn interface {
	io.Closer

	ConnSecurity
	ConnMultiaddrs
	ConnStat
	ConnScoper

	// CloseWithError closes the connection with errCode. The errCode is sent to the
	// peer on a best effort basis. For transports that do not support sending error
	// codes on connection close, the behavior is identical to calling Close.
	CloseWithError(errCode ConnErrorCode) error

	// ID returns an identifier that uniquely identifies this Conn within this
	// host, during this run. Connection IDs may repeat across restarts.
	ID() string

	// NewStream constructs a new Stream over this conn.
	NewStream(context.Context) (Stream, error)

	// GetStreams returns all open streams over this conn.
	GetStreams() []Stream

	// IsClosed returns whether a connection is fully closed, so it can
	// be garbage collected.
	IsClosed() bool

	// As finds the first conn in Conn's wrapped types that matches target, and
	// if one is found, sets target to that conn value and returns true.
	// Otherwise, it returns false. Similar to errors.As.
	//
	// target must be a pointer to the type you are matching against.
	//
	// This is an EXPERIMENTAL API. Getting access to the underlying type can
	// lead to hard to debug issues. For example, if you mutate connection state
	// on the underlying type, hooks that relied on only mutating that state
	// from the wrapped connection would never be called.
	//
	// You very likely do not need to use this method.
	As(target any) bool

	// Context returns a context that is cancelled when the connection is closed.
	// This can be used to clean up resources associated with the connection
	// and to signal early cancellation of work that depends on the connection.
	Context() context.Context
}

// ConnectionState holds information about the connection.
type ConnectionState struct {
	// The stream multiplexer used on this connection (if any). For example: /yamux/1.0.0
	StreamMultiplexer protocol.ID
	// The security protocol used on this connection (if any). For example: /tls/1.0.0
	Security protocol.ID
	// the transport used on this connection. For example: tcp
	Transport string
	// indicates whether StreamMultiplexer was selected using inlined muxer negotiation
	UsedEarlyMuxerNegotiation bool
}

// ConnSecurity is the interface that one can mix into a connection interface to
// give it the security methods.
type ConnSecurity interface {
	// LocalPeer returns our peer ID
	LocalPeer() peer.ID

	// RemotePeer returns the peer ID of the remote peer.
	RemotePeer() peer.ID

	// RemotePublicKey returns the public key of the remote peer.
	RemotePublicKey() ic.PubKey

	// ConnState returns information about the connection state.
	ConnState() ConnectionState
}

// ConnMultiaddrs is an interface mixin for connection types that provide multiaddr
// addresses for the endpoints.
type ConnMultiaddrs interface {
	// LocalMultiaddr returns the local Multiaddr associated
	// with this connection
	LocalMultiaddr() ma.Multiaddr

	// RemoteMultiaddr returns the remote Multiaddr associated
	// with this connection
	RemoteMultiaddr() ma.Multiaddr
}

// ConnStat is an interface mixin for connection types that provide connection statistics.
type ConnStat interface {
	// Stat stores metadata pertaining to this conn.
	Stat() ConnStats
}

// ConnScoper is the interface that one can mix into a connection interface to give it a resource
// management scope
type ConnScoper interface {
	// Scope returns the user view of this connection's resource scope
	Scope() ConnScope
}
