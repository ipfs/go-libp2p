package swarm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"

	ma "github.com/multiformats/go-multiaddr"
)

// TODO: Put this elsewhere.

// ErrConnClosed is returned when operating on a closed connection.
var ErrConnClosed = errors.New("connection closed")

// Conn is the connection type used by swarm. In general, you won't use this
// type directly.
type Conn struct {
	id    uint64
	conn  transport.CapableConn
	swarm *Swarm

	closeOnce sync.Once
	err       error

	streams struct {
		sync.Mutex
		m map[*Stream]struct{}
	}

	stat network.ConnStats

	// Context and cancel function for connection lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
}

var _ network.Conn = &Conn{}

func (c *Conn) As(target any) bool {
	return c.conn.As(target)
}

func (c *Conn) IsClosed() bool {
	return c.conn.IsClosed()
}

func (c *Conn) ID() string {
	// format: <first 10 chars of peer id>-<global conn ordinal>
	return fmt.Sprintf("%s-%d", c.RemotePeer().String()[:10], c.id)
}

// Close closes this connection. It does not wait for the Disconnected
// notification to be dispatched to Notifiees: doClose spawns a goroutine for
// that, tracked by swarm.refs, so Close is safe to call from inside a
// Notifiee.Connected handler without deadlocking.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.doClose(0)
	})
	return c.err
}

func (c *Conn) CloseWithError(errCode network.ConnErrorCode) error {
	c.closeOnce.Do(func() {
		c.doClose(errCode)
	})
	return c.err
}

func (c *Conn) doClose(errCode network.ConnErrorCode) {
	c.swarm.removeConn(c)

	// Cancel the context to signal that the connection is closed
	c.cancel()

	// Prevent new streams from opening.
	c.streams.Lock()
	streams := c.streams.m
	c.streams.m = nil
	c.streams.Unlock()

	if errCode != 0 {
		c.err = c.conn.CloseWithError(errCode)
	} else {
		c.err = c.conn.Close()
	}

	// This is just for cleaning up state. The connection has already been closed.
	// We *could* optimize this but it really isn't worth it.
	for s := range streams {
		s.Reset()
	}

	// Dispatch the close notifications in a goroutine. Two deadlocks are
	// avoided by this:
	//   - A PeerConnectednessChanged subscriber that calls Conn.Close would
	//     otherwise call RemoveConn synchronously, blocking on the emitter's
	//     event channel while the run loop is itself blocked waiting for the
	//     subscriber to return.
	//   - A Notifiee.Disconnected handler that calls Conn.Close (which is a
	//     misuse — Disconnected fires because the conn is already closing)
	//     would otherwise re-enter closeOnce.Do while the in-flight doClose
	//     still holds it, deadlocking on sync.Once's internal mutex. We
	//     tolerate the misuse rather than deadlock the caller.
	// The s.refs ref added in addConn is released here.
	go func() {
		defer c.swarm.refs.Done()
		c.swarm.connectionEventsEmitter.RemoveConn(c)
	}()
}

func (c *Conn) removeStream(s *Stream) {
	c.streams.Lock()
	c.stat.NumStreams--
	delete(c.streams.m, s)
	c.streams.Unlock()
	s.scope.Done()
}

// listens for new streams.
//
// The caller must take a swarm ref before calling. This function decrements the
// swarm ref count.
func (c *Conn) start() {
	go func() {
		defer c.swarm.refs.Done()
		defer c.Close()
		for {
			ts, err := c.conn.AcceptStream()
			if err != nil {
				return
			}
			scope, err := c.swarm.ResourceManager().OpenStream(c.RemotePeer(), network.DirInbound)
			if err != nil {
				ts.ResetWithError(network.StreamResourceLimitExceeded)
				continue
			}
			c.swarm.refs.Add(1)
			go func() {
				s, err := c.addStream(ts, network.DirInbound, scope)

				// Don't defer this. We don't want to block
				// swarm shutdown on the connection handler.
				c.swarm.refs.Done()

				// We only get an error here when the swarm is closed or closing.
				if err != nil {
					scope.Done()
					return
				}

				if h := c.swarm.StreamHandler(); h != nil {
					h(s)
				}
				s.completeAcceptStreamGoroutine()
			}()
		}
	}()
}

func (c *Conn) String() string {
	return fmt.Sprintf(
		"<swarm.Conn[%T] %s (%s) <-> %s (%s)>",
		c.conn.Transport(),
		c.conn.LocalMultiaddr(),
		c.conn.LocalPeer(),
		c.conn.RemoteMultiaddr(),
		c.conn.RemotePeer(),
	)
}

// LocalMultiaddr is the Multiaddr on this side
func (c *Conn) LocalMultiaddr() ma.Multiaddr {
	return c.conn.LocalMultiaddr()
}

// LocalPeer is the Peer on our side of the connection
func (c *Conn) LocalPeer() peer.ID {
	return c.conn.LocalPeer()
}

// RemoteMultiaddr is the Multiaddr on the remote side
func (c *Conn) RemoteMultiaddr() ma.Multiaddr {
	return c.conn.RemoteMultiaddr()
}

// RemotePeer is the Peer on the remote side
func (c *Conn) RemotePeer() peer.ID {
	return c.conn.RemotePeer()
}

// RemotePublicKey is the public key of the peer on the remote side
func (c *Conn) RemotePublicKey() ic.PubKey {
	return c.conn.RemotePublicKey()
}

// ConnState is the security connection state. including early data result.
// Empty if not supported.
func (c *Conn) ConnState() network.ConnectionState {
	return c.conn.ConnState()
}

// Stat returns metadata pertaining to this connection
func (c *Conn) Stat() network.ConnStats {
	c.streams.Lock()
	defer c.streams.Unlock()
	return c.stat
}

// NewStream returns a new Stream from this connection
func (c *Conn) NewStream(ctx context.Context) (network.Stream, error) {
	if c.Stat().Limited {
		if useLimited, _ := network.GetAllowLimitedConn(ctx); !useLimited {
			return nil, network.ErrLimitedConn
		}
	}

	scope, err := c.swarm.ResourceManager().OpenStream(c.RemotePeer(), network.DirOutbound)
	if err != nil {
		return nil, err
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultNewStreamTimeout)
		defer cancel()
	}

	s, err := c.openAndAddStream(ctx, scope)
	if err != nil {
		scope.Done()
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("timed out: %w", err)
		}
		return nil, err
	}
	return s, nil
}

func (c *Conn) openAndAddStream(ctx context.Context, scope network.StreamManagementScope) (network.Stream, error) {
	ts, err := c.conn.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	return c.addStream(ts, network.DirOutbound, scope)
}

func (c *Conn) addStream(ts network.MuxedStream, dir network.Direction, scope network.StreamManagementScope) (*Stream, error) {
	c.streams.Lock()
	// Are we still online?
	if c.streams.m == nil {
		c.streams.Unlock()
		ts.Reset()
		return nil, ErrConnClosed
	}

	// Wrap and register the stream.
	s := &Stream{
		stream: ts,
		conn:   c,
		scope:  scope,
		stat: network.Stats{
			Direction: dir,
			Opened:    time.Now(),
		},
		id:                             c.swarm.nextStreamID.Add(1),
		acceptStreamGoroutineCompleted: dir != network.DirInbound,
	}
	c.stat.NumStreams++
	c.streams.m[s] = struct{}{}

	// Released once the stream disconnect notifications have finished
	// firing (in Swarm.remove).
	c.swarm.refs.Add(1)

	c.streams.Unlock()
	return s, nil
}

// GetStreams returns the streams associated with this connection.
func (c *Conn) GetStreams() []network.Stream {
	c.streams.Lock()
	defer c.streams.Unlock()
	streams := make([]network.Stream, 0, len(c.streams.m))
	for s := range c.streams.m {
		streams = append(streams, s)
	}
	return streams
}

func (c *Conn) Scope() network.ConnScope {
	return c.conn.Scope()
}

// Context returns a context that is cancelled when the connection is closed.
func (c *Conn) Context() context.Context {
	return c.ctx
}
