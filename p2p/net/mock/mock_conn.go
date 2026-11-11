package mocknet

import (
	"container/list"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var connCounter atomic.Int64

// conn represents one side's perspective of a
// live connection between two peers.
// it goes over a particular link.
type conn struct {
	notifLk sync.Mutex

	id int64

	local  peer.ID
	remote peer.ID

	localAddr  ma.Multiaddr
	remoteAddr ma.Multiaddr

	localPrivKey ic.PrivKey
	remotePubKey ic.PubKey

	net     *peernet
	link    *link
	rconn   *conn // counterpart
	streams list.List
	stat    network.ConnStats

	closeOnce sync.Once

	isClosed atomic.Bool

	sync.RWMutex
}

func newConn(ln, rn *peernet, l *link, dir network.Direction) *conn {
	c := &conn{net: ln, link: l}
	c.local = ln.peer
	c.remote = rn.peer
	c.stat.Direction = dir
	c.id = connCounter.Add(1)

	c.localAddr = ln.ps.Addrs(ln.peer)[0]
	for _, a := range rn.ps.Addrs(rn.peer) {
		if !manet.IsIPUnspecified(a) {
			c.remoteAddr = a
			break
		}
	}
	if c.remoteAddr == nil {
		c.remoteAddr = rn.ps.Addrs(rn.peer)[0]
	}

	c.localPrivKey = ln.ps.PrivKey(ln.peer)
	c.remotePubKey = rn.ps.PubKey(rn.peer)
	return c
}

func (c *conn) IsClosed() bool {
	return c.isClosed.Load()
}

func (c *conn) ID() string {
	return strconv.FormatInt(c.id, 10)
}

func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		c.isClosed.Store(true)
		go c.rconn.Close()
		c.teardown()
	})
	return nil
}

func (c *conn) As(_ any) bool {
	return false
}

func (c *conn) teardown() {
	for _, s := range c.allStreams() {
		s.Reset()
	}

	c.net.removeConn(c)
}

func (c *conn) addStream(s *stream) {
	c.Lock()
	defer c.Unlock()
	s.conn = c
	c.streams.PushBack(s)
}

func (c *conn) removeStream(s *stream) {
	c.Lock()
	defer c.Unlock()
	for e := c.streams.Front(); e != nil; e = e.Next() {
		if s == e.Value {
			c.streams.Remove(e)
			return
		}
	}
}

func (c *conn) allStreams() []network.Stream {
	c.RLock()
	defer c.RUnlock()

	strs := make([]network.Stream, 0, c.streams.Len())
	for e := c.streams.Front(); e != nil; e = e.Next() {
		s := e.Value.(*stream)
		strs = append(strs, s)
	}
	return strs
}

func (c *conn) remoteOpenedStream(s *stream) {
	c.addStream(s)
	c.net.handleNewStream(s)
}

func (c *conn) openStream() *stream {
	sl, sr := newStreamPair()
	go c.rconn.remoteOpenedStream(sr)
	c.addStream(sl)
	return sl
}

func (c *conn) NewStream(context.Context) (network.Stream, error) {
	log.Debug("Conn.NewStreamWithProtocol", "source_peer", c.local, "destination_peer", c.remote)

	s := c.openStream()
	return s, nil
}

func (c *conn) GetStreams() []network.Stream {
	return c.allStreams()
}

// LocalMultiaddr is the Multiaddr on this side
func (c *conn) LocalMultiaddr() ma.Multiaddr {
	return c.localAddr
}

// LocalPeer is the Peer on our side of the connection
func (c *conn) LocalPeer() peer.ID {
	return c.local
}

// RemoteMultiaddr is the Multiaddr on the remote side
func (c *conn) RemoteMultiaddr() ma.Multiaddr {
	return c.remoteAddr
}

// RemotePeer is the Peer on the remote side
func (c *conn) RemotePeer() peer.ID {
	return c.remote
}

// RemotePublicKey is the private key of the peer on our side.
func (c *conn) RemotePublicKey() ic.PubKey {
	return c.remotePubKey
}

// ConnState of security connection. Empty if not supported.
func (c *conn) ConnState() network.ConnectionState {
	return network.ConnectionState{}
}

// Stat returns metadata about the connection
func (c *conn) Stat() network.ConnStats {
	return c.stat
}

func (c *conn) Scope() network.ConnScope {
	return &network.NullScope{}
}

func (c *conn) CloseWithError(_ network.ConnErrorCode) error {
	return c.Close()
}

// Context returns a context that is cancelled when the connection is closed.
func (c *conn) Context() context.Context {
	// For mock connections, we return a context that is cancelled when the connection is closed
	// This is a simplified implementation for testing purposes
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Wait for the connection to be closed by checking periodically
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if c.IsClosed() {
					cancel()
					return
				}
			}
		}
	}()
	return ctx
}
