package mocknet

import (
	"container/list"
	"strconv"
	"sync"
	"sync/atomic"

	process "github.com/jbenet/goprocess"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

var connCounter int64

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
	stat    network.Stat

	pairProc, connProc process.Process

	sync.RWMutex
}

func newConn(p process.Process, ln, rn *peernet, l *link, dir network.Direction) *conn {
	c := &conn{net: ln, link: l, pairProc: p}
	c.local = ln.peer
	c.remote = rn.peer
	c.stat = network.Stat{Direction: dir}
	c.id = atomic.AddInt64(&connCounter, 1)

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
	c.connProc = process.WithParent(c.pairProc)
	return c
}

func (c *conn) ID() string {
	return strconv.FormatInt(c.id, 10)
}

func (c *conn) Close() error {
	return c.pairProc.Close()
}

func (c *conn) setup() {
	c.connProc.SetTeardown(c.teardown)
}

func (c *conn) teardown() error {
	for _, s := range c.allStreams() {
		s.Reset()
	}
	c.net.removeConn(c)

	go func() {
		c.notifLk.Lock()
		defer c.notifLk.Unlock()
		c.net.notifyAll(func(n network.Notifiee) {
			n.Disconnected(c.net, c)
		})
	}()
	return nil
}

func (c *conn) addStream(s *stream) {
	c.Lock()
	s.conn = c
	c.streams.PushBack(s)
	s.notifLk.Lock()
	defer s.notifLk.Unlock()
	c.Unlock()
	c.net.notifyAll(func(n network.Notifiee) {
		n.OpenedStream(c.net, s)
	})
}

func (c *conn) removeStream(s *stream) {
	c.Lock()
	for e := c.streams.Front(); e != nil; e = e.Next() {
		if s == e.Value {
			c.streams.Remove(e)
			break
		}
	}
	c.Unlock()

	go func() {
		s.notifLk.Lock()
		defer s.notifLk.Unlock()
		s.conn.net.notifyAll(func(n network.Notifiee) {
			n.ClosedStream(s.conn.net, s)
		})
	}()
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

func (c *conn) NewStream() (network.Stream, error) {
	log.Debugf("Conn.NewStreamWithProtocol: %s --> %s", c.local, c.remote)

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

// LocalPrivateKey is the private key of the peer on our side.
func (c *conn) LocalPrivateKey() ic.PrivKey {
	return c.localPrivKey
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

// Stat returns metadata about the connection
func (c *conn) Stat() network.Stat {
	return c.stat
}
