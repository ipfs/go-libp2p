package pipetransport

import (
	"fmt"
	"io"
	"net"
	"sync"

	ic "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
	tpt "github.com/libp2p/go-libp2p-transport"
	streammux "github.com/libp2p/go-stream-muxer"
	ma "github.com/multiformats/go-multiaddr"
)

type PipeConn struct {
	streams chan *PipeStream

	mclosed *sync.RWMutex
	closed  bool

	addr      ma.Multiaddr
	id        peer.ID
	pubKey    ic.PubKey
	transport *PipeTransport
}

func NewPipeConn(id peer.ID, addr ma.Multiaddr, pubKey ic.PubKey, transport *PipeTransport) *PipeConn {
	return &PipeConn{
		streams:   make(chan *PipeStream),
		addr:      addr,
		id:        id,
		pubKey:    pubKey,
		transport: transport,
		mclosed:   new(sync.RWMutex),
		closed:    false,
	}
}

func (c *PipeConn) Close() error {
	c.mclosed.Lock()
	defer c.mclosed.Unlock()

	if c.closed {
		return fmt.Errorf("tried to close already closed connection")
	}

	c.closed = true
	close(c.streams)
	return nil
}

func (c *PipeConn) IsClosed() bool {
	c.mclosed.RLock()
	defer c.mclosed.RUnlock()

	return c.closed
}

func (c *PipeConn) AcceptStream() (streammux.Stream, error) {
	stream, ok := <-c.streams
	if !ok {
		return nil, io.EOF
	}
	return stream, nil
}

func (c *PipeConn) OpenStream() (streammux.Stream, error) {
	c.mclosed.RLock()
	defer c.mclosed.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("opening stream on closed connection")
	}

	connA, connB := net.Pipe()
	connC, connD := net.Pipe()
	streamA := NewPipeStream(connA, connC)
	streamB := NewPipeStream(connD, connB)
	c.streams <- streamB
	return streamA, nil
}

func (c *PipeConn) LocalMultiaddr() ma.Multiaddr {
	return c.addr
}

func (c *PipeConn) RemoteMultiaddr() ma.Multiaddr {
	return c.addr
}

func (c *PipeConn) LocalPeer() peer.ID {
	return c.transport.id
}

func (c *PipeConn) LocalPrivateKey() ic.PrivKey {
	return c.transport.privKey
}

func (c *PipeConn) RemotePeer() peer.ID {
	return c.id
}

func (c *PipeConn) RemotePublicKey() ic.PubKey {
	return c.pubKey
}

func (c *PipeConn) Transport() tpt.Transport {
	return c.transport
}

var _ tpt.Conn = (*PipeConn)(nil)
