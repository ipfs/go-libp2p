package pipetransport

import (
	"fmt"
	"net"

	tpt "github.com/libp2p/go-libp2p-transport"
	ma "github.com/multiformats/go-multiaddr"
)

type PipeListener struct {
	listenaddr ma.Multiaddr
	listench   chan *PipeConn
	transport  *PipeTransport
}

func NewPipeListener(addr ma.Multiaddr, ch chan *PipeConn, t *PipeTransport) *PipeListener {
	return &PipeListener{
		listenaddr: addr,
		listench:   ch,
		transport:  t,
	}
}

func (l *PipeListener) Accept() (tpt.Conn, error) {
	conn, ok := <-l.listench
	if !ok {
		return nil, fmt.Errorf("memorytransport closed")
	}
	return conn, nil
}

func (l *PipeListener) Close() error {
	l.transport.closeListener(l.listenaddr.String())
	return nil
}

func (l *PipeListener) Addr() net.Addr {
	return nil
}

func (l *PipeListener) Multiaddr() ma.Multiaddr {
	return l.listenaddr
}

var _ tpt.Listener = (*PipeListener)(nil)
