package pipetransport

import (
	"context"
	"fmt"

	"sync"

	ic "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
	tpt "github.com/libp2p/go-libp2p-transport"
	ma "github.com/multiformats/go-multiaddr"
)

type listenerChans struct {
	mlistenchans *sync.RWMutex
	listenchans  map[string]chan *PipeConn
}

var listeners = &listenerChans{
	mlistenchans: new(sync.RWMutex),
	listenchans:  make(map[string]chan *PipeConn),
}

type PipeTransport struct {
	id      peer.ID
	pubKey  ic.PubKey
	privKey ic.PrivKey
}

var _ tpt.Transport = (*PipeTransport)(nil)

func New(id peer.ID, pubKey ic.PubKey, privKey ic.PrivKey) *PipeTransport {
	return &PipeTransport{
		id:      id,
		pubKey:  pubKey,
		privKey: privKey,
	}
}

func (t *PipeTransport) closeListener(addr string) {
	listeners.mlistenchans.Lock()
	defer listeners.mlistenchans.Unlock()

	ch, ok := listeners.listenchans[addr]
	if !ok {
		return
	}
	close(ch)
	delete(listeners.listenchans, addr)
}

func (t *PipeTransport) CanDial(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	return len(protocols) == 1 && protocols[0].Code == ma.P_MEMORY
}

func (t *PipeTransport) Protocols() []int {
	return []int{
		ma.P_MEMORY,
	}
}

func (t *PipeTransport) Proxy() bool {
	return false
}

func (t *PipeTransport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (tpt.Conn, error) {
	listeners.mlistenchans.RLock()
	defer listeners.mlistenchans.RUnlock()
	raddrStr := raddr.String()

	ch, ok := listeners.listenchans[raddrStr]
	if !ok {
		return nil, fmt.Errorf("no memorylistener for %s", raddrStr)
	}

	conn := NewPipeConn(p, raddr, t.pubKey, t)
	ch <- conn
	return conn, nil
}

func (t *PipeTransport) Listen(laddr ma.Multiaddr) (tpt.Listener, error) {
	listeners.mlistenchans.Lock()
	defer listeners.mlistenchans.Unlock()

	laddrStr := laddr.String()
	if _, ok := listeners.listenchans[laddrStr]; ok {
		return nil, fmt.Errorf("already listening on %s", laddrStr)
	}

	ch := make(chan *PipeConn)
	listeners.listenchans[laddrStr] = ch

	listener := NewPipeListener(laddr, ch, t)

	return listener, nil
}
