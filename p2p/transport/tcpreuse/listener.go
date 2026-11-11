package tcpreuse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/transport"
	logging "github.com/libp2p/go-libp2p/gologshim"
	"github.com/libp2p/go-libp2p/p2p/net/reuseport"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const acceptQueueSize = 64 // It is fine to read 3 bytes from 64 connections in parallel.

// How long we wait for a connection to be accepted before dropping it.
const acceptTimeout = 30 * time.Second

var log = logging.Logger("tcp-demultiplex")

// ConnMgr enables you to share the same listen address between TCP and WebSocket transports.
type ConnMgr struct {
	enableReuseport bool
	reuse           reuseport.Transport
	upgrader        transport.Upgrader

	mx        sync.Mutex
	listeners map[string]*multiplexedListener
}

func NewConnMgr(enableReuseport bool, upgrader transport.Upgrader) *ConnMgr {
	return &ConnMgr{
		enableReuseport: enableReuseport,
		reuse:           reuseport.Transport{},
		upgrader:        upgrader,
		listeners:       make(map[string]*multiplexedListener),
	}
}

func (t *ConnMgr) gatedMaListen(listenAddr ma.Multiaddr) (transport.GatedMaListener, error) {
	var mal manet.Listener
	var err error
	if t.useReuseport() {
		mal, err = t.reuse.Listen(listenAddr)
		if err != nil {
			return nil, err
		}
	} else {
		mal, err = manet.Listen(listenAddr)
		if err != nil {
			return nil, err
		}
	}
	return t.upgrader.GateMaListener(mal), nil
}

func (t *ConnMgr) useReuseport() bool {
	return t.enableReuseport && ReuseportIsAvailable()
}

func getTCPAddr(listenAddr ma.Multiaddr) (ma.Multiaddr, error) {
	haveTCP := false
	addr, _ := ma.SplitFunc(listenAddr, func(c ma.Component) bool {
		if haveTCP {
			return true
		}
		if c.Protocol().Code == ma.P_TCP {
			haveTCP = true
		}
		return false
	})
	if !haveTCP {
		return nil, fmt.Errorf("invalid listen addr %s, need tcp address", listenAddr)
	}
	return addr, nil
}

// DemultiplexedListen returns a listener for laddr listening for `connType` connections. The connections
// accepted from returned listeners need to be upgraded with a `transport.Upgrader`.
// NOTE: All listeners for port 0 share the same underlying socket, so they have the same specific port.
func (t *ConnMgr) DemultiplexedListen(laddr ma.Multiaddr, connType DemultiplexedConnType) (transport.GatedMaListener, error) {
	if !connType.IsKnown() {
		return nil, fmt.Errorf("unknown connection type: %s", connType)
	}
	laddr, err := getTCPAddr(laddr)
	if err != nil {
		return nil, err
	}

	t.mx.Lock()
	defer t.mx.Unlock()
	ml, ok := t.listeners[laddr.String()]
	if ok {
		dl, err := ml.DemultiplexedListen(connType)
		if err != nil {
			return nil, err
		}
		return dl, nil
	}

	gmal, err := t.gatedMaListen(laddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	removeFunc := func() error {
		t.mx.Lock()
		defer t.mx.Unlock()
		delete(t.listeners, laddr.String())
		delete(t.listeners, gmal.Multiaddr().String())
		return nil
	}
	ml = &multiplexedListener{
		GatedMaListener: gmal,
		listeners:       make(map[DemultiplexedConnType]*demultiplexedListener),
		ctx:             ctx,
		cancel:          cancel,
		closeFn:         removeFunc,
	}
	t.listeners[laddr.String()] = ml
	t.listeners[gmal.Multiaddr().String()] = ml

	dl, err := ml.DemultiplexedListen(connType)
	if err != nil {
		cerr := ml.Close()
		return nil, errors.Join(err, cerr)
	}

	ml.wg.Add(1)
	go ml.run()

	return dl, nil
}

var _ transport.GatedMaListener = &demultiplexedListener{}

type multiplexedListener struct {
	transport.GatedMaListener
	listeners map[DemultiplexedConnType]*demultiplexedListener
	mx        sync.RWMutex

	ctx     context.Context
	cancel  context.CancelFunc
	closeFn func() error
	wg      sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

var ErrListenerExists = errors.New("listener already exists for this conn type on this address")

func (m *multiplexedListener) DemultiplexedListen(connType DemultiplexedConnType) (transport.GatedMaListener, error) {
	if !connType.IsKnown() {
		return nil, fmt.Errorf("unknown connection type: %s", connType)
	}

	m.mx.Lock()
	defer m.mx.Unlock()
	if m.ctx.Err() != nil {
		return nil, transport.ErrListenerClosed
	}
	if _, ok := m.listeners[connType]; ok {
		return nil, ErrListenerExists
	}

	ctx, cancel := context.WithCancel(m.ctx)
	l := &demultiplexedListener{
		buffer:     make(chan *connWithScope),
		inner:      m.GatedMaListener,
		ctx:        ctx,
		cancelFunc: cancel,
		closeFn:    func() error { m.removeDemultiplexedListener(connType); return nil },
	}

	m.listeners[connType] = l

	return l, nil
}

func (m *multiplexedListener) run() error {
	defer m.Close()
	defer m.wg.Done()
	acceptQueue := make(chan struct{}, acceptQueueSize)
	for {
		c, connScope, err := m.GatedMaListener.Accept()
		if err != nil {
			return err
		}
		ctx, cancelCtx := context.WithTimeout(m.ctx, acceptTimeout)
		select {
		case acceptQueue <- struct{}{}:
		case <-ctx.Done():
			cancelCtx()
			connScope.Done()
			c.Close()
			log.Debug("accept queue full, dropping connection", "remote_addr", c.RemoteMultiaddr())
			continue
		case <-m.ctx.Done():
			cancelCtx()
			connScope.Done()
			c.Close()
			log.Debug("listener closed; dropping connection", "remote_addr", c.RemoteMultiaddr())
			continue
		}

		m.wg.Add(1)
		go func() {
			defer func() { <-acceptQueue }()
			defer m.wg.Done()
			defer cancelCtx()
			t, c, err := identifyConnType(c)
			if err != nil {
				// conn closed by identifyConnType
				connScope.Done()
				log.Debug("error demultiplexing connection", "error", err)
				return
			}

			connWithScope, err := manetConnWithScope(c, connScope)
			if err != nil {
				connScope.Done()
				closeErr := c.Close()
				err = errors.Join(err, closeErr)
				log.Debug("error wrapping connection with scope", "error", err)
				return
			}

			m.mx.RLock()
			demux, ok := m.listeners[t]
			m.mx.RUnlock()
			if !ok {
				closeErr := connWithScope.Close()
				if closeErr != nil {
					log.Debug("no registered listener for demultiplex connection. Error closing the connection", "type", t, "close_error", closeErr)
				} else {
					log.Debug("no registered listener for demultiplex connection", "type", t)
				}
				return
			}

			select {
			case demux.buffer <- connWithScope:
			case <-ctx.Done():
				log.Debug("accept timeout; dropping connection", "remote", connWithScope.RemoteMultiaddr())
				connWithScope.Close()
			}
		}()
	}
}

func (m *multiplexedListener) Close() error {
	m.closeOnce.Do(func() {
		m.mx.Lock()
		m.cancel()
		for _, l := range m.listeners {
			l.cancelFunc()
		}
		m.mx.Unlock()

		// closeFn acquires the ConnMgr lock. Call it without holding m.mx,
		// since DemultiplexedListen acquires those locks in the opposite order.
		m.closeErr = m.closeListener()
		m.wg.Wait()
	})
	return m.closeErr
}

func (m *multiplexedListener) closeListener() error {
	lerr := m.GatedMaListener.Close()
	cerr := m.closeFn()
	return errors.Join(lerr, cerr)
}

func (m *multiplexedListener) removeDemultiplexedListener(c DemultiplexedConnType) {
	m.mx.Lock()
	delete(m.listeners, c)
	empty := len(m.listeners) == 0
	m.mx.Unlock()
	if empty {
		_ = m.Close()
	}
}

type demultiplexedListener struct {
	buffer     chan *connWithScope
	inner      transport.GatedMaListener
	ctx        context.Context
	cancelFunc context.CancelFunc
	closeFn    func() error
}

func (m *demultiplexedListener) Accept() (manet.Conn, network.ConnManagementScope, error) {
	select {
	case c := <-m.buffer:
		return c.ManetTCPConnInterface, c.ConnScope, nil
	case <-m.ctx.Done():
		return nil, nil, transport.ErrListenerClosed
	}
}

func (m *demultiplexedListener) Close() error {
	m.cancelFunc()
	return m.closeFn()
}

func (m *demultiplexedListener) Multiaddr() ma.Multiaddr {
	return m.inner.Multiaddr()
}

func (m *demultiplexedListener) Addr() net.Addr {
	return m.inner.Addr()
}
