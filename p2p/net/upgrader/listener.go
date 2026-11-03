package upgrader

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/transport"

	tec "github.com/jbenet/go-temp-err-catcher"
	logging "github.com/libp2p/go-libp2p/gologshim"
	manet "github.com/multiformats/go-multiaddr/net"
)

var log = logging.Logger("upgrader")

type listener struct {
	transport.GatedMaListener

	transport transport.Transport
	upgrader  *upgrader
	rcmgr     network.ResourceManager

	incoming chan transport.CapableConn
	err      error

	// Used for backpressure
	threshold *threshold

	// Canceling this context isn't sufficient to tear down the listener.
	// Call close.
	ctx    context.Context
	cancel func()
}

var _ transport.Listener = (*listener)(nil)

// Close closes the listener.
func (l *listener) Close() error {
	// Do this first to try to get any relevant errors.
	err := l.GatedMaListener.Close()

	l.cancel()
	// Drain and wait.
	for c := range l.incoming {
		c.Close()
	}
	return err
}

// handles inbound connections.
//
// This function does a few interesting things that should be noted:
//
//  1. It logs and discards temporary/transient errors (errors with a Temporary()
//     function that returns true).
//  2. It stops accepting new connections once AcceptQueueLength connections have
//     been fully negotiated but not accepted. This gives us a basic backpressure
//     mechanism while still allowing us to negotiate connections in parallel.
func (l *listener) handleIncoming() {
	var wg sync.WaitGroup
	defer func() {
		// make sure we're closed
		l.GatedMaListener.Close()
		if l.err == nil {
			l.err = fmt.Errorf("listener closed")
		}

		wg.Wait()
		close(l.incoming)
	}()

	var catcher tec.TempErrCatcher
	for l.ctx.Err() == nil {
		maconn, connScope, err := l.GatedMaListener.Accept()
		if err != nil {
			// Note: function may pause the accept loop.
			if catcher.IsTemporary(err) {
				log.Info("temporary accept error", "err", err)
				continue
			}
			l.err = err
			return
		}
		catcher.Reset()

		if connScope == nil {
			log.Error("BUG: got nil connScope for incoming connection", "remote_multiaddr", maconn.RemoteMultiaddr())
			maconn.Close()
			continue
		}

		// The go routine below calls Release when the context is
		// canceled so there's no need to wait on it here.
		l.threshold.Wait()

		log.Debug("listener got connection",
			"listener", l,
			"local_multiaddr", maconn.LocalMultiaddr(),
			"remote_multiaddr", maconn.RemoteMultiaddr())

		wg.Go(func() {

			ctx, cancel := context.WithTimeout(l.ctx, l.upgrader.acceptTimeout)
			defer cancel()

			conn, err := l.upgrader.Upgrade(ctx, l.transport, maconn, network.DirInbound, "", connScope)
			if err != nil {
				// Don't bother bubbling this up. We just failed
				// to completely negotiate the connection.
				log.Debug("accept upgrade error",
					"err", err,
					"local_multiaddr", maconn.LocalMultiaddr(),
					"remote_multiaddr", maconn.RemoteMultiaddr())
				connScope.Done()
				return
			}

			log.Debug("listener accepted connection",
				"listener", l,
				"connection", conn)

			// This records the fact that the connection has been
			// setup and is waiting to be accepted. This call
			// *never* blocks, even if we go over the threshold. It
			// simply ensures that calls to Wait block while we're
			// over the threshold.
			l.threshold.Acquire()
			defer l.threshold.Release()

			select {
			case l.incoming <- conn:
			case <-ctx.Done():
				// Listener not closed but the accept timeout expired.
				if l.ctx.Err() == nil {
					log.Warn("listener dropped connection due to slow accept", "remote_multiaddr", maconn.RemoteMultiaddr(), "peer", conn.RemotePeer())
				}
				conn.CloseWithError(network.ConnRateLimited)
			}
		})
	}
}

// Accept accepts a connection.
func (l *listener) Accept() (transport.CapableConn, error) {
	for c := range l.incoming {
		// Could have been sitting there for a while.
		if !c.IsClosed() {
			return c, nil
		}
		// Nothing else owns this conn now, so close it to release its
		// resource scope. IsClosed is also true after a local keepalive or
		// write failure, not only a remote close. This may wait for a close
		// already in flight, about 5s at worst with a stalled TLS peer.
		c.Close()
	}
	if strings.Contains(l.err.Error(), "use of closed network connection") {
		return nil, transport.ErrListenerClosed
	}
	return nil, l.err
}

func (l *listener) String() string {
	if s, ok := l.transport.(fmt.Stringer); ok {
		return fmt.Sprintf("<stream.Listener[%s] %s>", s, l.Multiaddr())
	}
	return fmt.Sprintf("<stream.Listener %s>", l.Multiaddr())
}

type gatedMaListener struct {
	manet.Listener
	rcmgr     network.ResourceManager
	connGater connmgr.ConnectionGater
}

var _ transport.GatedMaListener = &gatedMaListener{}

func (l *gatedMaListener) Accept() (manet.Conn, network.ConnManagementScope, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, nil, err
		}
		// gate the connection if applicable
		if l.connGater != nil && !l.connGater.InterceptAccept(conn) {
			log.Debug("gater blocked incoming connection",
				"local_multiaddr", conn.LocalMultiaddr(),
				"remote_multiaddr", conn.RemoteMultiaddr())
			if err := conn.Close(); err != nil {
				log.Warn("failed to close incoming connection rejected by gater", "err", err)
			}
			continue
		}

		connScope, err := l.rcmgr.OpenConnection(network.DirInbound, true, conn.RemoteMultiaddr())
		if err != nil {
			log.Debug("resource manager blocked accept of new connection", "err", err)
			if err := conn.Close(); err != nil {
				log.Warn("failed to open incoming connection. Rejected by resource manager", "err", err)
			}
			continue
		}
		return conn, connScope, nil
	}
}
