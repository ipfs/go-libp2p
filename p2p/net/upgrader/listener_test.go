package upgrader_test

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	mocknetwork "github.com/libp2p/go-libp2p/core/network/mocks"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/net/upgrader"
	"github.com/libp2p/go-libp2p/p2p/security/insecure"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func createListener(t *testing.T, u transport.Upgrader) transport.Listener {
	t.Helper()
	addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	require.NoError(t, err)
	ln, err := manet.Listen(addr)
	require.NoError(t, err)
	return u.UpgradeGatedMaListener(nil, u.GateMaListener(ln))
}

func TestAcceptSingleConn(t *testing.T) {
	require := require.New(t)

	id, u := createUpgrader(t)
	ln := createListener(t, u)
	defer ln.Close()

	cconn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
	require.NoError(err)

	sconn, err := ln.Accept()
	require.NoError(err)

	testConn(t, cconn, sconn)
}

func TestAcceptMultipleConns(t *testing.T) {
	require := require.New(t)

	id, u := createUpgrader(t)
	ln := createListener(t, u)
	defer ln.Close()

	toClose := make([]io.Closer, 0, 20)
	defer func() {
		for _, c := range toClose {
			_ = c.Close()
		}
	}()

	for range 10 {
		cconn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
		require.NoError(err)
		toClose = append(toClose, cconn)

		sconn, err := ln.Accept()
		require.NoError(err)
		toClose = append(toClose, sconn)

		testConn(t, cconn, sconn)
	}
}

func TestConnectionsClosedIfNotAccepted(t *testing.T) {
	require := require.New(t)

	var timeout = 100 * time.Millisecond
	if os.Getenv("CI") != "" {
		timeout = 500 * time.Millisecond
	}

	id, u := createUpgraderWithOpts(t, upgrader.WithAcceptTimeout(timeout))
	ln := createListener(t, u)
	defer ln.Close()

	conn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
	require.NoError(err)

	errCh := make(chan error)
	go func() {
		defer conn.Close()
		str, err := conn.OpenStream(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		// start a Read. It will block until the connection is closed
		_, _ = str.Read([]byte{0})
		errCh <- nil
	}()

	time.Sleep(timeout / 2)
	select {
	case err := <-errCh:
		t.Fatalf("connection closed earlier than expected. expected nothing on channel, got: %v", err)
	default:
	}

	time.Sleep(timeout)
	require.NoError(<-errCh)
}

func TestFailedUpgradeOnListen(t *testing.T) {
	require := require.New(t)

	id, u := createUpgraderWithMuxers(t, []upgrader.StreamMuxer{{ID: "errorMuxer", Muxer: &errorMuxer{}}}, nil, nil)
	ln := createListener(t, u)

	errCh := make(chan error)
	go func() {
		_, err := ln.Accept()
		errCh <- err
	}()

	_, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
	require.Error(err)

	// close the listener.
	ln.Close()
	require.Error(<-errCh)
}

func TestListenerClose(t *testing.T) {
	require := require.New(t)

	_, u := createUpgrader(t)
	ln := createListener(t, u)

	errCh := make(chan error)
	go func() {
		_, err := ln.Accept()
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("connection closed earlier than expected. expected nothing on channel, got: %v", err)
	case <-time.After(200 * time.Millisecond):
		// nothing in 200ms.
	}

	// unblocks Accept when it is closed.
	require.NoError(ln.Close())
	err := <-errCh
	require.Error(err)
	require.Equal(err, transport.ErrListenerClosed)

	// doesn't accept new connections when it is closed
	_, err = dial(t, u, ln.Multiaddr(), peer.ID("1"), &network.NullScope{})
	require.Error(err)
}

func TestListenerCloseClosesQueued(t *testing.T) {
	require := require.New(t)

	id, upgrader := createUpgrader(t)
	ln := createListener(t, upgrader)

	conns := make([]transport.CapableConn, 0, 10)
	for range 10 {
		conn, err := dial(t, upgrader, ln.Multiaddr(), id, &network.NullScope{})
		require.NoError(err)
		conns = append(conns, conn)
	}

	// wait for all the dials to happen.
	time.Sleep(500 * time.Millisecond)

	// all the connections are opened.
	for _, c := range conns {
		require.False(c.IsClosed())
	}

	// expect that all the connections will be closed.
	err := ln.Close()
	require.NoError(err)

	// all the connections are closed.
	require.Eventually(func() bool {
		for _, c := range conns {
			if !c.IsClosed() {
				return false
			}
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	for _, c := range conns {
		_ = c.Close()
	}
}

func TestConcurrentAccept(t *testing.T) {
	var num = 3 * upgrader.AcceptQueueLength

	blockingMuxer := newBlockingMuxer()
	id, u := createUpgraderWithMuxers(t, []upgrader.StreamMuxer{{ID: "blockingMuxer", Muxer: blockingMuxer}}, nil, nil)
	ln := createListener(t, u)
	defer ln.Close()

	accepted := make(chan transport.CapableConn, num)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
			accepted <- conn
		}
	}()

	// start num dials, which all block while setting up the muxer
	errCh := make(chan error, num)
	var wg sync.WaitGroup
	for range num {
		wg.Go(func() {

			conn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()

			_, err = conn.AcceptStream() // wait for conn to be accepted.
			errCh <- err
		})
	}

	time.Sleep(200 * time.Millisecond)
	// the dials are still blocked, so we shouldn't have any connection available yet
	require.Empty(t, accepted)
	blockingMuxer.Unblock() // make all dials succeed
	require.Eventually(t, func() bool { return len(accepted) == num }, 3*time.Second, 100*time.Millisecond)
	wg.Wait()
}

func TestAcceptQueueBacklogged(t *testing.T) {
	require := require.New(t)

	id, u := createUpgrader(t)
	ln := createListener(t, u)
	defer ln.Close()

	// setup AcceptQueueLength connections, but don't accept any of them
	var counter atomic.Int32
	doDial := func() {
		conn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
		require.NoError(err)
		counter.Add(1)
		t.Cleanup(func() { conn.Close() })
	}

	for i := 0; i < upgrader.AcceptQueueLength; i++ {
		go doDial()
	}

	require.Eventually(func() bool { return int(counter.Load()) == upgrader.AcceptQueueLength }, 2*time.Second, 50*time.Millisecond)

	// dial a new connection. This connection should not complete setup, since the queue is full
	go doDial()

	time.Sleep(100 * time.Millisecond)
	require.Equal(int(counter.Load()), upgrader.AcceptQueueLength)

	// accept a single connection. Now the new connection should be set up, and fill the queue again
	conn, err := ln.Accept()
	require.NoError(err)
	require.NoError(conn.Close())

	require.Eventually(func() bool { return int(counter.Load()) == upgrader.AcceptQueueLength+1 }, 2*time.Second, 50*time.Millisecond)
}

func TestListenerConnectionGater(t *testing.T) {
	require := require.New(t)

	testGater := &testGater{}
	id, u := createUpgraderWithConnGater(t, testGater)

	ln := createListener(t, u)
	defer ln.Close()

	// no gating.
	conn, err := dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
	require.NoError(err)
	require.False(conn.IsClosed())
	_ = conn.Close()

	// rejecting after handshake.
	testGater.BlockSecured(true)
	testGater.BlockAccept(false)
	conn, err = dial(t, u, ln.Multiaddr(), "invalid", &network.NullScope{})
	require.Error(err)
	require.Nil(conn)

	// rejecting on accept will trigger firupgrader.
	testGater.BlockSecured(true)
	testGater.BlockAccept(true)
	conn, err = dial(t, u, ln.Multiaddr(), "invalid", &network.NullScope{})
	require.Error(err)
	require.Nil(conn)

	// rejecting only on acceptance.
	testGater.BlockSecured(false)
	testGater.BlockAccept(true)
	conn, err = dial(t, u, ln.Multiaddr(), "invalid", &network.NullScope{})
	require.Error(err)
	require.Nil(conn)

	// back to normal
	testGater.BlockSecured(false)
	testGater.BlockAccept(false)
	conn, err = dial(t, u, ln.Multiaddr(), id, &network.NullScope{})
	require.NoError(err)
	require.False(conn.IsClosed())
	_ = conn.Close()
}

func TestListenerResourceManagement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rcmgr := mocknetwork.NewMockResourceManager(ctrl)
	id, upgrader := createUpgraderWithResourceManager(t, rcmgr)
	ln := createListener(t, upgrader)
	defer ln.Close()

	connScope := mocknetwork.NewMockConnManagementScope(ctrl)
	gomock.InOrder(
		rcmgr.EXPECT().OpenConnection(network.DirInbound, true, gomock.Not(ln.Multiaddr())).Return(connScope, nil),
		connScope.EXPECT().PeerScope(),
		connScope.EXPECT().SetPeer(id),
		connScope.EXPECT().PeerScope(),
	)

	cconn, err := dial(t, upgrader, ln.Multiaddr(), id, &network.NullScope{})
	require.NoError(t, err)
	defer cconn.Close()

	sconn, err := ln.Accept()
	require.NoError(t, err)
	connScope.EXPECT().Done()
	defer sconn.Close()
}

func TestListenerResourceManagementDenied(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rcmgr := mocknetwork.NewMockResourceManager(ctrl)
	id, upgrader := createUpgraderWithResourceManager(t, rcmgr)
	ln := createListener(t, upgrader)

	rcmgr.EXPECT().OpenConnection(network.DirInbound, true, gomock.Not(ln.Multiaddr())).Return(nil, errors.New("nope"))
	_, err := dial(t, upgrader, ln.Multiaddr(), id, &network.NullScope{})
	require.Error(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ln.Accept()
	}()

	select {
	case <-done:
		t.Fatal("accept shouldn't have accepted anything")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, ln.Close())
	<-done
}

func TestNoCommonSecurityProto(t *testing.T) {
	idA, privA := newPeer(t)
	idB, privB := newPeer(t)
	atInsecure := insecure.NewWithIdentity("/plaintext1", idA, privA)
	btInsecure := insecure.NewWithIdentity("/plaintext2", idB, privB)

	ua, err := upgrader.New([]sec.SecureTransport{atInsecure}, []upgrader.StreamMuxer{{ID: "negotiate", Muxer: &negotiatingMuxer{}}}, nil, nil, nil)
	require.NoError(t, err)
	ub, err := upgrader.New([]sec.SecureTransport{btInsecure}, []upgrader.StreamMuxer{{ID: "negotiate", Muxer: &negotiatingMuxer{}}}, nil, nil, nil)
	require.NoError(t, err)

	ln := createListener(t, ua)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ln.Accept()
	}()

	_, err = dial(t, ub, ln.Multiaddr(), idA, &network.NullScope{})
	require.ErrorContains(t, err, "failed to negotiate security protocol: protocols not supported")
	select {
	case <-done:
		t.Fatal("didn't expect to accept a connection")
	case <-time.After(50 * time.Millisecond):
	}

	ln.Close()
	<-done
}
