package upgrader

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	"github.com/libp2p/go-libp2p/core/transport"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/insecure"
	"github.com/libp2p/go-libp2p/x/rate"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
)

type closedMuxedConn struct {
	network.MuxedConn
	closed bool
}

func (c *closedMuxedConn) IsClosed() bool { return c.closed }
func (c *closedMuxedConn) Close() error   { return nil }

// blockingCloseConn is closed, and its Close blocks until unblock is closed.
type blockingCloseConn struct {
	closedMuxedConn
	unblock chan struct{}
}

func (c *blockingCloseConn) Close() error {
	<-c.unblock
	return nil
}

func newTestRcmgr(t *testing.T) network.ResourceManager {
	t.Helper()
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits), rcmgr.WithConnRateLimiters(&rate.Limiter{}))
	require.NoError(t, err)
	t.Cleanup(func() { rm.Close() })
	return rm
}

func systemStat(t *testing.T, rm network.ResourceManager) network.ScopeStat {
	t.Helper()
	var st network.ScopeStat
	require.NoError(t, rm.ViewSystem(func(s network.ResourceScope) error {
		st = s.Stat()
		return nil
	}))
	return st
}

func newTestPeer(t *testing.T) (peer.ID, crypto.PrivKey) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	return id, priv
}

// acceptFrom runs Accept over a queue the test controls, so that "closed before
// Accept" is a precondition rather than a timing assumption.
func acceptFrom(conns ...transport.CapableConn) (transport.CapableConn, error) {
	ch := make(chan transport.CapableConn, len(conns))
	for _, c := range conns {
		ch <- c
	}
	close(ch)
	return (&listener{incoming: ch, err: errors.New("drained")}).Accept()
}

func TestAcceptReleasesScopeOfClosedConn(t *testing.T) {
	rm := newTestRcmgr(t)
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	deadPeer, _ := newTestPeer(t)

	dead, err := rm.OpenConnection(network.DirInbound, true, addr)
	require.NoError(t, err)
	require.NoError(t, dead.SetPeer(deadPeer))
	live, err := rm.OpenConnection(network.DirInbound, true, addr)
	require.NoError(t, err)

	liveConn := &transportConn{MuxedConn: &closedMuxedConn{}, scope: live}
	got, err := acceptFrom(&transportConn{MuxedConn: &closedMuxedConn{closed: true}, scope: dead}, liveConn)
	require.NoError(t, err)
	require.Same(t, liveConn, got)

	require.Eventually(t, func() bool { return systemStat(t, rm).NumConnsInbound == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, 1, systemStat(t, rm).NumFD)
	require.NoError(t, rm.ViewPeer(deadPeer, func(s network.PeerScope) error {
		require.Equal(t, 0, s.Stat().NumConnsInbound)
		return nil
	}))
}

// Uses real TCP and yamux. The conn is held out of Accept until the server has
// observed the remote close, so no sleep is needed.
func TestAcceptReleasesScopeOfConnClosedByRemote(t *testing.T) {
	rm := newTestRcmgr(t)
	id, priv := newTestPeer(t)
	u, err := New([]sec.SecureTransport{insecure.NewWithIdentity(insecure.ID, id, priv)},
		[]StreamMuxer{{ID: "yamux", Muxer: yamux.DefaultTransport}}, nil, rm, nil)
	require.NoError(t, err)
	mal, err := manet.Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	ln := u.UpgradeGatedMaListener(nil, u.GateMaListener(mal)).(*listener)
	defer ln.Close()

	raw, err := manet.Dial(ln.Multiaddr())
	require.NoError(t, err)
	cc, err := u.Upgrade(context.Background(), nil, raw, network.DirOutbound, id, &network.NullScope{})
	require.NoError(t, err)
	require.NoError(t, cc.Close())

	c := <-ln.incoming
	require.Eventually(t, c.IsClosed, 30*time.Second, time.Millisecond)
	require.Equal(t, 1, systemStat(t, rm).NumConnsInbound)

	// Hand it back to the running listener, as handleIncoming would.
	go ln.Accept()
	ln.incoming <- c
	require.Eventually(t, func() bool { return systemStat(t, rm).NumConnsInbound == 0 }, 5*time.Second, time.Millisecond)
	require.Equal(t, 0, systemStat(t, rm).NumFD)
}

// A leaked scope also holds a slot in the per-IP limiter, which would eventually
// refuse every connection from that IP.
func TestAcceptReleasesPerIPLimitOfClosedConn(t *testing.T) {
	rm := newTestRcmgr(t)
	addr := ma.StringCast("/ip4/203.0.113.7/tcp/1")
	for i := range 20 {
		s, err := rm.OpenConnection(network.DirInbound, true, addr)
		require.NoErrorf(t, err, "IP refused after %d conns closed before Accept", i)
		_, err = acceptFrom(&transportConn{MuxedConn: &closedMuxedConn{closed: true}, scope: s})
		require.Error(t, err)
		require.Eventually(t, func() bool { return systemStat(t, rm).NumConnsInbound == 0 }, 5*time.Second, time.Millisecond)
	}
}

// Closing a skipped conn can wait for a close already in flight. That must not
// hold up the live conn queued behind it.
func TestAcceptDoesNotBlockOnClose(t *testing.T) {
	rm := newTestRcmgr(t)
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	dead, err := rm.OpenConnection(network.DirInbound, true, addr)
	require.NoError(t, err)

	unblock := make(chan struct{})
	liveConn := &transportConn{MuxedConn: &closedMuxedConn{}, scope: &network.NullScope{}}
	done := make(chan transport.CapableConn, 1)
	go func() {
		c, _ := acceptFrom(&transportConn{MuxedConn: &blockingCloseConn{closedMuxedConn{closed: true}, unblock}, scope: dead}, liveConn)
		done <- c
	}()
	select {
	case c := <-done:
		require.Same(t, liveConn, c)
	case <-time.After(5 * time.Second):
		close(unblock)
		t.Fatal("Accept blocked on closing a skipped conn")
	}

	require.Equal(t, 1, systemStat(t, rm).NumConnsInbound, "scope is released when Close returns")
	close(unblock)
	require.Eventually(t, func() bool { return systemStat(t, rm).NumConnsInbound == 0 }, 5*time.Second, time.Millisecond)
}
