package identify_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	p2putil "github.com/libp2p/go-libp2p-netutil"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	identify "github.com/libp2p/go-libp2p/p2p/protocol/identify"

	detectrace "github.com/ipfs/go-detect-race"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

type harness struct {
	t *testing.T

	mocknet mocknet.Mocknet
	host    host.Host

	oas *identify.ObservedAddrManager
}

func (h *harness) add(observer ma.Multiaddr) peer.ID {
	// create a new fake peer.
	sk, err := p2putil.RandTestBogusPrivateKey()
	if err != nil {
		h.t.Fatal(err)
	}
	h2, err := h.mocknet.AddPeer(sk, observer)
	if err != nil {
		h.t.Fatal(err)
	}
	_, err = h.mocknet.LinkPeers(h.host.ID(), h2.ID())
	if err != nil {
		h.t.Fatal(err)
	}
	return h2.ID()
}

func (h *harness) conn(observer peer.ID) network.Conn {
	c, err := h.mocknet.ConnectPeers(h.host.ID(), observer)
	if err != nil {
		h.t.Fatal(err)
	}
	if c.Stat().Direction != network.DirOutbound {
		h.t.Fatal("expected conn direction to be outbound")
	}
	return c
}

func (h *harness) connInbound(observer peer.ID) network.Conn {
	c, err := h.mocknet.ConnectPeers(observer, h.host.ID())
	if err != nil {
		h.t.Fatal(err)
	}

	c = mocknet.ConnComplement(c)
	if c.Stat().Direction != network.DirInbound {
		h.t.Fatal("expected conn direction to be inbound")
	}
	return c
}

func (h *harness) observe(observed ma.Multiaddr, observer peer.ID) network.Conn {
	c := h.conn(observer)
	h.oas.Record(c, observed)
	time.Sleep(50 * time.Millisecond) // let the worker run
	return c
}

func (h *harness) observeInbound(observed ma.Multiaddr, observer peer.ID) network.Conn {
	c := h.connInbound(observer)
	h.oas.Record(c, observed)
	time.Sleep(50 * time.Millisecond) // let the worker run
	return c
}

func newHarness(ctx context.Context, t *testing.T) harness {
	mn := mocknet.New(ctx)
	sk, err := p2putil.RandTestBogusPrivateKey()
	if err != nil {
		t.Fatal(err)
	}

	h, err := mn.AddPeer(sk, ma.StringCast("/ip4/127.0.0.1/tcp/10086"))
	if err != nil {
		t.Fatal(err)
	}

	oas, err := identify.NewObservedAddrManager(ctx, h)
	require.NoError(t, err)

	return harness{
		oas:     oas,
		mocknet: mn,
		host:    h,
		t:       t,
	}
}

// TestObsAddrSet
func TestObsAddrSet(t *testing.T) {
	m := func(s string) ma.Multiaddr {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			t.Error(err)
		}
		return m
	}

	addrsMatch := func(a, b []ma.Multiaddr) bool {
		if len(a) != len(b) {
			return false
		}

		for _, aa := range a {
			found := false
			for _, bb := range b {
				if aa.Equal(bb) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	a1 := m("/ip4/1.2.3.4/tcp/1231")
	a2 := m("/ip4/1.2.3.4/tcp/1232")
	a3 := m("/ip4/1.2.3.4/tcp/1233")
	a4 := m("/ip4/1.2.3.4/tcp/1234")
	a5 := m("/ip4/1.2.3.4/tcp/1235")

	b1 := m("/ip4/1.2.3.6/tcp/1236")
	b2 := m("/ip4/1.2.3.7/tcp/1237")
	b3 := m("/ip4/1.2.3.8/tcp/1237")
	b4 := m("/ip4/1.2.3.9/tcp/1237")
	b5 := m("/ip4/1.2.3.10/tcp/1237")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := newHarness(ctx, t)

	if !addrsMatch(harness.oas.Addrs(), nil) {
		t.Error("addrs should be empty")
	}

	pa4 := harness.add(a4)
	pa5 := harness.add(a5)

	pb1 := harness.add(b1)
	pb2 := harness.add(b2)
	pb3 := harness.add(b3)
	pb4 := harness.add(b4)
	pb5 := harness.add(b5)

	harness.observe(a1, pa4)
	harness.observe(a2, pa4)
	harness.observe(a3, pa4)

	// get the address as none has been activated yet.
	harness.observe(a1, pa4)
	harness.observe(a2, pa4)
	harness.observe(a3, pa4)

	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a1, a2}) {
		t.Error("should get expected addresses as there are no activated addresses")
	}

	// different observer, but same observer group.
	harness.observe(a1, pa5)
	harness.observe(a2, pa5)
	harness.observe(a3, pa5)
	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a1, a2}) {
		t.Error("should get expected addresses as there are no activated addresses")
	}

	harness.observe(a1, pb1)
	harness.observe(a1, pb2)
	harness.observe(a1, pb3)
	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a1}) {
		t.Error("addrs should only have a1")
	}

	harness.observe(a2, pa5)
	harness.observe(a1, pa5)
	harness.observe(a1, pa5)
	harness.observe(a2, pb1)
	harness.observe(a1, pb1)
	harness.observe(a1, pb1)
	harness.observe(a2, pb2)
	harness.observe(a1, pb2)
	harness.observe(a1, pb2)
	harness.observe(a2, pb4)
	harness.observe(a2, pb5)
	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a1, a2}) {
		t.Error("addrs should only have a1, a2")
	}

	// force a refresh.
	harness.oas.SetTTL(time.Millisecond * 200)
	<-time.After(time.Millisecond * 210)
	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a1, a2}) {
		t.Error("addrs should only have a1, a2")
	}

	// disconnect from all but b5.
	for _, p := range harness.host.Network().Peers() {
		if p == pb5 {
			continue
		}
		harness.host.Network().ClosePeer(p)
	}

	// wait for all other addresses to time out.
	<-time.After(time.Millisecond * 210)

	// Should still have a2
	if !addrsMatch(harness.oas.Addrs(), []ma.Multiaddr{a2}) {
		t.Error("should only have a2, have: ", harness.oas.Addrs())
	}

	harness.host.Network().ClosePeer(pb5)

	// wait for all addresses to timeout
	<-time.After(time.Millisecond * 400)

	// Should still have a2
	if !addrsMatch(harness.oas.Addrs(), nil) {
		t.Error("addrs should have timed out")
	}
}

func TestAddAddrsProfile(t *testing.T) {
	if detectrace.WithRace() {
		t.Skip("test too slow when the race detector is running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := newHarness(ctx, t)

	addr := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	p := harness.add(ma.StringCast("/ip4/1.2.3.6/tcp/1236"))

	c := harness.conn(p)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				harness.oas.Record(c, addr)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func TestObservedAddrFiltering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newHarness(ctx, t)
	require.Empty(t, harness.oas.Addrs())

	// IP4/TCP
	it1 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	it2 := ma.StringCast("/ip4/1.2.3.4/tcp/1232")
	it3 := ma.StringCast("/ip4/1.2.3.4/tcp/1233")
	it4 := ma.StringCast("/ip4/1.2.3.4/tcp/1234")
	it5 := ma.StringCast("/ip4/1.2.3.4/tcp/1235")
	it6 := ma.StringCast("/ip4/1.2.3.4/tcp/1236")
	it7 := ma.StringCast("/ip4/1.2.3.4/tcp/1237")

	// observers
	b1 := ma.StringCast("/ip4/1.2.3.6/tcp/1236")
	b2 := ma.StringCast("/ip4/1.2.3.7/tcp/1237")
	b3 := ma.StringCast("/ip4/1.2.3.8/tcp/1237")
	b4 := ma.StringCast("/ip4/1.2.3.9/tcp/1237")
	b5 := ma.StringCast("/ip4/1.2.3.10/tcp/1237")

	b6 := ma.StringCast("/ip4/1.2.3.11/tcp/1237")
	b7 := ma.StringCast("/ip4/1.2.3.12/tcp/1237")

	// These are all observers in the same group.
	b8 := ma.StringCast("/ip4/1.2.3.13/tcp/1237")
	b9 := ma.StringCast("/ip4/1.2.3.13/tcp/1238")
	b10 := ma.StringCast("/ip4/1.2.3.13/tcp/1239")

	peers := []peer.ID{
		harness.add(b1),
		harness.add(b2),
		harness.add(b3),
		harness.add(b4),
		harness.add(b5),

		harness.add(b6),
		harness.add(b7),

		harness.add(b8),
		harness.add(b9),
		harness.add(b10),
	}
	for i := 0; i < 4; i++ {
		harness.observe(it1, peers[i])
		harness.observe(it2, peers[i])
		harness.observe(it3, peers[i])
		harness.observe(it4, peers[i])
		harness.observe(it5, peers[i])
		harness.observe(it6, peers[i])
		harness.observe(it7, peers[i])
	}

	harness.observe(it1, peers[4])
	harness.observe(it7, peers[4])

	addrs := harness.oas.Addrs()
	require.Len(t, addrs, 2)
	require.Contains(t, addrs, it1)
	require.Contains(t, addrs, it7)

	// Bump the number of observations so 1 & 7 have 7 observations.
	harness.observe(it1, peers[5])
	harness.observe(it1, peers[6])
	harness.observe(it7, peers[5])
	harness.observe(it7, peers[6])

	// Add an observation from IP 1.2.3.13
	// 2 & 3 now have 5 observations
	harness.observe(it2, peers[7])
	harness.observe(it3, peers[7])

	addrs = harness.oas.Addrs()
	require.Len(t, addrs, 2)
	require.Contains(t, addrs, it1)
	require.Contains(t, addrs, it7)

	// Add an inbound observation from IP 1.2.3.13, it should override the
	// existing observation and it should make these addresses win even
	// though we have fewer observations.
	//
	// 2 & 3 now have 6 observations.
	harness.observeInbound(it2, peers[8])
	harness.observeInbound(it3, peers[8])
	addrs = harness.oas.Addrs()
	require.Len(t, addrs, 2)
	require.Contains(t, addrs, it2)
	require.Contains(t, addrs, it3)

	// Adding an outbound observation shouldn't "downgrade" it.
	//
	// 2 & 3 now have 7 observations.
	harness.observe(it2, peers[9])
	harness.observe(it3, peers[9])
	addrs = harness.oas.Addrs()
	require.Len(t, addrs, 2)
	require.Contains(t, addrs, it2)
	require.Contains(t, addrs, it3)
}

func TestEmitNATDeviceTypeSymmetric(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newHarness(ctx, t)
	require.Empty(t, harness.oas.Addrs())

	emitter, err := harness.host.EventBus().Emitter(new(event.EvtLocalReachabilityChanged), eventbus.Stateful)
	require.NoError(t, err)
	require.NoError(t, emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPrivate}))

	// TCP
	it1 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	it2 := ma.StringCast("/ip4/1.2.3.4/tcp/1232")
	it3 := ma.StringCast("/ip4/1.2.3.4/tcp/1233")
	it4 := ma.StringCast("/ip4/1.2.3.4/tcp/1234")

	// observers
	b1 := ma.StringCast("/ip4/1.2.3.6/tcp/1236")
	b2 := ma.StringCast("/ip4/1.2.3.7/tcp/1237")
	b3 := ma.StringCast("/ip4/1.2.3.8/tcp/1237")
	b4 := ma.StringCast("/ip4/1.2.3.9/tcp/1237")

	peers := []peer.ID{
		harness.add(b1),
		harness.add(b2),
		harness.add(b3),
		harness.add(b4),
	}

	harness.observe(it1, peers[0])
	harness.observe(it2, peers[1])
	harness.observe(it3, peers[2])
	harness.observe(it4, peers[3])

	sub, err := harness.host.EventBus().Subscribe(new(event.EvtNATDeviceTypeChanged))
	require.NoError(t, err)
	select {
	case ev := <-sub.Out():
		evt := ev.(event.EvtNATDeviceTypeChanged)
		require.Equal(t, network.NATDeviceTypeSymmetric, evt.NatDeviceType)
		require.Equal(t, network.NATTransportTCP, evt.TransportProtocol)
	case <-time.After(5 * time.Second):
		t.Fatal("did not get Symmetric NAT event")
	}

	natType, err := harness.host.Peerstore().Get(harness.host.ID(), identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)

	require.Equal(t, network.NATDeviceTypeSymmetric, natType.(network.NATDeviceType))

}

func TestEmitNATDeviceTypeCone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newHarness(ctx, t)
	require.Empty(t, harness.oas.Addrs())

	emitter, err := harness.host.EventBus().Emitter(new(event.EvtLocalReachabilityChanged), eventbus.Stateful)
	require.NoError(t, err)
	require.NoError(t, emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPrivate}))

	it1 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	it2 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	it3 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")
	it4 := ma.StringCast("/ip4/1.2.3.4/tcp/1231")

	// observers
	b1 := ma.StringCast("/ip4/1.2.3.6/tcp/1236")
	b2 := ma.StringCast("/ip4/1.2.3.7/tcp/1237")
	b3 := ma.StringCast("/ip4/1.2.3.8/tcp/1237")
	b4 := ma.StringCast("/ip4/1.2.3.9/tcp/1237")

	peers := []peer.ID{
		harness.add(b1),
		harness.add(b2),
		harness.add(b3),
		harness.add(b4),
	}

	harness.observe(it1, peers[0])
	harness.observe(it2, peers[1])
	harness.observe(it3, peers[2])
	harness.observe(it4, peers[3])

	sub, err := harness.host.EventBus().Subscribe(new(event.EvtNATDeviceTypeChanged))
	require.NoError(t, err)
	select {
	case ev := <-sub.Out():
		evt := ev.(event.EvtNATDeviceTypeChanged)
		require.Equal(t, network.NATDeviceTypeCone, evt.NatDeviceType)
	case <-time.After(5 * time.Second):
		t.Fatal("did not get Cone NAT event")
	}

	natType, err := harness.host.Peerstore().Get(harness.host.ID(), identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)

	require.Equal(t, network.NATDeviceTypeCone, natType.(network.NATDeviceType))
}
