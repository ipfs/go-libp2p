package swarm

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	libp2pwebtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	"github.com/quic-go/quic-go"

	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	matest "github.com/multiformats/go-multiaddr/matest"
	"github.com/stretchr/testify/require"
)

func TestAddrsForDial(t *testing.T) {
	mockResolver := madns.MockResolver{IP: make(map[string][]net.IPAddr)}
	ipaddr, err := net.ResolveIPAddr("ip4", "1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}
	mockResolver.IP["example.com"] = []net.IPAddr{*ipaddr}

	resolver, err := madns.NewResolver(madns.WithDomainResolver("example.com", &mockResolver))
	if err != nil {
		t.Fatal(err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	ps.AddPubKey(id, priv.GetPublic())
	ps.AddPrivKey(id, priv)
	t.Cleanup(func() { ps.Close() })

	tpt, err := websocket.New(nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	s, err := NewSwarm(id, ps, eventbus.NewBus(), WithMultiaddrResolver(ResolverFromMaDNS{resolver}))
	require.NoError(t, err)
	defer s.Close()
	err = s.AddTransport(tpt)
	require.NoError(t, err)

	otherPeer := test.RandPeerIDFatal(t)

	ps.AddAddr(otherPeer, ma.StringCast("/dns4/example.com/tcp/1234/wss"), time.Hour)

	ctx := context.Background()
	mas, _, err := s.addrsForDial(ctx, otherPeer)
	require.NoError(t, err)

	require.NotZero(t, len(mas))
}

func TestDedupAddrsForDial(t *testing.T) {
	mockResolver := madns.MockResolver{IP: make(map[string][]net.IPAddr)}
	ipaddr, err := net.ResolveIPAddr("ip4", "1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}
	mockResolver.IP["example.com"] = []net.IPAddr{*ipaddr}

	resolver, err := madns.NewResolver(madns.WithDomainResolver("example.com", &mockResolver))
	if err != nil {
		t.Fatal(err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	ps.AddPubKey(id, priv.GetPublic())
	ps.AddPrivKey(id, priv)
	t.Cleanup(func() { ps.Close() })

	s, err := NewSwarm(id, ps, eventbus.NewBus(), WithMultiaddrResolver(ResolverFromMaDNS{resolver}))
	require.NoError(t, err)
	defer s.Close()

	tpt, err := tcp.NewTCPTransport(nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	err = s.AddTransport(tpt)
	require.NoError(t, err)

	otherPeer := test.RandPeerIDFatal(t)

	ps.AddAddr(otherPeer, ma.StringCast("/dns4/example.com/tcp/1234"), time.Hour)
	ps.AddAddr(otherPeer, ma.StringCast("/ip4/1.2.3.4/tcp/1234"), time.Hour)

	ctx := context.Background()
	mas, _, err := s.addrsForDial(ctx, otherPeer)
	require.NoError(t, err)

	require.Len(t, mas, 1)
}

func newTestSwarmWithResolver(t *testing.T, resolver *madns.Resolver) *Swarm {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	ps.AddPubKey(id, priv.GetPublic())
	ps.AddPrivKey(id, priv)
	t.Cleanup(func() { ps.Close() })
	s, err := NewSwarm(id, ps, eventbus.NewBus(), WithMultiaddrResolver(ResolverFromMaDNS{resolver}))
	require.NoError(t, err)
	t.Cleanup(func() {
		s.Close()
	})

	// Add a tcp transport so that we know we can dial a tcp multiaddr and we don't filter it out.
	tpt, err := tcp.NewTCPTransport(nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	err = s.AddTransport(tpt)
	require.NoError(t, err)

	connmgr, err := quicreuse.NewConnManager(quic.StatelessResetKey{}, quic.TokenGeneratorKey{})
	require.NoError(t, err)
	quicTpt, err := libp2pquic.NewTransport(priv, connmgr, nil, nil, &network.NullResourceManager{})
	require.NoError(t, err)
	err = s.AddTransport(quicTpt)
	require.NoError(t, err)

	wtTpt, err := libp2pwebtransport.New(priv, nil, connmgr, nil, &network.NullResourceManager{})
	require.NoError(t, err)
	err = s.AddTransport(wtTpt)
	require.NoError(t, err)

	wsTpt, err := websocket.New(nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	err = s.AddTransport(wsTpt)
	require.NoError(t, err)

	return s
}

func TestAddrResolution(t *testing.T) {
	ctx := context.Background()

	p1 := test.RandPeerIDFatal(t)
	p2 := test.RandPeerIDFatal(t)
	addr1 := ma.StringCast("/dnsaddr/example.com")
	addr2 := ma.StringCast("/ip4/192.0.2.1/tcp/123")

	p2paddr2 := ma.StringCast("/ip4/192.0.2.1/tcp/123/p2p/" + p1.String())
	p2paddr3 := ma.StringCast("/ip4/192.0.2.1/tcp/123/p2p/" + p2.String())

	backend := &madns.MockResolver{
		TXT: map[string][]string{"_dnsaddr.example.com": {
			"dnsaddr=" + p2paddr2.String(), "dnsaddr=" + p2paddr3.String(),
		}},
	}
	resolver, err := madns.NewResolver(madns.WithDefaultResolver(backend))
	require.NoError(t, err)

	s := newTestSwarmWithResolver(t, resolver)

	s.peers.AddAddr(p1, addr1, time.Hour)

	tctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	mas, _, err := s.addrsForDial(tctx, p1)
	require.NoError(t, err)

	require.Len(t, mas, 1)
	matest.AssertMultiaddrsContain(t, mas, addr2)

	addrs := s.peers.Addrs(p1)
	require.Len(t, addrs, 2)
	matest.AssertMultiaddrsContain(t, addrs, addr1)
	matest.AssertMultiaddrsContain(t, addrs, addr2)
}

func TestAddrResolutionRecursive(t *testing.T) {
	p1 := test.RandPeerIDFatal(t)
	p2 := test.RandPeerIDFatal(t)

	addr1 := ma.StringCast("/dnsaddr/example.com")
	addr2 := ma.StringCast("/ip4/192.0.2.1/tcp/123")
	p2paddr1 := ma.StringCast("/dnsaddr/example.com/p2p/" + p1.String())
	p2paddr2 := ma.StringCast("/dnsaddr/example.com/p2p/" + p2.String())
	p2paddr1i := ma.StringCast("/dnsaddr/foo.example.com/p2p/" + p1.String())
	p2paddr2i := ma.StringCast("/dnsaddr/bar.example.com/p2p/" + p2.String())
	p2paddr1f := ma.StringCast("/ip4/192.0.2.1/tcp/123/p2p/" + p1.String())

	backend := &madns.MockResolver{
		TXT: map[string][]string{
			"_dnsaddr.example.com": {
				"dnsaddr=" + p2paddr1i.String(),
				"dnsaddr=" + p2paddr2i.String(),
			},
			"_dnsaddr.foo.example.com": {"dnsaddr=" + p2paddr1f.String()},
			"_dnsaddr.bar.example.com": {"dnsaddr=" + p2paddr2i.String()},
		},
	}
	resolver, err := madns.NewResolver(madns.WithDefaultResolver(backend))
	require.NoError(t, err)

	s := newTestSwarmWithResolver(t, resolver)

	pi1, err := peer.AddrInfoFromP2pAddr(p2paddr1)
	require.NoError(t, err)

	tctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	s.Peerstore().AddAddrs(pi1.ID, pi1.Addrs, peerstore.TempAddrTTL)
	_, _, err = s.addrsForDial(tctx, p1)
	require.NoError(t, err)

	addrs1 := s.Peerstore().Addrs(pi1.ID)
	require.Len(t, addrs1, 2)
	matest.AssertMultiaddrsContain(t, addrs1, addr1)
	matest.AssertMultiaddrsContain(t, addrs1, addr2)

	pi2, err := peer.AddrInfoFromP2pAddr(p2paddr2)
	require.NoError(t, err)

	s.Peerstore().AddAddrs(pi2.ID, pi2.Addrs, peerstore.TempAddrTTL)
	_, _, err = s.addrsForDial(tctx, p2)
	// This never resolves to a good address
	require.Equal(t, ErrNoGoodAddresses, err)

	addrs2 := s.Peerstore().Addrs(pi2.ID)
	require.Len(t, addrs2, 1)
	matest.AssertMultiaddrsContain(t, addrs2, addr1)
}

// see https://github.com/libp2p/go-libp2p/issues/2562
func TestAddrResolutionRecursiveTransportSpecific(t *testing.T) {
	p := test.RandPeerIDFatal(t)

	backend := &madns.MockResolver{
		IP: map[string][]net.IPAddr{
			"sub.example.com": {net.IPAddr{IP: net.IPv4(1, 2, 3, 4)}},
		},
		TXT: map[string][]string{
			"_dnsaddr.example.com": {"dnsaddr=/dns4/sub.example.com/tcp/443/wss/p2p/" + p.String()},
		},
	}
	resolver, err := madns.NewResolver(madns.WithDefaultResolver(backend))
	require.NoError(t, err)

	s := newTestSwarmWithResolver(t, resolver)
	pi1, err := peer.AddrInfoFromP2pAddr(ma.StringCast("/dnsaddr/example.com/p2p/" + p.String()))
	require.NoError(t, err)

	tctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	s.Peerstore().AddAddrs(pi1.ID, pi1.Addrs, peerstore.TempAddrTTL)
	addrs, _, err := s.addrsForDial(tctx, p)
	require.NoError(t, err)
	require.Len(t, addrs, 1)
	require.Equal(t, "/ip4/1.2.3.4/tcp/443/tls/sni/sub.example.com/ws", addrs[0].String())
}

func TestAddrsForDialFiltering(t *testing.T) {
	q1 := ma.StringCast("/ip4/1.2.3.4/udp/1/quic-v1")
	q1v1 := ma.StringCast("/ip4/1.2.3.4/udp/1/quic-v1")
	wt1 := ma.StringCast("/ip4/1.2.3.4/udp/1/quic-v1/webtransport/")

	q2 := ma.StringCast("/ip4/1.2.3.4/udp/2/quic-v1")
	q2v1 := ma.StringCast("/ip4/1.2.3.4/udp/2/quic-v1")
	wt2 := ma.StringCast("/ip4/1.2.3.4/udp/2/quic-v1/webtransport/")

	q3 := ma.StringCast("/ip4/1.2.3.4/udp/3/quic-v1")

	t1 := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	ws1 := ma.StringCast("/ip4/1.2.3.4/tcp/1/ws")

	unSpecQ := ma.StringCast("/ip4/0.0.0.0/udp/2/quic-v1")
	unSpecT := ma.StringCast("/ip6/::/tcp/2/")

	resolver, err := madns.NewResolver(madns.WithDefaultResolver(&madns.MockResolver{}))
	require.NoError(t, err)
	s := newTestSwarmWithResolver(t, resolver)
	ourAddrs := s.ListenAddresses()

	testCases := []struct {
		name   string
		input  []ma.Multiaddr
		output []ma.Multiaddr
	}{
		{
			name:   "quic-filtered",
			input:  []ma.Multiaddr{q1, q1v1, q2, q2v1, q3},
			output: []ma.Multiaddr{q1v1, q2v1, q3},
		},
		{
			name:   "webtransport-filtered",
			input:  []ma.Multiaddr{q1, q1v1, wt1, wt2},
			output: []ma.Multiaddr{q1v1, wt2},
		},
		{
			name:   "all",
			input:  []ma.Multiaddr{q1, q1v1, wt1, q2, q2v1, wt2, t1, ws1},
			output: []ma.Multiaddr{q1v1, q2v1, t1},
		},
		{
			name:   "our-addrs-filtered",
			input:  append([]ma.Multiaddr{q1}, ourAddrs...),
			output: []ma.Multiaddr{q1},
		},
		{
			name:   "unspecified-filtered",
			input:  []ma.Multiaddr{q1v1, t1, unSpecQ, unSpecT},
			output: []ma.Multiaddr{q1v1, t1},
		},
	}

	ctx := context.Background()
	p1 := test.RandPeerIDFatal(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s.Peerstore().ClearAddrs(p1)
			s.Peerstore().AddAddrs(p1, tc.input, peerstore.PermanentAddrTTL)
			result, _, err := s.addrsForDial(ctx, p1)
			require.NoError(t, err)
			sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Bytes(), result[j].Bytes()) < 0 })
			sort.Slice(tc.output, func(i, j int) bool { return bytes.Compare(tc.output[i].Bytes(), tc.output[j].Bytes()) < 0 })
			if len(result) != len(tc.output) {
				t.Fatalf("output mismatch got: %s want: %s", result, tc.output)
			}
			for i := range result {
				if !result[i].Equal(tc.output[i]) {
					t.Fatalf("output mismatch got: %s want: %s", result, tc.output)
				}
			}
		})
	}
}

func TestBlackHoledAddrBlocked(t *testing.T) {
	resolver, err := madns.NewResolver()
	if err != nil {
		t.Fatal(err)
	}
	s := newTestSwarmWithResolver(t, resolver)
	defer s.Close()

	n := 3
	s.bhd.ipv6 = &BlackHoleSuccessCounter{N: n, MinSuccesses: 1, Name: "IPv6"}

	// All dials to this addr will fail.
	// manet.IsPublic is aggressive for IPv6 addresses. Use a NAT64 address.
	addr := ma.StringCast("/ip6/64:ff9b::1.2.3.4/tcp/54321/")

	p, err := test.RandPeerID()
	if err != nil {
		t.Error(err)
	}
	s.Peerstore().AddAddr(p, addr, peerstore.PermanentAddrTTL)

	// do 1 extra dial to ensure that the blackHoleDetector state is updated since it
	// happens in a different goroutine
	for i := 0; i < n+1; i++ {
		s.backf.Clear(p)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		conn, err := s.DialPeer(ctx, p)
		if err == nil || conn != nil {
			t.Fatalf("expected dial to fail")
		}
		cancel()
	}
	s.backf.Clear(p)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := s.DialPeer(ctx, p)
	require.Nil(t, conn)
	if _, ok := errors.AsType[*DialError](err); !ok {
		t.Fatalf("expected to receive an error of type *DialError, got %s of type %T", err, err)
	}
	require.ErrorIs(t, err, ErrDialRefusedBlackHole)
}

type mockDNSResolver struct {
	ipsToReturn  []net.IPAddr
	txtsToReturn []string
}

var _ madns.BasicResolver = (*mockDNSResolver)(nil)

func (m *mockDNSResolver) LookupIPAddr(_ context.Context, _ string) ([]net.IPAddr, error) {
	return m.ipsToReturn, nil
}

func (m *mockDNSResolver) LookupTXT(_ context.Context, _ string) ([]string, error) {
	return m.txtsToReturn, nil
}

func TestSkipDialingManyDNS(t *testing.T) {
	resolver, err := madns.NewResolver(madns.WithDefaultResolver(&mockDNSResolver{ipsToReturn: []net.IPAddr{{IP: net.ParseIP("1.2.3.4")}, {IP: net.ParseIP("1.2.3.5")}}}))
	if err != nil {
		t.Fatal(err)
	}
	s := newTestSwarmWithResolver(t, resolver)
	defer s.Close()
	id := test.RandPeerIDFatal(t)
	addr := ma.StringCast("/dns/example.com/udp/1234/p2p-circuit/dns/example.com/p2p-circuit/dns/example.com")

	resolved := s.resolveAddrs(context.Background(), peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{addr}})
	require.NoError(t, err)
	require.Less(t, len(resolved), 3, "got: %v", resolved)
}

// TestAddrsForDialForceDirectAfterClose checks that a direct dial that races
// with Swarm.Close does not panic. Close drops the transports, so
// TransportForDialing returns nil; such an address must be filtered out
// rather than dereferenced.
func TestAddrsForDialForceDirectAfterClose(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	ps.AddPubKey(id, priv.GetPublic())
	ps.AddPrivKey(id, priv)
	t.Cleanup(func() { ps.Close() })

	tpt, err := websocket.New(nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	s, err := NewSwarm(id, ps, eventbus.NewBus())
	require.NoError(t, err)
	require.NoError(t, s.AddTransport(tpt))

	addr := ma.StringCast("/ip4/1.2.3.4/tcp/1234/ws")
	require.True(t, s.nonProxyAddr(addr))

	otherPeer := test.RandPeerIDFatal(t)
	ps.AddAddr(otherPeer, addr, time.Hour)
	require.NoError(t, s.Close())

	require.NotPanics(t, func() {
		require.False(t, s.nonProxyAddr(addr))
		ctx := network.WithForceDirectDial(context.Background(), "test")
		_, _, _ = s.addrsForDial(ctx, otherPeer)
	})
}
