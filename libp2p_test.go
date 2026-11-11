package libp2p

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/core/transport"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	sectls "github.com/libp2p/go-libp2p/p2p/security/tls"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	"github.com/libp2p/go-yamux/v5"
	"github.com/pion/webrtc/v4"
	quicgo "github.com/quic-go/quic-go"
	wtgo "github.com/quic-go/webtransport-go"
	"go.uber.org/goleak"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestNewHost(t *testing.T) {
	h, err := makeRandomHost(t, 9191)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
}

func TestTransportConstructor(t *testing.T) {
	ctor := func(
		_ host.Host,
		_ connmgr.ConnectionGater,
		upgrader transport.Upgrader,
	) transport.Transport {
		tpt, err := tcp.NewTCPTransport(upgrader, nil, nil)
		require.NoError(t, err)
		return tpt
	}
	h, err := New(Transport(ctor))
	require.NoError(t, err)
	h.Close()
}

func TestNoListenAddrs(t *testing.T) {
	h, err := New(NoListenAddrs)
	require.NoError(t, err)
	defer h.Close()
	if len(h.Addrs()) != 0 {
		t.Fatal("expected no addresses")
	}
}

func TestNoTransports(t *testing.T) {
	ctx := context.Background()
	a, err := New(NoTransports)
	require.NoError(t, err)
	defer a.Close()

	b, err := New(ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer b.Close()

	err = a.Connect(ctx, peer.AddrInfo{
		ID:    b.ID(),
		Addrs: b.Addrs(),
	})
	if err == nil {
		t.Error("dial should have failed as no transports have been configured")
	}
}

func TestInsecure(t *testing.T) {
	h, err := New(NoSecurity)
	require.NoError(t, err)
	h.Close()
}

func TestDefaultListenAddrs(t *testing.T) {
	reTCP := regexp.MustCompile("/(ip)[4|6]/((0.0.0.0)|(::))/tcp/")
	reQUIC := regexp.MustCompile("/(ip)[4|6]/((0.0.0.0)|(::))/udp/([0-9]*)/quic-v1")
	reWebRTC := regexp.MustCompile("/(ip)[4|6]/((0.0.0.0)|(::))/udp/([0-9]*)/webrtc-direct/certhash/(.*)")
	reCircuit := regexp.MustCompile("/p2p-circuit")

	// Test 1: Setting the correct listen addresses if userDefined.Transport == nil && userDefined.ListenAddrs == nil
	h, err := New()
	require.NoError(t, err)
	for _, addr := range h.Network().ListenAddresses() {
		if reTCP.FindStringSubmatchIndex(addr.String()) == nil &&
			reQUIC.FindStringSubmatchIndex(addr.String()) == nil &&
			reWebRTC.FindStringSubmatchIndex(addr.String()) == nil &&
			reCircuit.FindStringSubmatchIndex(addr.String()) == nil {
			t.Error("expected ip4 or ip6 or relay interface")
		}
	}

	h.Close()

	// Test 2: Listen addr only include relay if user defined transport is passed.
	h, err = New(Transport(tcp.NewTCPTransport))
	require.NoError(t, err)

	if len(h.Network().ListenAddresses()) != 1 {
		t.Error("expected one listen addr with user defined transport")
	}
	if reCircuit.FindStringSubmatchIndex(h.Network().ListenAddresses()[0].String()) == nil {
		t.Error("expected relay address")
	}
	h.Close()
}

func makeRandomHost(t *testing.T, port int) (host.Host, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)

	return New([]Option{
		ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)),
		Identity(priv),
		DefaultTransports,
		DefaultMuxers,
		DefaultSecurity,
		NATPortMap(),
	}...)
}

func TestChainOptions(t *testing.T) {
	var cfg Config
	var optsRun []int
	optcount := 0
	newOpt := func() Option {
		index := optcount
		optcount++
		return func(_ *Config) error {
			optsRun = append(optsRun, index)
			return nil
		}
	}

	if err := cfg.Apply(newOpt(), nil, ChainOptions(newOpt(), newOpt(), ChainOptions(), ChainOptions(nil, newOpt()))); err != nil {
		t.Fatal(err)
	}

	// Make sure we ran all options.
	if optcount != 4 {
		t.Errorf("expected to have handled %d options, handled %d", optcount, len(optsRun))
	}

	// Make sure we ran the options in-order.
	for i, x := range optsRun {
		if i != x {
			t.Errorf("expected opt %d, got opt %d", i, x)
		}
	}
}

func TestTransportConstructorTCP(t *testing.T) {
	h, err := New(
		Transport(tcp.NewTCPTransport, tcp.DisableReuseport()),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()
	require.NoError(t, h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0")))
	err = h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/quic-v1"))
	require.Error(t, err)
	require.Contains(t, err.Error(), swarm.ErrNoTransport.Error())
}

func TestTransportConstructorQUIC(t *testing.T) {
	h, err := New(
		Transport(quic.NewTransport),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()
	require.NoError(t, h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/quic-v1")))
	err = h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.Error(t, err)
	require.Contains(t, err.Error(), swarm.ErrNoTransport.Error())
}

type mockTransport struct{}

func (m mockTransport) Dial(context.Context, ma.Multiaddr, peer.ID) (transport.CapableConn, error) {
	panic("implement me")
}

func (m mockTransport) CanDial(ma.Multiaddr) bool                       { panic("implement me") }
func (m mockTransport) Listen(ma.Multiaddr) (transport.Listener, error) { panic("implement me") }
func (m mockTransport) Protocols() []int                                { return []int{1337} }
func (m mockTransport) Proxy() bool                                     { panic("implement me") }

var _ transport.Transport = &mockTransport{}

func TestTransportConstructorWithoutOpts(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		var called bool
		constructor := func() transport.Transport {
			called = true
			return &mockTransport{}
		}

		h, err := New(
			Transport(constructor),
			DisableRelay(),
		)
		require.NoError(t, err)
		require.True(t, called, "expected constructor to be called")
		defer h.Close()
	})

	t.Run("with options", func(t *testing.T) {
		var called bool
		constructor := func() transport.Transport {
			called = true
			return &mockTransport{}
		}

		_, err := New(
			Transport(constructor, tcp.DisableReuseport()),
			DisableRelay(),
		)
		require.EqualError(t, err, "transport constructor doesn't take any options")
		require.False(t, called, "didn't expected constructor to be called")
	})
}

func TestTransportConstructorWithWrongOpts(t *testing.T) {
	_, err := New(
		Transport(quic.NewTransport, tcp.DisableReuseport()),
		DisableRelay(),
	)
	require.EqualError(t, err, "transport option of type tcp.Option not assignable to libp2pquic.Option")
}

func TestSecurityConstructor(t *testing.T) {
	h, err := New(
		Transport(tcp.NewTCPTransport),
		Security("/noisy", noise.New),
		Security("/tls", sectls.New),
		DefaultListenAddrs,
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()

	h1, err := New(
		NoListenAddrs,
		Transport(tcp.NewTCPTransport),
		Security("/noise", noise.New), // different name
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h1.Close()

	h2, err := New(
		NoListenAddrs,
		Transport(tcp.NewTCPTransport),
		Security("/noisy", noise.New),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h2.Close()

	ai := peer.AddrInfo{
		ID:    h.ID(),
		Addrs: h.Addrs(),
	}
	err = h1.Connect(context.Background(), ai)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to negotiate security protocol")
	require.NoError(t, h2.Connect(context.Background(), ai))
}

func TestTransportConstructorWebTransport(t *testing.T) {
	h, err := New(
		Transport(webtransport.New),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()
	require.NoError(t, h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/quic-v1/webtransport")))
	err = h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/quic-v1/"))
	require.Error(t, err)
	require.Contains(t, err.Error(), swarm.ErrNoTransport.Error())
}

func TestTransportCustomAddressWebTransport(t *testing.T) {
	customAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/0/quic-v1/webtransport")
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(
		Transport(webtransport.New),
		ListenAddrs(customAddr),
		DisableRelay(),
		AddrsFactory(func(_ []ma.Multiaddr) []ma.Multiaddr {
			return []ma.Multiaddr{customAddr}
		}),
	)
	require.NoError(t, err)
	defer h.Close()
	require.NoError(t, h.Network().Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/quic-v1/webtransport")))
	addrs := h.Addrs()
	require.Len(t, addrs, 1)
	require.NotEqual(t, addrs[0], customAddr)
	restOfAddr, lastComp := ma.SplitLast(addrs[0])
	restOfAddr, secondToLastComp := ma.SplitLast(restOfAddr)
	require.Equal(t, ma.P_CERTHASH, lastComp.Protocol().Code)
	require.Equal(t, ma.P_CERTHASH, secondToLastComp.Protocol().Code)
	require.True(t, restOfAddr.Equal(customAddr))
}

// TestTransportCustomAddressWebTransportDoesNotStall tests that if the user
// manually returns a webtransport address from AddrsFactory, but we aren't
// listening on a webtranport address, we don't stall.
func TestTransportCustomAddressWebTransportDoesNotStall(t *testing.T) {
	customAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/0/quic-v1/webtransport")
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(
		Transport(webtransport.New),
		// Purposely not listening on the custom address so that we make sure the node doesn't stall if it fails to add a certhash to the multiaddr
		// ListenAddrs(customAddr),
		DisableRelay(),
		AddrsFactory(func(_ []ma.Multiaddr) []ma.Multiaddr {
			return []ma.Multiaddr{customAddr}
		}),
	)
	require.NoError(t, err)
	defer h.Close()
	addrs := h.Addrs()
	require.Len(t, addrs, 1)
	_, lastComp := ma.SplitLast(addrs[0])
	require.NotEqual(t, ma.P_CERTHASH, lastComp.Protocol().Code)
	// We did not add the certhash to the multiaddr
	require.Equal(t, addrs[0], customAddr)
}

type mockPeerRouting struct {
	queried []peer.ID
}

func (r *mockPeerRouting) FindPeer(_ context.Context, id peer.ID) (peer.AddrInfo, error) {
	r.queried = append(r.queried, id)
	return peer.AddrInfo{}, errors.New("mock peer routing error")
}

func TestRoutedHost(t *testing.T) {
	mockRouter := &mockPeerRouting{}
	h, err := New(
		NoListenAddrs,
		Routing(func(host.Host) (routing.PeerRouting, error) { return mockRouter, nil }),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	require.EqualError(t, h.Connect(context.Background(), peer.AddrInfo{ID: id}), "mock peer routing error")
	require.Equal(t, []peer.ID{id}, mockRouter.queried)
}

func TestAutoNATService(t *testing.T) {
	h, err := New(EnableNATService())
	require.NoError(t, err)
	h.Close()
}

func TestInsecureConstructor(t *testing.T) {
	h, err := New(
		EnableNATService(),
		NoSecurity,
	)
	require.NoError(t, err)
	h.Close()

	h, err = New(
		NoSecurity,
	)
	require.NoError(t, err)
	h.Close()
}

func TestAutoNATv2Service(t *testing.T) {
	h, err := New(EnableAutoNATv2())
	require.NoError(t, err)
	h.Close()
}

func TestAutoNATv2ForcedReachability(t *testing.T) {
	client, err := New(
		EnableAutoNATv2(),
		ForceReachabilityPrivate(),
	)
	require.NoError(t, err)
	defer client.Close()

	confirmedAddrsHost, ok := client.(interface {
		ConfirmedAddrs() (reachable, unreachable, unknown []ma.Multiaddr)
	})
	require.True(t, ok)
	require.Eventually(t, func() bool {
		reachable, unreachable, unknown := confirmedAddrsHost.ConfirmedAddrs()
		return len(reachable) == 0 && len(unreachable) > 0 && len(unknown) == 0
	}, 10*time.Second, 50*time.Millisecond)
}

func TestDisableIdentifyAddressDiscovery(t *testing.T) {
	h, err := New(DisableIdentifyAddressDiscovery())
	require.NoError(t, err)
	h.Close()
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		// This will return eventually (5s timeout) but doesn't take a context.
		goleak.IgnoreAnyFunction("github.com/koron/go-ssdp.Search"),
		goleak.IgnoreAnyFunction("github.com/pion/sctp.(*Stream).SetReadDeadline.func1"),
		// Stats
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		// nat-pmp
		goleak.IgnoreAnyFunction("github.com/jackpal/go-nat-pmp.(*Client).GetExternalAddress"),
	)
}

func TestDialCircuitAddrWithWrappedResourceManager(t *testing.T) {
	relay, err := New(EnableRelayService(), ForceReachabilityPublic())
	require.NoError(t, err)
	defer relay.Close()

	peerBehindRelay, err := New(
		EnableAutoRelayWithStaticRelays([]peer.AddrInfo{{ID: relay.ID(), Addrs: relay.Addrs()}}),
		ForceReachabilityPrivate())
	require.NoError(t, err)
	defer peerBehindRelay.Close()

	// Use a wrapped resource manager to test that the circuit dialing works
	// with it. Look at the PR introducing this test for context
	type wrappedRcmgr struct{ network.ResourceManager }
	mgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()))
	require.NoError(t, err)
	wmgr := wrappedRcmgr{mgr}
	h, err := New(ResourceManager(wmgr))
	require.NoError(t, err)
	defer h.Close()

	h.Peerstore().AddAddrs(relay.ID(), relay.Addrs(), 10*time.Minute)
	h.Peerstore().AddAddr(peerBehindRelay.ID(),
		ma.StringCast(
			fmt.Sprintf("/p2p/%s/p2p-circuit", relay.ID()),
		),
		peerstore.TempAddrTTL,
	)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		res := <-ping.Ping(ctx, h, peerBehindRelay.ID())
		return res.Error == nil
	}, 5*time.Second, 50*time.Millisecond)
}

func TestHostAddrsFactoryAddsCerthashes(t *testing.T) {
	addr := ma.StringCast("/ip4/1.2.3.4/udp/1/quic-v1/webtransport")
	h, err := New(
		AddrsFactory(func(_ []ma.Multiaddr) []ma.Multiaddr {
			return []ma.Multiaddr{addr}
		}),
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		addrs := h.Addrs()
		for _, a := range addrs {
			first, last := ma.SplitFunc(a, func(c ma.Component) bool {
				return c.Protocol().Code == ma.P_CERTHASH
			})
			if addr.Equal(first) && last != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
	h.Close()
}

func newRandomPort(t *testing.T) string {
	t.Helper()
	// Find an available port
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	c.LocalAddr().Network()
	ipPort := netip.MustParseAddrPort(c.LocalAddr().String())
	port := strconv.Itoa(int(ipPort.Port()))
	require.NoError(t, c.Close())
	return port
}

func TestWebRTCReuseAddrWithQUIC(t *testing.T) {
	port := newRandomPort(t)
	order := [][]string{
		{"/ip4/127.0.0.1/udp/" + port + "/quic-v1", "/ip4/127.0.0.1/udp/" + port + "/webrtc-direct"},
		{"/ip4/127.0.0.1/udp/" + port + "/webrtc-direct", "/ip4/127.0.0.1/udp/" + port + "/quic-v1"},
		// We do not support WebRTC automatically reusing QUIC addresses if port is not specified, yet.
		// {"/ip4/127.0.0.1/udp/0/webrtc-direct", "/ip4/127.0.0.1/udp/0/quic-v1"},
	}
	for i, addrs := range order {
		t.Run("Order "+strconv.Itoa(i), func(t *testing.T) {
			h1, err := New(ListenAddrStrings(addrs...), Transport(quic.NewTransport), Transport(libp2pwebrtc.New))
			require.NoError(t, err)
			defer h1.Close()

			seenPorts := make(map[string]struct{})
			for _, addr := range h1.Addrs() {
				s, err := addr.ValueForProtocol(ma.P_UDP)
				require.NoError(t, err)
				seenPorts[s] = struct{}{}
			}
			require.Len(t, seenPorts, 1)

			quicClient, err := New(NoListenAddrs, Transport(quic.NewTransport))
			require.NoError(t, err)
			defer quicClient.Close()

			webrtcClient, err := New(NoListenAddrs, Transport(libp2pwebrtc.New))
			require.NoError(t, err)
			defer webrtcClient.Close()

			for _, client := range []host.Host{quicClient, webrtcClient} {
				err := client.Connect(context.Background(), peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()})
				require.NoError(t, err)
			}

			t.Run("quic client can connect", func(t *testing.T) {
				ctx := t.Context()
				p := ping.NewPingService(quicClient)
				resCh := p.Ping(ctx, h1.ID())
				res := <-resCh
				require.NoError(t, res.Error)
			})

			t.Run("webrtc client can connect", func(t *testing.T) {
				ctx := t.Context()
				p := ping.NewPingService(webrtcClient)
				resCh := p.Ping(ctx, h1.ID())
				res := <-resCh
				require.NoError(t, res.Error)
			})
		})
	}

	swapPort := func(addrStrs []string, oldPort, newPort string) []string {
		out := make([]string, 0, len(addrStrs))
		for _, addrStr := range addrStrs {
			out = append(out, strings.Replace(addrStr, oldPort, newPort, 1))
		}
		return out
	}

	t.Run("setup with no reuseport. Should fail", func(t *testing.T) {
		oldPort := port
		newPort := newRandomPort(t)
		h1, err := New(ListenAddrStrings(swapPort(order[0], oldPort, newPort)...), Transport(quic.NewTransport), Transport(libp2pwebrtc.New), QUICReuse(quicreuse.NewConnManager, quicreuse.DisableReuseport()))
		require.NoError(t, err) // It's a bug/feature that swarm.Listen does not error if at least one transport succeeds in listening.
		defer h1.Close()
		// Check that webrtc did fail to listen
		require.Equal(t, 1, len(h1.Addrs()))
		require.Contains(t, h1.Addrs()[0].String(), "quic-v1")
	})

	t.Run("setup with autonat", func(t *testing.T) {
		oldPort := port
		newPort := newRandomPort(t)
		h1, err := New(EnableAutoNATv2(), ListenAddrStrings(swapPort(order[0], oldPort, newPort)...), Transport(quic.NewTransport), Transport(libp2pwebrtc.New), QUICReuse(quicreuse.NewConnManager, quicreuse.DisableReuseport()))
		require.NoError(t, err) // It's a bug/feature that swarm.Listen does not error if at least one transport succeeds in listening.
		defer h1.Close()
		// Check that webrtc did fail to listen
		require.Equal(t, 1, len(h1.Addrs()))
		require.Contains(t, h1.Addrs()[0].String(), "quic-v1")
	})
}

func TestUseCorrectTransportForDialOut(t *testing.T) {
	listAddrOrder := [][]string{
		{"/ip4/127.0.0.1/udp/0/quic-v1", "/ip4/127.0.0.1/udp/0/quic-v1/webtransport"},
		{"/ip4/127.0.0.1/udp/0/quic-v1/webtransport", "/ip4/127.0.0.1/udp/0/quic-v1"},
		{"/ip4/0.0.0.0/udp/0/quic-v1", "/ip4/0.0.0.0/udp/0/quic-v1/webtransport"},
		{"/ip4/0.0.0.0/udp/0/quic-v1/webtransport", "/ip4/0.0.0.0/udp/0/quic-v1"},
	}
	for _, order := range listAddrOrder {
		h1, err := New(ListenAddrStrings(order...), Transport(quic.NewTransport), Transport(webtransport.New))
		require.NoError(t, err)
		t.Cleanup(func() {
			h1.Close()
		})

		go func() {
			h1.SetStreamHandler("/echo-port", func(s network.Stream) {
				m := s.Conn().RemoteMultiaddr()
				v, err := m.ValueForProtocol(ma.P_UDP)
				if err != nil {
					s.Reset()
					return
				}
				s.Write([]byte(v))
				s.Close()
			})
		}()

		for _, addr := range h1.Addrs() {
			t.Run("order "+strings.Join(order, ",")+" Dial to "+addr.String(), func(t *testing.T) {
				h2, err := New(ListenAddrStrings(
					"/ip4/0.0.0.0/udp/0/quic-v1",
					"/ip4/0.0.0.0/udp/0/quic-v1/webtransport",
				), Transport(quic.NewTransport), Transport(webtransport.New))
				require.NoError(t, err)
				defer h2.Close()
				t.Log("H2 Addrs", h2.Addrs())
				var myExpectedDialOutAddr ma.Multiaddr
				addrIsWT, _ := webtransport.IsWebtransportMultiaddr(addr)
				isLocal := func(a ma.Multiaddr) bool {
					return strings.Contains(a.String(), "127.0.0.1")
				}
				addrIsLocal := isLocal(addr)
				for _, a := range h2.Addrs() {
					aIsWT, _ := webtransport.IsWebtransportMultiaddr(a)
					if addrIsWT == aIsWT && isLocal(a) == addrIsLocal {
						myExpectedDialOutAddr = a
						break
					}
				}

				err = h2.Connect(context.Background(), peer.AddrInfo{ID: h1.ID(), Addrs: []ma.Multiaddr{addr}})
				require.NoError(t, err)

				s, err := h2.NewStream(context.Background(), h1.ID(), "/echo-port")
				require.NoError(t, err)

				port, err := io.ReadAll(s)
				require.NoError(t, err)

				myExpectedPort, err := myExpectedDialOutAddr.ValueForProtocol(ma.P_UDP)
				require.NoError(t, err)
				require.Equal(t, myExpectedPort, string(port))
			})
		}
	}
}

func TestCircuitBehindWSS(t *testing.T) {
	relayTLSConf := getTLSConf(t, net.IPv4(127, 0, 0, 1), time.Now(), time.Now().Add(time.Hour))
	serverNameChan := make(chan string, 2) // Channel that returns what server names the client hello specified
	relayTLSConf.GetConfigForClient = func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
		serverNameChan <- chi.ServerName
		return relayTLSConf, nil
	}

	relay, err := New(
		EnableRelayService(),
		ForceReachabilityPublic(),
		Transport(websocket.New, websocket.WithTLSConfig(relayTLSConf)),
		ListenAddrStrings("/ip4/127.0.0.1/tcp/0/wss"),
	)
	require.NoError(t, err)
	defer relay.Close()

	relayAddrPort, _ := relay.Addrs()[0].ValueForProtocol(ma.P_TCP)
	relayAddrWithSNIString := fmt.Sprintf(
		"/dns4/localhost/tcp/%s/wss", relayAddrPort,
	)
	relayAddrWithSNI := []ma.Multiaddr{ma.StringCast(relayAddrWithSNIString)}

	h, err := New(
		NoListenAddrs,
		EnableRelay(),
		Transport(websocket.New, websocket.WithTLSClientConfig(&tls.Config{InsecureSkipVerify: true})),
		ForceReachabilityPrivate())
	require.NoError(t, err)
	defer h.Close()

	peerBehindRelay, err := New(
		NoListenAddrs,
		Transport(websocket.New, websocket.WithTLSClientConfig(&tls.Config{InsecureSkipVerify: true})),
		EnableRelay(),
		EnableAutoRelayWithStaticRelays([]peer.AddrInfo{{ID: relay.ID(), Addrs: relayAddrWithSNI}}),
		ForceReachabilityPrivate())
	require.NoError(t, err)
	defer peerBehindRelay.Close()

	require.Equal(t,
		"localhost",
		<-serverNameChan, // The server connects to the relay
	)

	// Connect to the peer behind the relay
	h.Connect(context.Background(), peer.AddrInfo{
		ID: peerBehindRelay.ID(),
		Addrs: []ma.Multiaddr{ma.StringCast(
			fmt.Sprintf("%s/p2p/%s/p2p-circuit", relayAddrWithSNIString, relay.ID()),
		)},
	})
	require.NoError(t, err)

	require.Equal(t,
		"localhost",
		<-serverNameChan, // The client connects to the relay and sends the SNI
	)
}

// getTLSConf is a helper to generate a self-signed TLS config
func getTLSConf(t *testing.T, ip net.IP, start, end time.Time) *tls.Config {
	t.Helper()
	certTempl := &x509.Certificate{
		SerialNumber:          big.NewInt(1234),
		Subject:               pkix.Name{Organization: []string{"websocket"}},
		NotBefore:             start,
		NotAfter:              end,
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{ip},
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caBytes, err := x509.CreateCertificate(rand.Reader, certTempl, certTempl, &priv.PublicKey, priv)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(caBytes)
	require.NoError(t, err)
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{cert.Raw},
			PrivateKey:  priv,
			Leaf:        cert,
		}},
	}
}

func TestSharedTCPAddr(t *testing.T) {
	h, err := New(
		ShareTCPListener(),
		Transport(tcp.NewTCPTransport),
		Transport(websocket.New),
		ListenAddrStrings("/ip4/0.0.0.0/tcp/8188"),
		ListenAddrStrings("/ip4/0.0.0.0/tcp/8188/ws"),
	)
	require.NoError(t, err)
	defer h.Close()
	sawTCP := false
	sawWS := false
	for _, addr := range h.Addrs() {
		if strings.HasSuffix(addr.String(), "/tcp/8188") {
			sawTCP = true
		}
		if strings.HasSuffix(addr.String(), "/tcp/8188/ws") {
			sawWS = true
		}
	}
	require.True(t, sawTCP)
	require.True(t, sawWS)

	_, err = New(
		ShareTCPListener(),
		Transport(tcp.NewTCPTransport),
		Transport(websocket.New),
		PrivateNetwork(pnet.PSK([]byte{1, 2, 3})),
	)
	require.ErrorContains(t, err, "cannot use shared TCP listener with PSK")
}

func TestCustomTCPDialer(t *testing.T) {
	expectedErr := errors.New("custom dialer called, but not implemented")
	customDialer := func(_ ma.Multiaddr) (tcp.ContextDialer, error) {
		// Normally a user would implement this by returning a custom dialer
		// Here, we just test that this is called.
		return nil, expectedErr
	}

	h, err := New(
		Transport(tcp.NewTCPTransport, tcp.WithDialerForAddr(customDialer)),
	)
	require.NoError(t, err)
	defer h.Close()

	var randID peer.ID
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	randID, err = peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	err = h.Connect(context.Background(), peer.AddrInfo{
		ID: randID,
		// This won't actually be dialed since we return an error above
		Addrs: []ma.Multiaddr{ma.StringCast("/ip4/1.2.3.4/tcp/4")},
	})
	require.ErrorContains(t, err, expectedErr.Error())
}

func TestBasicHostInterfaceAssertion(t *testing.T) {
	mockRouter := &mockPeerRouting{}
	h, err := New(
		NoListenAddrs,
		Routing(func(host.Host) (routing.PeerRouting, error) { return mockRouter, nil }),
		DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()

	require.NotNil(t, h)
	require.NotEmpty(t, h.ID())

	_, ok := h.(interface{ AllAddrs() []ma.Multiaddr })
	require.True(t, ok)
}

func BenchmarkAllAddrs(b *testing.B) {
	h, err := New()

	addrsHost := h.(interface{ AllAddrs() []ma.Multiaddr })
	require.NoError(b, err)
	defer h.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addrsHost.AllAddrs()
	}
}

func TestConnAs(t *testing.T) {
	type testCase struct {
		name       string
		listenAddr string
		testAs     func(t *testing.T, c network.Conn)
	}

	testCases := []testCase{
		{
			"QUIC",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			func(t *testing.T, c network.Conn) {
				var quicConn *quicgo.Conn
				require.True(t, c.As(&quicConn))
			},
		},
		{
			"TCP+Yamux",
			"/ip4/0.0.0.0/tcp/0",
			func(t *testing.T, c network.Conn) {
				var yamuxSession *yamux.Session
				require.True(t, c.As(&yamuxSession))
			},
		},
		{
			"WebRTC",
			"/ip4/0.0.0.0/udp/0/webrtc-direct",
			func(t *testing.T, c network.Conn) {
				var webrtcPC *webrtc.PeerConnection
				require.True(t, c.As(&webrtcPC))
			},
		},
		{
			"WebTransport Session",
			"/ip4/0.0.0.0/udp/0/quic-v1/webtransport",
			func(t *testing.T, c network.Conn) {
				var s *wtgo.Session
				require.True(t, c.As(&s))
			},
		},
		{
			"WebTransport QUIC Conn",
			"/ip4/0.0.0.0/udp/0/quic-v1/webtransport",
			func(t *testing.T, c network.Conn) {
				var quicConn *quicgo.Conn
				require.True(t, c.As(&quicConn))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h1, err := New(ListenAddrStrings(
				tc.listenAddr,
			))
			require.NoError(t, err)
			defer h1.Close()
			h2, err := New(ListenAddrStrings(
				tc.listenAddr,
			))
			require.NoError(t, err)
			defer h2.Close()
			err = h1.Connect(context.Background(), peer.AddrInfo{
				ID:    h2.ID(),
				Addrs: h2.Addrs(),
			})
			require.NoError(t, err)
			c := h1.Network().ConnsToPeer(h2.ID())[0]
			tc.testAs(t, c)
		})
	}
}
