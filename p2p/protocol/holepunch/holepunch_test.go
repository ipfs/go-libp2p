package holepunch_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	holepunch_pb "github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/marcopolo/simnet"
	"go.uber.org/fx"

	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEventTracer struct {
	mutex  sync.Mutex
	events []*holepunch.Event
}

func (m *mockEventTracer) Trace(evt *holepunch.Event) {
	m.mutex.Lock()
	m.events = append(m.events, evt)
	m.mutex.Unlock()
}

func (m *mockEventTracer) getEvents() []*holepunch.Event {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// copy the slice
	return append([]*holepunch.Event{}, m.events...)
}

var _ holepunch.EventTracer = &mockEventTracer{}

type mockMaddrFilter struct {
	filterLocal  func(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr
	filterRemote func(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr
}

func (m mockMaddrFilter) FilterLocal(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr {
	return m.filterLocal(remoteID, maddrs)
}

func (m mockMaddrFilter) FilterRemote(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr {
	return m.filterRemote(remoteID, maddrs)
}

var _ holepunch.AddrFilter = &mockMaddrFilter{}

func newIDService(t *testing.T, h host.Host) identify.IDService {
	ids, err := identify.NewIDService(h)
	require.NoError(t, err)
	ids.Start()
	t.Cleanup(func() { ids.Close() })
	return ids
}

func TestNoHolePunchIfDirectConnExists(t *testing.T) {
	router := &simnet.SimpleFirewallRouter{}
	relay := MustNewHost(t,
		quicSimnet(true, router),
		libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
		libp2p.DisableRelay(),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
			// Setup relay service
			_, err := relayv2.New(h)
			require.NoError(t, err)
		})),
	)

	tr := &mockEventTracer{}
	h1 := MustNewHost(t,
		quicSimnet(false, router),
		libp2p.EnableHolePunching(holepunch.DirectDialTimeout(100*time.Millisecond)),
		libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8000/quic-v1")),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)

	h2 := MustNewHost(t,
		quicSimnet(true, router),
		libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.ForceReachabilityPublic(),
		connectToRelay(&relay),
		libp2p.EnableHolePunching(holepunch.WithTracer(tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
	)

	defer h1.Close()
	defer h2.Close()
	defer relay.Close()

	waitForHolePunchingSvcActive(t, h1)
	waitForHolePunchingSvcActive(t, h2)

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.ConnectedAddrTTL)
	// try to hole punch without any connection and streams, if it works -> it's a direct connection
	require.Empty(t, h1.Network().ConnsToPeer(h2.ID()))
	pingAtoB(t, h1, h2)

	nc1 := len(h1.Network().ConnsToPeer(h2.ID()))
	require.Equal(t, nc1, 1)
	nc2 := len(h2.Network().ConnsToPeer(h1.ID()))
	require.Equal(t, nc2, 1)
	assert.Never(t, func() bool {
		return (len(h1.Network().ConnsToPeer(h2.ID())) != nc1 ||
			len(h2.Network().ConnsToPeer(h1.ID())) != nc2 ||
			len(tr.getEvents()) != 0)
	}, time.Second, 100*time.Millisecond)
}

func TestDirectDialWorks(t *testing.T) {
	router := &simnet.SimpleFirewallRouter{}
	relay := MustNewHost(t,
		quicSimnet(true, router),
		libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
		libp2p.DisableRelay(),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
			// Setup relay service
			_, err := relayv2.New(h)
			require.NoError(t, err)
		})),
	)

	tr := &mockEventTracer{}
	// h1 is public
	h1 := MustNewHost(t,
		quicSimnet(true, router),
		libp2p.ForceReachabilityPublic(),
		libp2p.EnableHolePunching(holepunch.DirectDialTimeout(100*time.Millisecond)),
		libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8000/quic-v1")),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)

	h2 := MustNewHost(t,
		quicSimnet(false, router),
		libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		connectToRelay(&relay),
		libp2p.EnableHolePunching(holepunch.WithTracer(tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
		libp2p.ForceReachabilityPrivate(),
	)

	defer h1.Close()
	defer h2.Close()
	defer relay.Close()

	// wait for dcutr to be available
	waitForHolePunchingSvcActive(t, h2)

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.ConnectedAddrTTL)
	// try to hole punch without any connection and streams, if it works -> it's a direct connection
	require.Empty(t, h1.Network().ConnsToPeer(h2.ID()))
	pingAtoB(t, h1, h2)

	// require.NoError(t, h1ps.DirectConnect(h2.ID()))
	require.GreaterOrEqual(t, len(h1.Network().ConnsToPeer(h2.ID())), 1)
	require.GreaterOrEqual(t, len(h2.Network().ConnsToPeer(h1.ID())), 1)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := tr.getEvents()
		fmt.Println("events:", events)
		if !assert.Len(collect, events, 1) {
			return
		}
		assert.Equal(t, holepunch.DirectDialEvtT, events[0].Type)
	}, 2*time.Second, 100*time.Millisecond)
}

func connectToRelay(relayPtr *host.Host) libp2p.Option {
	return func(cfg *libp2p.Config) error {
		if relayPtr == nil {
			return nil
		}
		relay := *relayPtr
		pi := peer.AddrInfo{
			ID:    relay.ID(),
			Addrs: relay.Addrs(),
		}

		return cfg.Apply(
			libp2p.EnableRelay(),
			libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{pi}),
		)
	}
}

func learnAddrs(h1, h2 host.Host) {
	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.ConnectedAddrTTL)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.ConnectedAddrTTL)
}

func pingAtoB(t *testing.T, a, b host.Host) {
	t.Helper()
	p1 := ping.NewPingService(a)
	require.NoError(t, a.Connect(context.Background(), peer.AddrInfo{
		ID:    b.ID(),
		Addrs: b.Addrs(),
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := p1.Ping(ctx, b.ID())
	result := <-res
	require.NoError(t, result.Error)
}

func MustNewHost(t *testing.T, opts ...libp2p.Option) host.Host {
	t.Helper()
	h, err := libp2p.New(opts...)
	require.NoError(t, err)
	return h
}

func TestEndToEndSimConnect(t *testing.T) {
	for _, useLegacyHolePunchingBehavior := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%t", useLegacyHolePunchingBehavior), func(t *testing.T) {
			h1tr := &mockEventTracer{}
			h2tr := &mockEventTracer{}

			router := &simnet.SimpleFirewallRouter{}
			relay := MustNewHost(t,
				quicSimnet(true, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
				libp2p.DisableRelay(),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
					// Setup relay service
					_, err := relayv2.New(h)
					require.NoError(t, err)
				})),
			)

			h1 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.EnableHolePunching(holepunch.WithTracer(h1tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8000/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				libp2p.ForceReachabilityPrivate(),
			)

			h2 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				connectToRelay(&relay),
				libp2p.EnableHolePunching(holepunch.WithTracer(h2tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
				libp2p.ForceReachabilityPrivate(),
			)

			defer h1.Close()
			defer h2.Close()
			defer relay.Close()

			// Wait for holepunch service to start
			waitForHolePunchingSvcActive(t, h1)
			waitForHolePunchingSvcActive(t, h2)

			learnAddrs(h1, h2)
			pingAtoB(t, h1, h2)

			// wait till a direct connection is complete
			ensureDirectConn(t, h1, h2)
			// ensure no hole-punching streams are open on either side
			ensureNoHolePunchingStream(t, h1, h2)
			var h2Events []*holepunch.Event
			require.Eventually(t,
				func() bool {
					h2Events = h2tr.getEvents()
					return len(h2Events) == 4
				},
				time.Second,
				100*time.Millisecond,
			)
			require.Equal(t, holepunch.DirectDialEvtT, h2Events[0].Type)
			require.Equal(t, holepunch.StartHolePunchEvtT, h2Events[1].Type)
			require.Equal(t, holepunch.HolePunchAttemptEvtT, h2Events[2].Type)
			require.Equal(t, holepunch.EndHolePunchEvtT, h2Events[3].Type)

			h1Events := h1tr.getEvents()
			// We don't really expect a hole-punched connection to be established in this test,
			// as we probably don't get the timing right for the TCP simultaneous open.
			// From time to time, it still happens occasionally, and then we get a EndHolePunchEvtT here.
			if len(h1Events) != 2 && len(h1Events) != 3 {
				t.Fatal("expected either 2 or 3 events")
			}
			require.Equal(t, holepunch.StartHolePunchEvtT, h1Events[0].Type)
			require.Equal(t, holepunch.HolePunchAttemptEvtT, h1Events[1].Type)
			if len(h1Events) == 3 {
				require.Equal(t, holepunch.EndHolePunchEvtT, h1Events[2].Type)
			}
		})
	}
}

func TestFailuresOnInitiator(t *testing.T) {
	tcs := map[string]struct {
		rhandler         func(s network.Stream)
		errMsg           string
		holePunchTimeout time.Duration
		filter           func(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr
	}{
		"responder does NOT send a CONNECT message": {
			rhandler: func(s network.Stream) {
				wr := pbio.NewDelimitedWriter(s)
				wr.WriteMsg(&holepunch_pb.HolePunch{Type: holepunch_pb.HolePunch_SYNC.Enum()})
			},
			errMsg: "expect CONNECT message, got SYNC",
		},
		"responder does NOT support protocol": {
			rhandler: nil,
		},
		"unable to READ CONNECT message from responder": {
			rhandler: func(s network.Stream) {
				s.Reset()
			},
			errMsg: "failed to read CONNECT message",
		},
		"responder does NOT reply within hole punch deadline": {
			holePunchTimeout: 200 * time.Millisecond,
			rhandler:         func(_ network.Stream) { time.Sleep(5 * time.Second) },
			errMsg:           "i/o deadline reached",
		},
		"no addrs after filtering": {
			errMsg:   "aborting hole punch initiation as we have no public address",
			rhandler: func(_ network.Stream) { time.Sleep(5 * time.Second) },
			filter: func(_ peer.ID, _ []ma.Multiaddr) []ma.Multiaddr {
				return []ma.Multiaddr{}
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			if tc.holePunchTimeout != 0 {
				cpy := holepunch.StreamTimeout
				holepunch.StreamTimeout = tc.holePunchTimeout
				defer func() { holepunch.StreamTimeout = cpy }()
			}

			router := &simnet.SimpleFirewallRouter{}
			relay := MustNewHost(t,
				quicSimnet(true, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
				libp2p.DisableRelay(),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
					// Setup relay service
					_, err := relayv2.New(h)
					require.NoError(t, err)
				})),
			)

			// h1 does not have a holepunching service because we'll mock the holepunching stream handler below.
			h1 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.ForceReachabilityPrivate(),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8000/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				connectToRelay(&relay),
			)

			h2 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				connectToRelay(&relay),
			)

			defer h1.Close()
			defer h2.Close()
			defer relay.Close()

			time.Sleep(100 * time.Millisecond)

			tr := &mockEventTracer{}
			opts := []holepunch.Option{holepunch.WithTracer(tr), holepunch.DirectDialTimeout(100 * time.Millisecond)}
			if tc.filter != nil {
				f := mockMaddrFilter{
					filterLocal:  tc.filter,
					filterRemote: tc.filter,
				}
				opts = append(opts, holepunch.WithAddrFilter(f))
			}

			hps := addHolePunchService(t, h2, []ma.Multiaddr{ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")}, opts...)
			// We are only holepunching from h2 to h1. Remove h2's holepunching stream handler to avoid confusion.
			h2.RemoveStreamHandler(holepunch.Protocol)
			if tc.rhandler != nil {
				h1.SetStreamHandler(holepunch.Protocol, tc.rhandler)
			}

			require.NoError(t, h2.Connect(context.Background(), peer.AddrInfo{
				ID:    h1.ID(),
				Addrs: h1.Addrs(),
			}))

			err := hps.DirectConnect(h1.ID())
			require.Error(t, err)
			if tc.errMsg != "" {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func addrsToBytes(as []ma.Multiaddr) [][]byte {
	bzs := make([][]byte, 0, len(as))
	for _, a := range as {
		bzs = append(bzs, a.Bytes())
	}
	return bzs
}

func TestFailuresOnResponder(t *testing.T) {
	tcs := map[string]struct {
		initiator        func(s network.Stream)
		errMsg           string
		holePunchTimeout time.Duration
		filter           func(remoteID peer.ID, maddrs []ma.Multiaddr) []ma.Multiaddr
	}{
		"initiator does NOT send a CONNECT message": {
			initiator: func(s network.Stream) {
				pbio.NewDelimitedWriter(s).WriteMsg(&holepunch_pb.HolePunch{Type: holepunch_pb.HolePunch_SYNC.Enum()})
			},
			errMsg: "expected CONNECT message",
		},
		"initiator does NOT send a SYNC message after a CONNECT message": {
			initiator: func(s network.Stream) {
				w := pbio.NewDelimitedWriter(s)
				w.WriteMsg(&holepunch_pb.HolePunch{
					Type:     holepunch_pb.HolePunch_CONNECT.Enum(),
					ObsAddrs: addrsToBytes([]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/1234")}),
				})
				w.WriteMsg(&holepunch_pb.HolePunch{Type: holepunch_pb.HolePunch_CONNECT.Enum()})
			},
			errMsg: "expected SYNC message",
		},
		"initiator does NOT reply within hole punch deadline": {
			holePunchTimeout: 10 * time.Millisecond,
			initiator: func(s network.Stream) {
				pbio.NewDelimitedWriter(s).WriteMsg(&holepunch_pb.HolePunch{
					Type:     holepunch_pb.HolePunch_CONNECT.Enum(),
					ObsAddrs: addrsToBytes([]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/1234")}),
				})
				time.Sleep(10 * time.Second)
			},
			errMsg: "i/o deadline reached",
		},
		"initiator does NOT send any addresses in CONNECT": {
			holePunchTimeout: 10 * time.Millisecond,
			initiator: func(s network.Stream) {
				pbio.NewDelimitedWriter(s).WriteMsg(&holepunch_pb.HolePunch{Type: holepunch_pb.HolePunch_CONNECT.Enum()})
				time.Sleep(10 * time.Second)
			},
			errMsg: "expected CONNECT message to contain at least one address",
		},
		"no addrs after filtering": {
			errMsg: "rejecting hole punch request, as we don't have any public addresses",
			initiator: func(s network.Stream) {
				pbio.NewDelimitedWriter(s).WriteMsg(&holepunch_pb.HolePunch{
					Type:     holepunch_pb.HolePunch_CONNECT.Enum(),
					ObsAddrs: addrsToBytes([]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/1234")}),
				})
				time.Sleep(10 * time.Second)
			},
			filter: func(_ peer.ID, _ []ma.Multiaddr) []ma.Multiaddr {
				return []ma.Multiaddr{}
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			if tc.holePunchTimeout != 0 {
				cpy := holepunch.StreamTimeout
				holepunch.StreamTimeout = tc.holePunchTimeout
				defer func() { holepunch.StreamTimeout = cpy }()
			}
			tr := &mockEventTracer{}

			opts := []holepunch.Option{holepunch.WithTracer(tr), holepunch.DirectDialTimeout(100 * time.Millisecond)}
			if tc.filter != nil {
				f := mockMaddrFilter{
					filterLocal:  tc.filter,
					filterRemote: tc.filter,
				}
				opts = append(opts, holepunch.WithAddrFilter(f))
			}

			router := &simnet.SimpleFirewallRouter{}
			relay := MustNewHost(t,
				quicSimnet(true, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
				libp2p.DisableRelay(),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
					// Setup relay service
					_, err := relayv2.New(h)
					require.NoError(t, err)
				})),
			)
			h1 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.EnableHolePunching(opts...),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8000/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				connectToRelay(&relay),
				libp2p.ForceReachabilityPrivate(),
			)

			h2 := MustNewHost(t,
				quicSimnet(false, router),
				libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1")),
				libp2p.ResourceManager(&network.NullResourceManager{}),
				connectToRelay(&relay),
				libp2p.ForceReachabilityPrivate(),
			)

			defer h1.Close()
			defer h2.Close()
			defer relay.Close()

			// The holepunch.Service installs its stream handler asynchronously
			// from waitForPublicAddr (see p2p/protocol/holepunch/svc.go),
			// gated on the local node observing at least one public address.
			// Block until that handler is registered on h1 before connecting,
			// otherwise the test can race the service startup. Issue #3440.
			require.Eventually(t, func() bool {
				return slices.Contains(h1.Mux().Protocols(), holepunch.Protocol)
			}, 5*time.Second, 10*time.Millisecond, "holepunch protocol never registered on h1")

			require.NoError(t, h1.Connect(context.Background(), peer.AddrInfo{
				ID:    h2.ID(),
				Addrs: h2.Addrs(),
			}))

			// After Connect, h2 learns about h1's protocols via the identify
			// exchange, which happens asynchronously. h2.NewStream below
			// relies on h2 already knowing h1 supports the holepunch
			// protocol, so wait for the identify-driven peerstore update.
			require.Eventually(t, func() bool {
				protos, err := h2.Peerstore().GetProtocols(h1.ID())
				if err != nil {
					return false
				}
				return slices.Contains(protos, holepunch.Protocol)
			}, 5*time.Second, 10*time.Millisecond, "h2 never observed holepunch protocol on h1 after identify")

			s, err := h2.NewStream(network.WithAllowLimitedConn(context.Background(), "holepunch"), h1.ID(), holepunch.Protocol)
			require.NoError(t, err)

			go tc.initiator(s)

			getTracerError := func(tr *mockEventTracer) []string {
				var errs []string
				events := tr.getEvents()
				for _, ev := range events {
					if errEv, ok := ev.Evt.(*holepunch.ProtocolErrorEvt); ok {
						errs = append(errs, errEv.Error)
					}
				}
				return errs
			}

			require.Eventually(t, func() bool { return len(getTracerError(tr)) > 0 }, 5*time.Second, 100*time.Millisecond)
			errs := getTracerError(tr)
			require.Len(t, errs, 1)
			require.Contains(t, errs[0], tc.errMsg)
		})
	}
}

func ensureNoHolePunchingStream(t *testing.T, h1, h2 host.Host) {
	require.Eventually(t, func() bool {
		for _, c := range h1.Network().ConnsToPeer(h2.ID()) {
			for _, s := range c.GetStreams() {
				if s.ID() == string(holepunch.Protocol) {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		for _, c := range h2.Network().ConnsToPeer(h1.ID()) {
			for _, s := range c.GetStreams() {
				if s.ID() == string(holepunch.Protocol) {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)
}

func ensureDirectConn(t *testing.T, h1, h2 host.Host) {
	require.Eventually(t, func() bool {
		for _, c := range h1.Network().ConnsToPeer(h2.ID()) {
			if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		for _, c := range h2.Network().ConnsToPeer(h1.ID()) {
			if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
}

type MockSourceIPSelector struct {
	ip atomic.Pointer[net.IP]
}

func (m *MockSourceIPSelector) PreferredSourceIPForDestination(_ *net.UDPAddr) (net.IP, error) {
	return *m.ip.Load(), nil
}

func quicSimnet(isPubliclyReachably bool, router *simnet.SimpleFirewallRouter) libp2p.Option {
	m := &MockSourceIPSelector{}
	return libp2p.QUICReuse(
		quicreuse.NewConnManager,
		quicreuse.OverrideSourceIPSelector(func() (quicreuse.SourceIPSelector, error) {
			return m, nil
		}),
		quicreuse.OverrideListenUDP(func(_ string, address *net.UDPAddr) (net.PacketConn, error) {
			m.ip.Store(&address.IP)
			if isPubliclyReachably {
				router.SetAddrPubliclyReachable(address)
			}
			c := simnet.NewSimConn(address)
			// AddNode only wires the router -> conn direction. The conn ->
			// router direction needs the router set as the up receiver, else
			// the first write panics.
			c.SetUpPacketReceiver(router)
			router.AddNode(address, c)
			return c, nil
		}))
}

func addHolePunchService(t *testing.T, h host.Host, extraAddrs []ma.Multiaddr, opts ...holepunch.Option) *holepunch.Service {
	t.Helper()
	hps, err := holepunch.NewService(h, newIDService(t, h), func() []ma.Multiaddr {
		addrs := h.Addrs()
		addrs = append(addrs, extraAddrs...)
		return addrs
	}, opts...)
	require.NoError(t, err)
	return hps
}

func waitForHolePunchingSvcActive(t *testing.T, h host.Host) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, h.Mux().Protocols(), holepunch.Protocol)
	}, time.Second, 100*time.Millisecond)
}

// TestEndToEndSimConnectQUICReuse tests that hole punching works if we are
// reusing the same port for QUIC and WebTransport, and when we have multiple
// QUIC listeners on different ports.
//
// If this tests fails or is flaky it may be because:
// - The quicreuse logic (and association logic) is not returning the appropriate transport for holepunching.
// - The ordering of listeners is unexpected (remember the swarm will sort the listeners with `.ListenOrder()`).
func TestEndToEndSimConnectQUICReuse(t *testing.T) {
	h1tr := &mockEventTracer{}
	h2tr := &mockEventTracer{}

	router := &simnet.SimpleFirewallRouter{}
	relay := MustNewHost(t,
		quicSimnet(true, router),
		libp2p.ListenAddrs(ma.StringCast("/ip4/1.2.0.1/udp/8000/quic-v1")),
		libp2p.DisableRelay(),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.WithFxOption(fx.Invoke(func(h host.Host) {
			// Setup relay service
			_, err := relayv2.New(h)
			require.NoError(t, err)
		})),
	)

	// We return addrs of quic on port 8001 and circuit.
	// This lets us listen on other ports for QUIC in order to confuse the quicreuse logic during hole punching.
	onlyQuicOnPort8001AndCircuit := func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return slices.DeleteFunc(addrs, func(a ma.Multiaddr) bool {
			_, err := a.ValueForProtocol(ma.P_CIRCUIT)
			isCircuit := err == nil
			if isCircuit {
				return false
			}
			_, err = a.ValueForProtocol(ma.P_QUIC_V1)
			isQuic := err == nil
			if !isQuic {
				return true
			}
			port, err := a.ValueForProtocol(ma.P_UDP)
			if err != nil {
				return true
			}
			isPort8001 := port == "8001"
			return !isPort8001
		})
	}

	h1 := MustNewHost(t,
		quicSimnet(false, router),
		libp2p.EnableHolePunching(holepunch.WithTracer(h1tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
		libp2p.ListenAddrs(ma.StringCast("/ip4/2.2.0.1/udp/8001/quic-v1/webtransport")),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.AddrsFactory(onlyQuicOnPort8001AndCircuit),
		libp2p.ForceReachabilityPrivate(),
	)
	// Listen on quic *after* listening on webtransport.
	// This is to test that the quicreuse logic is not returning the wrong transport.
	// See: https://github.com/libp2p/go-libp2p/issues/3165#issuecomment-2700126706 for details.
	h1.Network().Listen(
		ma.StringCast("/ip4/2.2.0.1/udp/8001/quic-v1"),
		ma.StringCast("/ip4/2.2.0.1/udp/9001/quic-v1"),
	)

	h2 := MustNewHost(t,
		quicSimnet(false, router),
		libp2p.ListenAddrs(
			ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1/webtransport"),
		),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		connectToRelay(&relay),
		libp2p.EnableHolePunching(holepunch.WithTracer(h2tr), holepunch.DirectDialTimeout(100*time.Millisecond)),
		libp2p.AddrsFactory(onlyQuicOnPort8001AndCircuit),
		libp2p.ForceReachabilityPrivate(),
	)
	// Listen on quic after listening on webtransport.
	h2.Network().Listen(
		ma.StringCast("/ip4/2.2.0.2/udp/8001/quic-v1"),
		ma.StringCast("/ip4/2.2.0.2/udp/9001/quic-v1"),
	)

	defer h1.Close()
	defer h2.Close()
	defer relay.Close()

	// Wait for holepunch service to start
	waitForHolePunchingSvcActive(t, h1)
	waitForHolePunchingSvcActive(t, h2)

	learnAddrs(h1, h2)
	pingAtoB(t, h1, h2)

	// wait till a direct connection is complete
	ensureDirectConn(t, h1, h2)
}
