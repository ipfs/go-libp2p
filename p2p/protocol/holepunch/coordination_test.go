package holepunch_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
)

func TestDirectDialWorks(t *testing.T) {
	// all addrs should be marked as public
	cpy := manet.Private4
	manet.Private4 = []*net.IPNet{}
	defer func() {
		manet.Private4 = cpy
	}()

	ctx := context.Background()

	// try to hole punch without any connection and streams, if it works -> it's a direct connection
	h1, h1ps := mkHostWithHolePunchSvc(t, ctx)
	h2, _ := mkHostWithHolePunchSvc(t, ctx)
	h2.RemoveStreamHandler(holepunch.Protocol)
	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.ConnectedAddrTTL)

	require.NoError(t, h1ps.HolePunch(h2.ID()))

	cs := h1.Network().ConnsToPeer(h2.ID())
	require.Len(t, cs, 1)

	cs = h2.Network().ConnsToPeer(h1.ID())
	require.Len(t, cs, 1)
}

func TestEndToEndSimConnect(t *testing.T) {
	// all addrs should be marked as public
	cpy := manet.Private4
	manet.Private4 = []*net.IPNet{}
	defer func() {
		manet.Private4 = cpy
	}()
	ctx := context.Background()
	r := mkRelay(t, ctx)

	h1, _ := mkHostWithHolePunchSvc(t, ctx)
	h2, _ := mkHostWithStaticAutoRelay(t, ctx, r)

	// h1 has a relay addr
	// h2 should connect to the relay addr
	var raddr ma.Multiaddr
	for _, a := range h2.Addrs() {
		if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err == nil {
			raddr = a
			break
		}
	}
	require.NotEmpty(t, raddr)

	require.NoError(t, h1.Connect(ctx, peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: []ma.Multiaddr{raddr},
	}))

	// wait till a direct connection is complete
	ensureDirectConn(t, h1, h2)
	// ensure no hole-punching streams are open on either side
	ensureNoHolePunchingStream(t, h1, h2)
}

func TestFailuresOnInitiator(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		rhandler         func(s network.Stream)
		errMsg           string
		holePunchTimeout time.Duration
	}{
		"responder does NOT send a CONNECT message": {
			rhandler: func(s network.Stream) {
				wr := protoio.NewDelimitedWriter(s)
				msg := new(holepunch_pb.HolePunch)
				msg.Type = holepunch_pb.HolePunch_SYNC.Enum()
				wr.WriteMsg(msg)
			},
			errMsg: "expected HolePunch_CONNECT message",
		},
		"responder does NOT support protocol": {
			rhandler: nil,
			errMsg:   "protocol not supported",
		},
		"unable to READ CONNECT message from responder": {
			rhandler: func(s network.Stream) {
				s.Reset()
			},
			errMsg: "failed to read HolePunch_CONNECT message",
		},
		"responder does NOT reply within hole punch deadline": {
			holePunchTimeout: 10 * time.Millisecond,
			rhandler: func(s network.Stream) {
				for {

				}
			},
			errMsg: "i/o deadline reached",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			if tc.holePunchTimeout != 0 {
				cpy := holepunch.HolePunchTimeout
				holepunch.HolePunchTimeout = tc.holePunchTimeout
				defer func() {
					holepunch.HolePunchTimeout = cpy
				}()
			}

			h1, h1ps := mkHostWithHolePunchSvc(t, ctx)
			h2, _ := mkHostWithHolePunchSvc(t, ctx)

			if tc.rhandler != nil {
				h2.SetStreamHandler(holepunch.Protocol, tc.rhandler)
			} else {
				h2.RemoveStreamHandler(holepunch.Protocol)
			}

			connect(t, ctx, h1, h2)
			err := h1ps.HolePunch(h2.ID())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})

	}
}

func TestFailuresOnResponder(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		initiator        func(s network.Stream)
		errMsg           string
		holePunchTimeout time.Duration
	}{
		"initiator does NOT send a CONNECT message": {
			initiator: func(s network.Stream) {
				w := protoio.NewDelimitedWriter(s)
				msg := new(holepunch_pb.HolePunch)
				msg.Type = holepunch_pb.HolePunch_SYNC.Enum()
				w.WriteMsg(msg)

			},
			errMsg: "expected HolePunch_CONNECT message",
		},

		"initiator does NOT send a SYNC message after a Connect message": {
			initiator: func(s network.Stream) {
				w := protoio.NewDelimitedWriter(s)
				msg := new(holepunch_pb.HolePunch)
				msg.Type = holepunch_pb.HolePunch_CONNECT.Enum()
				w.WriteMsg(msg)

				msg = new(holepunch_pb.HolePunch)
				msg.Type = holepunch_pb.HolePunch_CONNECT.Enum()
				w.WriteMsg(msg)
			},
			errMsg: "expected HolePunch_SYNC message",
		},

		"initiator does NOT reply within hole punch deadline": {
			holePunchTimeout: 10 * time.Millisecond,
			initiator: func(s network.Stream) {
				w := protoio.NewDelimitedWriter(s)
				msg := new(holepunch_pb.HolePunch)
				msg.Type = holepunch_pb.HolePunch_CONNECT.Enum()
				w.WriteMsg(msg)
				for {

				}

			},
			errMsg: "i/o deadline reached",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			if tc.holePunchTimeout != 0 {
				cpy := holepunch.HolePunchTimeout
				holepunch.HolePunchTimeout = tc.holePunchTimeout
				defer func() {
					holepunch.HolePunchTimeout = cpy
				}()
			}

			h1, _ := mkHostWithHolePunchSvc(t, ctx)
			h2, h2ps := mkHostWithHolePunchSvc(t, ctx)
			connect(t, ctx, h1, h2)

			s, err := h1.NewStream(ctx, h2.ID(), holepunch.Protocol)
			require.NoError(t, err)

			go tc.initiator(s)

			require.Eventually(t, func() bool {
				return len(h2ps.HandlerErrors()) != 0
			}, 5*time.Second, 100*time.Millisecond)

			errs := h2ps.HandlerErrors()
			require.Len(t, errs, 1)
			err = errs[0]
			require.Contains(t, err.Error(), tc.errMsg)
		})

	}
}

func TestHolePunchingFailsOnSymmetricNAT(t *testing.T) {
	ctx := context.Background()

	h1, h1ps := mkHostWithHolePunchSvc(t, ctx)
	h2, _ := mkHostWithHolePunchSvc(t, ctx)
	// connect and wait for identify to exchange NAT types
	connect(t, ctx, h1, h2)

	// self peer is behind a Symmetric NAT and remote peer is behind a Cone NAT
	h1.Peerstore().Put(h1.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeSymmetric)
	h1.Peerstore().Put(h2.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeCone)
	require.Equal(t, holepunch.ErrNATHolePunchingUnsupported, h1ps.HolePunch(h2.ID()))

	// remote peer is behind a Symmetric NAT and self is behind a Cone NAT
	h1.Peerstore().Put(h1.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeCone)
	h1.Peerstore().Put(h2.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeSymmetric)
	require.Equal(t, holepunch.ErrNATHolePunchingUnsupported, h1ps.HolePunch(h2.ID()))
}

func TestProtocolHandlerOnNATType(t *testing.T) {
	ctx := context.Background()

	// Unknown NAT -> Protocol is supported
	h, _ := mkHostWithHolePunchSvc(t, ctx)
	em, err := h.EventBus().Emitter(new(event.EvtNATDeviceTypeChanged))
	require.NoError(t, err)
	require.Contains(t, h.Mux().Protocols(), string(holepunch.Protocol))

	// Symmetric NAT -> Protocol is NOT supported
	h.Peerstore().Put(h.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeSymmetric)

	require.NoError(t, em.Emit(event.EvtNATDeviceTypeChanged{
		TransportProtocol: network.NATTransportTCP,
		NatDeviceType:     network.NATDeviceTypeSymmetric,
	}))
	require.Eventually(t, func() bool {
		for _, p := range h.Mux().Protocols() {
			if p == string(holepunch.Protocol) {
				return false
			}
		}
		return true
	}, 5*time.Second, 200*time.Millisecond)

	// Cone NAT -> Protocol is supported
	h.Peerstore().Put(h.ID(), identify.TCPNATDeviceTypeKey, network.NATDeviceTypeCone)

	require.NoError(t, em.Emit(event.EvtNATDeviceTypeChanged{
		TransportProtocol: network.NATTransportTCP,
		NatDeviceType:     network.NATDeviceTypeCone,
	}))
	require.Eventually(t, func() bool {
		for _, p := range h.Mux().Protocols() {
			if p == string(holepunch.Protocol) {
				return true
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)
}

func TestPeerSupportsHolePunching(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		udpSupported         bool
		tcpSupported         bool
		udpNat               network.NATDeviceType
		tcpNat               network.NATDeviceType
		supportsHolePunching bool
	}{
		"udp/tcp supported and symmetric for both -> no hole punching": {
			udpSupported:         true,
			tcpSupported:         true,
			udpNat:               network.NATDeviceTypeSymmetric,
			tcpNat:               network.NATDeviceTypeSymmetric,
			supportsHolePunching: false,
		},
		"tcp supported and symmetric -> no hole punching": {
			tcpSupported:         true,
			tcpNat:               network.NATDeviceTypeSymmetric,
			udpNat:               network.NATDeviceTypeCone,
			supportsHolePunching: false,
		},
		"udp supported and symmetric -> no hole punching": {
			udpSupported:         true,
			udpNat:               network.NATDeviceTypeSymmetric,
			tcpNat:               network.NATDeviceTypeCone,
			supportsHolePunching: false,
		},
		"udp/tcp supported and cone for both -> hole punching possible": {
			udpSupported:         true,
			tcpSupported:         true,
			udpNat:               network.NATDeviceTypeCone,
			tcpNat:               network.NATDeviceTypeCone,
			supportsHolePunching: true,
		},
		"tcp supported and cone -> hole punching possible": {
			tcpSupported:         true,
			tcpNat:               network.NATDeviceTypeCone,
			udpNat:               network.NATDeviceTypeSymmetric,
			supportsHolePunching: true,
		},
		"udp supported and cone -> hole punching possible": {
			udpSupported:         true,
			udpNat:               network.NATDeviceTypeCone,
			tcpNat:               network.NATDeviceTypeSymmetric,
			supportsHolePunching: true,
		},
		"udp/tcp supported and unknown for both -> hole punching possible": {
			udpSupported:         true,
			tcpSupported:         true,
			udpNat:               network.NATDeviceTypeUnknown,
			tcpNat:               network.NATDeviceTypeUnknown,
			supportsHolePunching: true,
		},
		"tcp supported and unknown -> hole punching possible": {
			tcpSupported:         true,
			tcpNat:               network.NATDeviceTypeUnknown,
			udpNat:               network.NATDeviceTypeSymmetric,
			supportsHolePunching: true,
		},
		"udp supported and unknown -> hole punching possible": {
			udpSupported:         true,
			udpNat:               network.NATDeviceTypeUnknown,
			tcpNat:               network.NATDeviceTypeSymmetric,
			supportsHolePunching: true,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			h, hps := mkHostWithHolePunchSvc(t, ctx)
			var addrs []ma.Multiaddr
			if tc.udpSupported {
				addrs = append(addrs, ma.StringCast("/ip4/8.8.8.8/udp/1234/quic"))
			}

			if tc.tcpSupported {
				addrs = append(addrs, ma.StringCast("/ip4/8.8.8.8/tcp/1234"))
			}

			h.Peerstore().Put(h.ID(), identify.TCPNATDeviceTypeKey, tc.tcpNat)
			h.Peerstore().Put(h.ID(), identify.UDPNATDeviceTypeKey, tc.udpNat)

			require.Equal(t, tc.supportsHolePunching, hps.PeerSupportsHolePunching(h.ID(), addrs))
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

	}, 5*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		for _, c := range h2.Network().ConnsToPeer(h1.ID()) {
			for _, s := range c.GetStreams() {
				if s.ID() == string(holepunch.Protocol) {
					return false
				}
			}
		}

		return true

	}, 5*time.Second, 200*time.Millisecond)
}

func ensureDirectConn(t *testing.T, h1, h2 host.Host) {
	require.Eventually(t, func() bool {
		cs := h1.Network().ConnsToPeer(h2.ID())
		if len(cs) != 2 {
			return false
		}
		for _, c := range cs {
			if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		cs := h2.Network().ConnsToPeer(h1.ID())
		if len(cs) != 2 {
			return false
		}
		for _, c := range cs {
			if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)
}

func connect(t *testing.T, ctx context.Context, h1, h2 host.Host) network.Conn {
	require.NoError(t, h1.Connect(ctx, peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}))

	cs := h1.Network().ConnsToPeer(h2.ID())
	require.Len(t, cs, 1)
	return cs[0]
}

func mkHostWithStaticAutoRelay(t *testing.T, ctx context.Context, relay host.Host) (host.Host, *holepunch.HolePunchService) {
	pi := peer.AddrInfo{
		ID:    relay.ID(),
		Addrs: relay.Addrs(),
	}

	h, err := libp2p.New(ctx, libp2p.EnableRelay(), libp2p.EnableAutoRelay(), libp2p.ForceReachabilityPrivate(),
		libp2p.StaticRelays([]peer.AddrInfo{pi}))
	require.NoError(t, err)
	ids, err := identify.NewIDService(h)
	require.NoError(t, err)
	hps, err := holepunch.NewHolePunchService(h, ids, true)
	require.NoError(t, err)

	// wait till we have a relay addr
	require.Eventually(t, func() bool {
		for _, a := range h.Addrs() {
			if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err == nil {
				return true
			}
		}

		return false
	}, 5*time.Second, 200*time.Millisecond)

	return h, hps
}

func mkRelay(t *testing.T, ctx context.Context) host.Host {
	h, err := libp2p.New(ctx, libp2p.EnableRelay(circuit.OptHop), libp2p.ForceReachabilityPrivate())
	require.NoError(t, err)
	return h
}

func mkHostWithHolePunchSvc(t *testing.T, ctx context.Context) (host.Host, *holepunch.HolePunchService) {
	h, err := libp2p.New(ctx, libp2p.ForceReachabilityPrivate())
	require.NoError(t, err)
	ids, err := identify.NewIDService(h)
	require.NoError(t, err)
	hps, err := holepunch.NewHolePunchService(h, ids, true)
	require.NoError(t, err)

	return h, hps
}
