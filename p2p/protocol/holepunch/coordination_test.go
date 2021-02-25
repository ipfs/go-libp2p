package holepunch_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	identify_pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
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
	t.Skip("broken test")

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
			errMsg: "expected CONNECT message",
		},
		"responder does NOT support protocol": {
			rhandler: nil,
			errMsg:   "protocol not supported",
		},
		"unable to READ CONNECT message from responder": {
			rhandler: func(s network.Stream) {
				s.Reset()
			},
			errMsg: "failed to read CONNECT message",
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
			errMsg: "expected CONNECT message",
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
			errMsg: "expected SYNC message",
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

func TestObservedAddressesAreExchanged(t *testing.T) {
	ctx := context.Background()

	obsAddrs1 := ma.StringCast("/ip4/8.8.8.8/tcp/1234")
	obsAddrs2 := ma.StringCast("/ip4/9.8.8.8/tcp/1234")

	h1, h1ps := mkHostWithHolePunchSvc(t, ctx)
	h2, _ := mkHostWithHolePunchSvc(t, ctx)

	// modify identify handlers to send our fake observed addresses
	h1.SetStreamHandler(identify.ID, func(s network.Stream) {
		writer := protoio.NewDelimitedWriter(s)
		msg := new(identify_pb.Identify)
		msg.ObservedAddr = obsAddrs2.Bytes()
		writer.WriteMsg(msg)
		s.Close()
	})

	h2.SetStreamHandler(identify.ID, func(s network.Stream) {
		writer := protoio.NewDelimitedWriter(s)
		msg := new(identify_pb.Identify)
		msg.ObservedAddr = obsAddrs1.Bytes()
		writer.WriteMsg(msg)
		s.Close()
	})

	connect(t, ctx, h1, h2)

	// hole punch so both peers exchange each other's observed addresses and save to peerstore
	require.NoError(t, h1ps.HolePunch(h2.ID()))

	require.Eventually(t, func() bool {
		h2Addrs := h1.Peerstore().Addrs(h2.ID())
		h1Addrs := h2.Peerstore().Addrs(h1.ID())

		b1 := false
		b2 := false
		for _, a := range h1Addrs {
			if a.Equal(obsAddrs1) {
				b1 = true
				break
			}
		}

		for _, a := range h2Addrs {
			if a.Equal(obsAddrs2) {
				b2 = true
				break
			}
		}

		return b1 && b2
	}, 2*time.Second, 100*time.Millisecond)
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

func TestNoHolePunchingIfDirectConnAlreadyExists(t *testing.T) {

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
	hps, err := holepunch.NewHolePunchService(h, ids, withTest)
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
	h, err := libp2p.New(ctx, libp2p.EnableRelay(circuit.OptHop))
	require.NoError(t, err)
	return h
}

func mkHostWithHolePunchSvc(t *testing.T, ctx context.Context) (host.Host, *holepunch.HolePunchService) {
	h, err := libp2p.New(ctx)
	require.NoError(t, err)
	ids, err := identify.NewIDService(h)
	require.NoError(t, err)
	hps, err := holepunch.NewHolePunchService(h, ids, withTest)
	require.NoError(t, err)

	return h, hps
}

func withTest(hps *HolePunchService) error {
	hps.isTest = true
}
