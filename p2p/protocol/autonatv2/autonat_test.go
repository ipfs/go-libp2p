package autonatv2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	bhost "github.com/libp2p/go-libp2p/p2p/host/blank"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/libp2p/go-libp2p/p2p/protocol/autonatv2/pb"

	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAutoNAT(t testing.TB, dialer host.Host, opts ...AutoNATOption) *AutoNAT {
	t.Helper()
	b := eventbus.NewBus()
	h := bhost.NewBlankHost(
		swarmt.GenSwarm(t, swarmt.EventBus(b), swarmt.OptDisableWebTransport, swarmt.OptDisableWebRTC), bhost.WithEventBus(b))
	if dialer == nil {
		dialer = bhost.NewBlankHost(
			swarmt.GenSwarm(t,
				swarmt.WithSwarmOpts(
					swarm.WithUDPBlackHoleSuccessCounter(nil),
					swarm.WithIPv6BlackHoleSuccessCounter(nil))))
	}
	opts = append([]AutoNATOption{withThrottlePeerDuration(0)}, opts...)
	an, err := New(dialer, opts...)
	if err != nil {
		t.Error(err)
	}
	require.NoError(t, an.Start(h))
	t.Cleanup(an.Close)
	return an
}

func parseAddrs(t *testing.T, msg *pb.Message) []ma.Multiaddr {
	t.Helper()
	req := msg.GetDialRequest()
	addrs := make([]ma.Multiaddr, 0, len(req.Addrs))
	for _, ab := range req.Addrs {
		a, err := ma.NewMultiaddrBytes(ab)
		if err != nil {
			t.Error("invalid addr bytes", ab)
		}
		addrs = append(addrs, a)
	}
	return addrs
}

// idAndConnect identifies b to a and connects them
func idAndConnect(t testing.TB, a, b host.Host) {
	a.Peerstore().AddAddrs(b.ID(), b.Addrs(), peerstore.PermanentAddrTTL)
	a.Peerstore().AddProtocols(b.ID(), DialProtocol)

	err := a.Connect(context.Background(), peer.AddrInfo{ID: b.ID()})
	require.NoError(t, err)
}

// waitForPeer waits for a to have 1 peer in the peerMap
func waitForPeer(t testing.TB, a *AutoNAT) {
	t.Helper()
	require.Eventually(t, func() bool {
		a.mx.Lock()
		defer a.mx.Unlock()
		return len(a.peers.peers) != 0
	}, 5*time.Second, 100*time.Millisecond)
}

// idAndWait provides server address and protocol to client
func idAndWait(t testing.TB, cli *AutoNAT, srv *AutoNAT) {
	idAndConnect(t, cli.host, srv.host)
	waitForPeer(t, cli)
}

func TestAutoNATPrivateAddr(t *testing.T) {
	an := newAutoNAT(t, nil)
	res, err := an.GetReachability(context.Background(), []Request{{Addr: ma.StringCast("/ip4/192.168.0.1/udp/10/quic-v1")}})
	require.Equal(t, res, Result{})
	require.ErrorIs(t, err, ErrPrivateAddrs)
}

func TestGetReachabilityAfterClose(t *testing.T) {
	// The host closes autonat before the address manager, whose reachability
	// tracker workers may still issue checks during shutdown.
	an := newAutoNAT(t, nil)
	an.Close()
	res, err := an.GetReachability(context.Background(), []Request{{Addr: ma.StringCast("/ip4/1.2.3.4/udp/10/quic-v1")}})
	require.ErrorIs(t, err, ErrNoPeers)
	require.Equal(t, Result{}, res)
}

func TestForcedReachability(t *testing.T) {
	addr := ma.StringCast("/ip4/192.168.0.1/udp/10/quic-v1")
	for _, reachability := range []network.Reachability{
		network.ReachabilityPrivate,
		network.ReachabilityPublic,
	} {
		t.Run(reachability.String(), func(t *testing.T) {
			an := newAutoNAT(t, nil, WithReachability(reachability))
			defer an.Close()
			defer an.host.Close()

			res, err := an.GetReachability(context.Background(), []Request{{Addr: addr}})
			require.NoError(t, err)
			require.Equal(t, Result{Addr: addr, Reachability: reachability}, res)
		})
	}
}

func TestClientRequest(t *testing.T) {
	an := newAutoNAT(t, nil, AllowPrivateAddrs)
	defer an.Close()
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	idAndConnect(t, an.host, b)
	waitForPeer(t, an)

	addrs := an.host.Addrs()
	addrbs := make([][]byte, len(addrs))
	for i := range addrs {
		addrbs[i] = addrs[i].Bytes()
	}

	var receivedRequest atomic.Bool
	b.SetStreamHandler(DialProtocol, func(s network.Stream) {
		receivedRequest.Store(true)
		r := pbio.NewDelimitedReader(s, maxMsgSize)
		var msg pb.Message
		assert.NoError(t, r.ReadMsg(&msg))
		assert.NotNil(t, msg.GetDialRequest())
		assert.Equal(t, addrbs, msg.GetDialRequest().Addrs)
		s.Reset()
	})

	res, err := an.GetReachability(context.Background(), []Request{
		{Addr: addrs[0], SendDialData: true}, {Addr: addrs[1]},
	})
	require.Equal(t, res, Result{})
	require.NotNil(t, err)
	require.True(t, receivedRequest.Load())
}

func TestClientServerError(t *testing.T) {
	an := newAutoNAT(t, nil, AllowPrivateAddrs)
	defer an.Close()
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	idAndConnect(t, an.host, b)
	waitForPeer(t, an)

	tests := []struct {
		handler  func(network.Stream)
		errorStr string
	}{
		{
			handler: func(s network.Stream) {
				s.Reset()
			},
			errorStr: "stream reset",
		},
		{
			handler: func(s network.Stream) {
				w := pbio.NewDelimitedWriter(s)
				assert.NoError(t, w.WriteMsg(
					&pb.Message{Msg: &pb.Message_DialRequest{DialRequest: &pb.DialRequest{}}}))
			},
			errorStr: "invalid msg type",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			b.SetStreamHandler(DialProtocol, tc.handler)
			addrs := an.host.Addrs()
			res, err := an.GetReachability(
				context.Background(),
				newTestRequests(addrs, false))
			require.Equal(t, res, Result{})
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errorStr)
		})
	}
}

func TestClientDataRequest(t *testing.T) {
	an := newAutoNAT(t, nil, AllowPrivateAddrs)
	defer an.Close()
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	idAndConnect(t, an.host, b)
	waitForPeer(t, an)

	tests := []struct {
		handler func(network.Stream)
		name    string
	}{
		{
			name: "provides dial data",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				var msg pb.Message
				assert.NoError(t, r.ReadMsg(&msg))
				w := pbio.NewDelimitedWriter(s)
				if err := w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialDataRequest{
						DialDataRequest: &pb.DialDataRequest{
							AddrIdx:  0,
							NumBytes: 10000,
						},
					}},
				); err != nil {
					t.Error(err)
					s.Reset()
					return
				}
				var dialData []byte
				for len(dialData) < 10000 {
					if err := r.ReadMsg(&msg); err != nil {
						t.Error(err)
						s.Reset()
						return
					}
					if msg.GetDialDataResponse() == nil {
						t.Errorf("expected to receive msg of type DialDataResponse")
						s.Reset()
						return
					}
					dialData = append(dialData, msg.GetDialDataResponse().Data...)
				}
				s.Reset()
			},
		},
		{
			name: "low priority addr",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				var msg pb.Message
				assert.NoError(t, r.ReadMsg(&msg))
				w := pbio.NewDelimitedWriter(s)
				if err := w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialDataRequest{
						DialDataRequest: &pb.DialDataRequest{
							AddrIdx:  1,
							NumBytes: 10000,
						},
					}},
				); err != nil {
					t.Error(err)
					s.Reset()
					return
				}
				assert.Error(t, r.ReadMsg(&msg))
				s.Reset()
			},
		},
		{
			name: "too high dial data request",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				var msg pb.Message
				assert.NoError(t, r.ReadMsg(&msg))
				w := pbio.NewDelimitedWriter(s)
				if err := w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialDataRequest{
						DialDataRequest: &pb.DialDataRequest{
							AddrIdx:  0,
							NumBytes: 1 << 32,
						},
					}},
				); err != nil {
					t.Error(err)
					s.Reset()
					return
				}
				assert.Error(t, r.ReadMsg(&msg))
				s.Reset()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b.SetStreamHandler(DialProtocol, tc.handler)
			addrs := an.host.Addrs()

			res, err := an.GetReachability(
				context.Background(),
				[]Request{
					{Addr: addrs[0], SendDialData: true},
					{Addr: addrs[1]},
				})
			require.Equal(t, res, Result{})
			require.NotNil(t, err)
		})
	}
}

func TestAutoNATPrivateAndPublicAddrs(t *testing.T) {
	an := newAutoNAT(t, nil)
	defer an.Close()
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	idAndConnect(t, an.host, b)
	waitForPeer(t, an)

	dialerHost := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer dialerHost.Close()
	handler := func(s network.Stream) {
		w := pbio.NewDelimitedWriter(s)
		r := pbio.NewDelimitedReader(s, maxMsgSize)
		var msg pb.Message
		assert.NoError(t, r.ReadMsg(&msg))
		w.WriteMsg(&pb.Message{
			Msg: &pb.Message_DialResponse{
				DialResponse: &pb.DialResponse{
					Status:     pb.DialResponse_OK,
					DialStatus: pb.DialStatus_E_DIAL_ERROR,
					AddrIdx:    0,
				},
			},
		})
		s.Close()
	}

	b.SetStreamHandler(DialProtocol, handler)
	privateAddr := ma.StringCast("/ip4/192.168.0.1/udp/10/quic-v1")
	publicAddr := ma.StringCast("/ip4/1.2.3.4/udp/10/quic-v1")
	res, err := an.GetReachability(context.Background(),
		[]Request{
			{Addr: privateAddr},
			{Addr: publicAddr},
		})
	require.NoError(t, err)
	require.Equal(t, res.Addr, publicAddr, "%s\n%s", res.Addr, publicAddr)
	require.Equal(t, res.Idx, 1)
	require.Equal(t, res.Reachability, network.ReachabilityPrivate)
}

func TestClientDialBacks(t *testing.T) {
	an := newAutoNAT(t, nil, AllowPrivateAddrs)
	defer an.Close()
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	idAndConnect(t, an.host, b)
	waitForPeer(t, an)

	dialerHost := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer dialerHost.Close()

	readReq := func(r pbio.Reader) ([]ma.Multiaddr, uint64, error) {
		var msg pb.Message
		if err := r.ReadMsg(&msg); err != nil {
			return nil, 0, err
		}
		if msg.GetDialRequest() == nil {
			return nil, 0, errors.New("no dial request in msg")
		}
		addrs := parseAddrs(t, &msg)
		return addrs, msg.GetDialRequest().GetNonce(), nil
	}

	writeNonce := func(addr ma.Multiaddr, nonce uint64) error {
		pid := an.host.ID()
		dialerHost.Peerstore().AddAddr(pid, addr, peerstore.PermanentAddrTTL)
		defer func() {
			dialerHost.Network().ClosePeer(pid)
			dialerHost.Peerstore().RemovePeer(pid)
			dialerHost.Peerstore().ClearAddrs(pid)
		}()
		as, err := dialerHost.NewStream(context.Background(), pid, DialBackProtocol)
		if err != nil {
			return err
		}
		w := pbio.NewDelimitedWriter(as)
		if err := w.WriteMsg(&pb.DialBack{Nonce: nonce}); err != nil {
			return err
		}
		as.CloseWrite()
		data := make([]byte, 1)
		as.Read(data)
		as.Close()
		return nil
	}

	tests := []struct {
		name    string
		handler func(network.Stream)
		success bool
	}{
		{
			name: "correct dial attempt",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				w := pbio.NewDelimitedWriter(s)

				addrs, nonce, err := readReq(r)
				if err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				if err := writeNonce(addrs[1], nonce); err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialResponse{
						DialResponse: &pb.DialResponse{
							Status:     pb.DialResponse_OK,
							DialStatus: pb.DialStatus_OK,
							AddrIdx:    1,
						},
					},
				})
				s.Close()
			},
			success: true,
		},
		{
			name: "no dial attempt",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				if _, _, err := readReq(r); err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				resp := &pb.DialResponse{
					Status:     pb.DialResponse_OK,
					DialStatus: pb.DialStatus_OK,
					AddrIdx:    0,
				}
				w := pbio.NewDelimitedWriter(s)
				w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialResponse{
						DialResponse: resp,
					},
				})
				s.Close()
			},
			success: false,
		},
		{
			name: "invalid reported address",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				addrs, nonce, err := readReq(r)
				if err != nil {
					s.Reset()
					t.Error(err)
					return
				}

				if err := writeNonce(addrs[1], nonce); err != nil {
					s.Reset()
					t.Error(err)
					return
				}

				w := pbio.NewDelimitedWriter(s)
				w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialResponse{
						DialResponse: &pb.DialResponse{
							Status:     pb.DialResponse_OK,
							DialStatus: pb.DialStatus_OK,
							AddrIdx:    0,
						},
					},
				})
				s.Close()
			},
			success: false,
		},
		{
			name: "invalid nonce",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				addrs, nonce, err := readReq(r)
				if err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				if err := writeNonce(addrs[0], nonce-1); err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				w := pbio.NewDelimitedWriter(s)
				w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialResponse{
						DialResponse: &pb.DialResponse{
							Status:     pb.DialResponse_OK,
							DialStatus: pb.DialStatus_OK,
							AddrIdx:    0,
						},
					},
				})
				s.Close()
			},
			success: false,
		},
		{
			name: "invalid addr index",
			handler: func(s network.Stream) {
				r := pbio.NewDelimitedReader(s, maxMsgSize)
				_, _, err := readReq(r)
				if err != nil {
					s.Reset()
					t.Error(err)
					return
				}
				w := pbio.NewDelimitedWriter(s)
				w.WriteMsg(&pb.Message{
					Msg: &pb.Message_DialResponse{
						DialResponse: &pb.DialResponse{
							Status:     pb.DialResponse_OK,
							DialStatus: pb.DialStatus_OK,
							AddrIdx:    10,
						},
					},
				})
				s.Close()
			},
			success: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addrs := an.host.Addrs()
			b.SetStreamHandler(DialProtocol, tc.handler)
			res, err := an.GetReachability(
				context.Background(),
				[]Request{
					{Addr: addrs[0], SendDialData: true},
					{Addr: addrs[1]},
				})
			if !tc.success {
				require.Error(t, err)
				require.Equal(t, Result{}, res)
			} else {
				require.NoError(t, err)
				require.Equal(t, res.Reachability, network.ReachabilityPublic)
			}
		})
	}
}

func TestEventSubscription(t *testing.T) {
	an := newAutoNAT(t, nil)
	defer an.host.Close()

	b := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer b.Close()
	c := bhost.NewBlankHost(swarmt.GenSwarm(t))
	defer c.Close()

	idAndConnect(t, an.host, b)
	require.Eventually(t, func() bool {
		an.mx.Lock()
		defer an.mx.Unlock()
		return len(an.peers.peers) == 1
	}, 5*time.Second, 100*time.Millisecond)

	idAndConnect(t, an.host, c)
	require.Eventually(t, func() bool {
		an.mx.Lock()
		defer an.mx.Unlock()
		return len(an.peers.peers) == 2
	}, 5*time.Second, 100*time.Millisecond)

	an.host.Network().ClosePeer(b.ID())
	require.Eventually(t, func() bool {
		an.mx.Lock()
		defer an.mx.Unlock()
		return len(an.peers.peers) == 1
	}, 5*time.Second, 100*time.Millisecond)

	an.host.Network().ClosePeer(c.ID())
	require.Eventually(t, func() bool {
		an.mx.Lock()
		defer an.mx.Unlock()
		return len(an.peers.peers) == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestAreAddrsConsistency(t *testing.T) {
	c := &client{}
	tests := []struct {
		name      string
		localAddr ma.Multiaddr
		dialAddr  ma.Multiaddr
		success   bool
	}{
		{
			name:      "simple match",
			localAddr: ma.StringCast("/ip4/192.168.0.1/tcp/12345"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/tcp/23232"),
			success:   true,
		},
		{
			name:      "nat64",
			localAddr: ma.StringCast("/ip6/1::1/tcp/12345"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/tcp/23232"),
			success:   false,
		},
		{
			name:      "simple mismatch",
			localAddr: ma.StringCast("/ip4/192.168.0.1/tcp/12345"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/udp/23232/quic-v1"),
			success:   false,
		},
		{
			name:      "quic-vs-webtransport",
			localAddr: ma.StringCast("/ip4/192.168.0.1/udp/12345/quic-v1"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/udp/123/quic-v1/webtransport"),
			success:   false,
		},
		{
			name:      "webtransport-certhash",
			localAddr: ma.StringCast("/ip4/192.168.0.1/udp/12345/quic-v1/webtransport"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/udp/123/quic-v1/webtransport/certhash/uEgNmb28"),
			success:   true,
		},
		{
			name:      "dns",
			localAddr: ma.StringCast("/dns/lib.p2p/udp/12345/quic-v1"),
			dialAddr:  ma.StringCast("/ip6/1::1/udp/123/quic-v1/"),
			success:   false,
		},
		{
			name:      "dns6",
			localAddr: ma.StringCast("/dns6/lib.p2p/udp/12345/quic-v1"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/udp/123/quic-v1/"),
			success:   false,
		},
		{
			name:      "wss",
			dialAddr:  ma.StringCast("/dns/lib.p2p/tcp/1/wss"),
			localAddr: ma.StringCast("/ip4/1.2.3.4/tcp/1/tls/ws"),
			success:   true,
		},
		{
			name:      "tls-sni",
			localAddr: ma.StringCast("/ip4/1.2.3.4/tcp/1/wss"),
			dialAddr:  ma.StringCast("/ip4/1.2.3.4/tcp/1/tls/sni/abc.xyz/ws"),
			success:   true,
		},
		{
			name:      "only p2p",
			localAddr: ma.StringCast("/p2p/QmYo41GybvrXk8y8Xnm1P7pfA4YEXCpfnLyzgRPnNbG35e"),
			dialAddr:  ma.StringCast("/p2p/QmYo41GybvrXk8y8Xnm1P7pfA4YEXCpfnLyzgRPnNbG35e"),
			success:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if c.areAddrsConsistent(tc.localAddr, tc.dialAddr) != tc.success {
				wantStr := "match"
				if !tc.success {
					wantStr = "mismatch"
				}
				t.Errorf("expected %s between\nlocal addr: %s\ndial addr:  %s", wantStr, tc.localAddr, tc.dialAddr)
			}
		})
	}
}

func TestPeerMap(t *testing.T) {
	pm := newPeersMap()
	// Add 1, 2, 3
	pm.Put(peer.ID("1"))
	pm.Put(peer.ID("2"))
	pm.Put(peer.ID("3"))

	// Remove 3, 2
	pm.Delete(peer.ID("3"))
	pm.Delete(peer.ID("2"))

	// Add 4
	pm.Put(peer.ID("4"))

	// Remove 3, 2 again. Should be no op
	pm.Delete(peer.ID("3"))
	pm.Delete(peer.ID("2"))

	contains := []peer.ID{"1", "4"}
	elems := make([]peer.ID, 0, 8)
	for p := range pm.Shuffled() {
		elems = append(elems, p)
	}
	require.ElementsMatch(t, contains, elems)
}

func FuzzClient(f *testing.F) {
	a := newAutoNAT(f, nil, AllowPrivateAddrs, WithServerRateLimit(math.MaxInt32, math.MaxInt32, math.MaxInt32, 2))
	c := newAutoNAT(f, nil)
	idAndWait(f, c, a)

	// TODO: Move this to go-multiaddrs
	getProto := func(protos []byte) ma.Multiaddr {
		protoType := 0
		if len(protos) > 0 {
			protoType = int(protos[0])
		}

		port1, port2 := 0, 0
		if len(protos) > 1 {
			port1 = int(protos[1])
		}
		if len(protos) > 2 {
			port2 = int(protos[2])
		}
		protoTemplates := []string{
			"/tcp/%d/",
			"/udp/%d/",
			"/udp/%d/quic-v1/",
			"/udp/%d/quic-v1/tcp/%d",
			"/udp/%d/quic-v1/webtransport/",
			"/udp/%d/webrtc/",
			"/udp/%d/webrtc-direct/",
			"/unix/hello/",
		}
		s := protoTemplates[protoType%len(protoTemplates)]
		port1 %= (1 << 16)
		if strings.Count(s, "%d") == 1 {
			return ma.StringCast(fmt.Sprintf(s, port1))
		}
		port2 %= (1 << 16)
		return ma.StringCast(fmt.Sprintf(s, port1, port2))
	}

	getIP := func(ips []byte) ma.Multiaddr {
		ipType := 0
		if len(ips) > 0 {
			ipType = int(ips[0])
		}
		ips = ips[1:]
		var x, y int64
		split := min(len(ips), 128/8)
		var b [8]byte
		copy(b[:], ips[:split])
		x = int64(binary.LittleEndian.Uint64(b[:]))
		clear(b[:])
		copy(b[:], ips[split:])
		y = int64(binary.LittleEndian.Uint64(b[:]))

		var ip netip.Addr
		switch ipType % 3 {
		case 0:
			ip = netip.AddrFrom4([4]byte{byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24)})
			return ma.StringCast(fmt.Sprintf("/ip4/%s/", ip))
		case 1:
			pubIP := net.ParseIP("2005::") // Public IP address
			x := int64(binary.LittleEndian.Uint64(pubIP[0:8]))
			ip = netip.AddrFrom16([16]byte{
				byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24),
				byte(x >> 32), byte(x >> 40), byte(x >> 48), byte(x >> 56),
				byte(y), byte(y >> 8), byte(y >> 16), byte(y >> 24),
				byte(y >> 32), byte(y >> 40), byte(y >> 48), byte(y >> 56),
			})
			return ma.StringCast(fmt.Sprintf("/ip6/%s/", ip))
		default:
			ip := netip.AddrFrom16([16]byte{
				byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24),
				byte(x >> 32), byte(x >> 40), byte(x >> 48), byte(x >> 56),
				byte(y), byte(y >> 8), byte(y >> 16), byte(y >> 24),
				byte(y >> 32), byte(y >> 40), byte(y >> 48), byte(y >> 56),
			})
			return ma.StringCast(fmt.Sprintf("/ip6/%s/", ip))
		}
	}

	getAddr := func(addrType int, ips, protos []byte) ma.Multiaddr {
		switch addrType % 4 {
		case 0:
			return getIP(ips).Encapsulate(getProto(protos))
		case 1:
			return getProto(protos)
		case 2:
			return nil
		default:
			return getIP(ips).Encapsulate(getProto(protos))
		}
	}

	getDNSAddr := func(hostNameBytes, protos []byte) ma.Multiaddr {
		hostName := strings.ReplaceAll(string(hostNameBytes), "\\", "")
		hostName = strings.ReplaceAll(hostName, "/", "")
		if hostName == "" {
			hostName = "localhost"
		}
		dnsType := 0
		if len(hostNameBytes) > 0 {
			dnsType = int(hostNameBytes[0])
		}
		dnsProtos := []string{"dns", "dns4", "dns6", "dnsaddr"}
		da := ma.StringCast(fmt.Sprintf("/%s/%s/", dnsProtos[dnsType%len(dnsProtos)], hostName))
		return da.Encapsulate(getProto(protos))
	}

	const maxAddrs = 100
	getAddrs := func(numAddrs int, ips, protos, hostNames []byte) []ma.Multiaddr {
		if len(ips) == 0 || len(protos) == 0 || len(hostNames) == 0 {
			return nil
		}
		numAddrs = ((numAddrs % maxAddrs) + maxAddrs) % maxAddrs
		addrs := make([]ma.Multiaddr, numAddrs)
		ipIdx := 0
		protoIdx := 0
		for i := range numAddrs {
			addrs[i] = getAddr(i, ips[ipIdx:], protos[protoIdx:])
			ipIdx = (ipIdx + 1) % len(ips)
			protoIdx = (protoIdx + 1) % len(protos)
		}
		maxDNSAddrs := 10
		protoIdx = 0
		for i := 0; i < len(hostNames) && i < maxDNSAddrs; i += 2 {
			ed := min(i+2, len(hostNames))
			addrs = append(addrs, getDNSAddr(hostNames[i:ed], protos[protoIdx:]))
			protoIdx = (protoIdx + 1) % len(protos)
		}
		return addrs
	}
	// reduce the streamTimeout before running this. TODO: fix this
	f.Fuzz(func(_ *testing.T, numAddrs int, ips, protos, hostNames []byte) {
		addrs := getAddrs(numAddrs, ips, protos, hostNames)
		reqs := make([]Request, len(addrs))
		for i, addr := range addrs {
			reqs[i] = Request{Addr: addr, SendDialData: true}
		}
		c.GetReachability(context.Background(), reqs)
	})
}

func TestNormalizeMultiaddr(t *testing.T) {
	require.Equal(t,
		"/ip4/1.2.3.4/udp/9999/quic-v1/webtransport",
		normalizeMultiaddr(ma.StringCast("/ip4/1.2.3.4/udp/9999/quic-v1/webtransport/certhash/uEgNmb28")).String(),
	)
}
