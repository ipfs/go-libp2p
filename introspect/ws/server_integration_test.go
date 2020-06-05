package ws

import (
	"context"
	"io"
	"runtime"
	"testing"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/introspection/pb"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/introspect"

	"github.com/stretchr/testify/require"
)

var (
	msg1 = []byte("1")
	msg2 = []byte("12")
	msg3 = []byte("111")
	msg4 = []byte("0000")

	p1 = protocol.ID("h1h3")
	p2 = protocol.ID("h2h1")
)

func TestHostIntrospection(t *testing.T) {
	require := require.New(t)

	iaddr := "127.0.0.1:0"
	ctx := context.Background()

	// create host 1 with introspect
	h1, err := libp2p.New(ctx,
		libp2p.Introspection(
			introspect.NewDefaultIntrospector,
			EndpointWithConfig(&EndpointConfig{ListenAddrs: []string{iaddr}}),
		),
		libp2p.BandwidthReporter(metrics.NewBandwidthCounter()),
	)
	require.NoError(err)
	defer h1.Close()

	// create host 2
	h2, err := libp2p.New(ctx)
	defer h2.Close()

	// create host 3
	h3, err := libp2p.New(ctx)
	defer h3.Close()

	// host1 -> CONNECTS -> host2
	require.NoError(h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}))

	// host3 -> CONNECTS -> host1
	require.NoError(h3.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}))

	// host1 -> OPENS STREAM 1 -> host3, Writes a message & then reads the response
	h3.SetStreamHandler(p1, func(s network.Stream) {
		buf := make([]byte, len(msg1))

		_, err := io.ReadFull(s, buf)
		require.NoError(err)

		_, err = s.Write(msg2)
		require.NoError(err)
	})

	s1, err := h1.NewStream(ctx, h3.ID(), p1)
	require.NoError(err)

	_, err = s1.Write(msg1)
	require.NoError(err)

	buf := make([]byte, len(msg2))
	_, err = io.ReadFull(s1, buf)
	require.NoError(err)

	// host2 -> OPENS Stream 2 -> host1 , writes a message & reads the response
	h1.SetStreamHandler(p2, func(s network.Stream) {
		buf := make([]byte, len(msg3))

		_, err := io.ReadFull(s, buf)
		require.NoError(err)

		_, err = s.Write(msg4)
		require.NoError(err)
	})

	s2, err := h2.NewStream(ctx, h1.ID(), p2)
	require.NoError(err)

	_, err = s2.Write(msg3)
	require.NoError(err)

	buf = make([]byte, len(msg4))
	_, err = io.ReadFull(s2, buf)
	require.NoError(err)

	conn := createConn(t, h1.(host.IntrospectableHost).IntrospectionEndpoint())
	defer conn.Close()

	// first, we get the runtime and verify it.
	conn.greet()

	conn.sendCommand(&pb.ClientCommand{Id: 200, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_RUNTIME})
	msg := conn.readNext()
	rt := msg.GetRuntime()
	require.NotNil(t, rt)
	require.Equal(h1.ID().String(), rt.PeerId)
	require.Equal(runtime.GOOS, rt.Platform)
	require.Equal("go-libp2p", rt.Implementation)

	// now we get the state.
	conn.sendCommand(&pb.ClientCommand{Id: 200, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_STATE})
	msg = conn.readNext()
	st := msg.GetState()
	require.NotNil(t, st)
	assertState(require, st, h1, h2, h3, false)
}

func assertState(require *require.Assertions, state *pb.State, h1, h2, h3 host.Host,
	assertTraffic bool) {

	if assertTraffic {
		require.Greater(state.Traffic.TrafficIn.CumBytes, uint64(100))
		require.Greater(state.Traffic.TrafficOut.CumBytes, uint64(100))
	}

	// Connections
	conns := state.Subsystems.Connections
	peerIdToConns := make(map[string]*pb.Connection)
	for _, c := range conns {
		peerIdToConns[c.PeerId] = c
	}
	require.Len(peerIdToConns, 2)

	pconn := make(map[string]network.Conn)
	for _, c := range h1.Network().Conns() {
		pconn[c.RemotePeer().String()] = c
	}
	require.Len(pconn, 2)

	// host1 -> host2 connection
	h2Conn := peerIdToConns[h2.ID().String()]
	require.NotEmpty(h2Conn.Id)
	require.Equal(pb.Status_ACTIVE, h2Conn.Status)
	require.Equal(pconn[h2.ID().String()].LocalMultiaddr().String(), h2Conn.Endpoints.SrcMultiaddr)
	require.Equal(pconn[h2.ID().String()].RemoteMultiaddr().String(), h2Conn.Endpoints.DstMultiaddr)
	require.Equal(pb.Role_INITIATOR, h2Conn.Role)

	if assertTraffic {
		require.Greater(h2Conn.Traffic.TrafficIn.CumBytes, uint64(len(msg3)))
	}

	// host3 -> host1 connection
	h3Conn := peerIdToConns[h3.ID().String()]
	require.NotEmpty(h3Conn.Id)
	require.Equal(pb.Status_ACTIVE, h3Conn.Status)
	require.Equal(pconn[h3.ID().String()].LocalMultiaddr().String(), h3Conn.Endpoints.SrcMultiaddr)
	require.Equal(pconn[h3.ID().String()].RemoteMultiaddr().String(), h3Conn.Endpoints.DstMultiaddr)
	require.Equal(pb.Role_RESPONDER, h3Conn.Role)

	if assertTraffic {
		require.Greater(h3Conn.Traffic.TrafficIn.CumBytes, uint64(len(msg2)))
		require.Greater(h3Conn.Traffic.TrafficOut.CumBytes, uint64(len(msg1)))
	}
	// stream1
	require.Len(h3Conn.Streams.Streams, 1)
	h3Stream := h3Conn.Streams.Streams[0]
	require.NotEmpty(h3Stream.Id)
	require.Equal(string(p1), h3Stream.Protocol)
	require.Equal(pb.Role_INITIATOR, h3Stream.Role)
	require.Equal(pb.Status_ACTIVE, h3Stream.Status)
	// require.True(len(msg1) == int(h3Stream.Traffic.TrafficOut.CumBytes))
	// require.True(len(msg2) == int(h3Stream.Traffic.TrafficIn.CumBytes))

	// stream 2
	require.Len(h2Conn.Streams.Streams, 1)
	h1Stream := h2Conn.Streams.Streams[0]
	require.NotEmpty(h1Stream.Id)
	require.Equal(string(p2), h1Stream.Protocol)
	require.Equal(pb.Role_RESPONDER, h1Stream.Role)
	require.Equal(pb.Status_ACTIVE, h1Stream.Status)
	// require.True(len(msg3) == int(h1Stream.Traffic.TrafficIn.CumBytes))
}
