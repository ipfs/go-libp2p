package introspect_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/introspection/pb"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/introspect"

	"github.com/stretchr/testify/require"
)

func TestConnsAndStreamIntrospect(t *testing.T) {
	ctx := context.Background()

	bwc1 := metrics.NewBandwidthCounter()
	h1, err := libp2p.New(ctx, libp2p.BandwidthReporter(bwc1))
	require.NoError(t, err)

	bwc2 := metrics.NewBandwidthCounter()
	h2, err := libp2p.New(ctx, libp2p.BandwidthReporter(bwc2))
	require.NoError(t, err)

	introspector1, err := introspect.NewDefaultIntrospector(h1, bwc1)
	require.NoError(t, err)
	_, _ = introspect.NewDefaultIntrospector(h2, bwc2)

	h1.Peerstore().AddAddrs(h2.ID(), h2.Network().ListenAddresses(), peerstore.PermanentAddrTTL)
	err = h1.Connect(ctx, h2.Peerstore().PeerInfo(h2.ID()))
	require.NoError(t, err)

	// ----- H1 opens two streams to H2
	pid1, pid2 := protocol.ID("1"), protocol.ID("2")
	h2.SetStreamHandler(pid1, func(stream network.Stream) {})
	h2.SetStreamHandler(pid2, func(stream network.Stream) {})

	s1, err := h1.NewStream(ctx, h2.ID(), pid1)
	require.NoError(t, err)
	s2, err := h1.NewStream(ctx, h2.ID(), pid2)
	require.NoError(t, err)

	// send 4 bytes on stream 1 & 5 bytes on stream 2
	msg1 := "abcd"
	msg2 := "12345"
	_, err = s1.Write([]byte(msg1))
	require.NoError(t, err)
	_, err = s2.Write([]byte(msg2))
	require.NoError(t, err)

	// wait for the metrics to kick in
	require.Eventually(t, func() bool {
		state, _ := introspector1.FetchFullState()
		return state.Traffic.TrafficOut.CumBytes != 0
	}, 3*time.Second, 100*time.Millisecond)

	// ----- Introspect host 1.
	state, err := introspector1.FetchFullState()
	require.NoError(t, err)
	conns := state.Subsystems.Connections

	// connection asserts
	require.Len(t, conns, 1)
	require.NotEmpty(t, conns[0].Id)
	require.Equal(t, h2.ID().String(), conns[0].PeerId)
	require.Equal(t, pb.Status_ACTIVE, conns[0].Status)
	require.Equal(t, pb.Role_INITIATOR, conns[0].Role)
	require.Equal(t, h1.Network().Conns()[0].LocalMultiaddr().String(), conns[0].Endpoints.SrcMultiaddr)
	require.Equal(t, h1.Network().Conns()[0].RemoteMultiaddr().String(), conns[0].Endpoints.DstMultiaddr)

	// stream asserts.
	streams := conns[0].Streams.Streams
	require.Len(t, streams, 2)
	require.NoError(t, err)

	// map stream to protocols
	protos := make(map[string]*pb.Stream)
	for _, s := range streams {
		protos[s.Protocol] = s
	}

	// introspect stream 1
	stream1 := protos["1"]
	require.NotEmpty(t, stream1)
	require.Equal(t, "1", stream1.Protocol)
	require.Equal(t, pb.Role_INITIATOR, stream1.Role)
	require.Equal(t, pb.Status_ACTIVE, stream1.Status)
	require.NotEmpty(t, stream1.Id)
	require.NotNil(t, stream1.Traffic)
	require.NotNil(t, stream1.Traffic.TrafficIn)
	require.NotNil(t, stream1.Traffic.TrafficOut)

	// introspect stream 2
	stream2 := protos["2"]
	require.NotEmpty(t, stream2)
	require.Equal(t, "2", stream2.Protocol)
	require.Equal(t, pb.Role_INITIATOR, stream2.Role)
	require.Equal(t, pb.Status_ACTIVE, stream2.Status)
	require.NotEmpty(t, stream2.Id)
	require.NotEqual(t, stream2.Id, stream1.Id)

	// introspect traffic
	tr := state.Traffic
	require.NoError(t, err)
	require.NotZero(t, tr.TrafficOut.CumBytes)
	require.Zero(t, tr.TrafficIn.CumBytes == 0)
}
