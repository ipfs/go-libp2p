package introspect

import (
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/introspection"
	"github.com/libp2p/go-libp2p-core/introspection/pb"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-eventbus"
	"github.com/multiformats/go-multiaddr"

	"github.com/hashicorp/go-multierror"
)

var _ introspection.Introspector = (*DefaultIntrospector)(nil)

// DefaultIntrospector is an object that introspects the system.
type DefaultIntrospector struct {
	*eventManager

	host     host.Host
	bus      event.Bus
	wsub     event.Subscription
	reporter metrics.Reporter
	started  time.Time
}

func NewDefaultIntrospector(host host.Host, reporter metrics.Reporter) (introspection.Introspector, error) {
	bus := host.EventBus()
	if bus == nil {
		return nil, fmt.Errorf("introspector requires a host with eventbus capability")
	}

	sub, err := bus.Subscribe(event.WildcardSubscription, eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to susbcribe for events with WildcardSubscription")
	}

	d := &DefaultIntrospector{
		eventManager: newEventManager(sub.Out()),
		host:         host,
		bus:          bus,
		wsub:         sub,
		reporter:     reporter,
		started:      time.Now(),
	}

	return d, nil
}

func (d *DefaultIntrospector) Close() error {
	var err *multierror.Error
	if err := d.wsub.Close(); err != nil {
		err = multierror.Append(err, fmt.Errorf("failed while trying to close wildcard eventbus subscription: %w", err))
	}

	close(d.closeCh)
	d.closeWg.Wait()

	return err.ErrorOrNil()
}

func (d *DefaultIntrospector) FetchRuntime() (*pb.Runtime, error) {
	return &pb.Runtime{
		Implementation: "go-libp2p",
		Platform:       runtime.GOOS,
		PeerId:         d.host.ID().Pretty(),
		Version:        "",
	}, nil
}

func (d *DefaultIntrospector) FetchFullState() (state *pb.State, err error) {
	var (
		now      = time.Now()
		netconns = d.host.Network().Conns()
		conns    = make([]*pb.Connection, 0, len(netconns))
		traffic  *pb.Traffic
	)

	for _, conn := range netconns {
		c, err := d.IntrospectConnection(conn)
		if err != nil {
			return nil, err
		}
		conns = append(conns, c)
	}

	// subsystems and traffic.
	traffic, err = d.IntrospectGlobalTraffic()
	if err != nil {
		return nil, err
	}

	state = &pb.State{
		// timestamps in millis since epoch.
		StartTs:   uint64(d.started.UnixNano() / int64(time.Millisecond)),
		InstantTs: uint64(now.UnixNano() / int64(time.Millisecond)),
		Subsystems: &pb.Subsystems{
			Connections: conns,
		},
		Traffic: traffic,
	}

	return state, nil
}

// IntrospectGlobalTraffic introspects and returns total traffic stats for this swarm.
func (d *DefaultIntrospector) IntrospectGlobalTraffic() (*pb.Traffic, error) {
	if d.reporter == nil {
		return nil, nil
	}

	metrics := d.reporter.GetBandwidthTotals()
	t := &pb.Traffic{
		TrafficIn: &pb.DataGauge{
			CumBytes: uint64(metrics.TotalIn),
			InstBw:   uint64(metrics.RateIn),
		},
		TrafficOut: &pb.DataGauge{
			CumBytes: uint64(metrics.TotalOut),
			InstBw:   uint64(metrics.RateOut),
		},
	}

	return t, nil
}

func (d *DefaultIntrospector) IntrospectConnection(conn network.Conn) (*pb.Connection, error) {
	stat := conn.Stat()
	openTs := uint64(stat.Opened.UnixNano() / 1000000)

	res := &pb.Connection{
		Id:     []byte(conn.ID()),
		Status: pb.Status_ACTIVE,
		PeerId: conn.RemotePeer().Pretty(),
		Endpoints: &pb.EndpointPair{
			SrcMultiaddr: conn.LocalMultiaddr().String(),
			DstMultiaddr: conn.RemoteMultiaddr().String(),
		},
		Role: translateRole(stat),

		Timeline: &pb.Connection_Timeline{
			OpenTs:     openTs,
			UpgradedTs: openTs,
			// TODO ClosedTs, UpgradedTs.
		},
	}

	// TODO this is a per-peer, not a per-conn measurement. In the future, when
	//  we have multiple connections per peer, this will produce inaccurate
	//  numbers. Also, we do not record stream-level stats.
	//  We don't have packet I/O stats.
	if r := d.reporter; r != nil {
		bw := r.GetBandwidthForPeer(conn.RemotePeer())
		res.Traffic = &pb.Traffic{
			TrafficIn: &pb.DataGauge{
				CumBytes: uint64(bw.TotalIn),
				InstBw:   uint64(math.Round(bw.RateIn)),
			},
			TrafficOut: &pb.DataGauge{
				CumBytes: uint64(bw.TotalOut),
				InstBw:   uint64(math.Round(bw.RateOut)),
			},
		}
	}

	// TODO I don't think we pin the multiplexer and the secure channel we've
	//  negotiated anywhere.
	res.Attribs = &pb.Connection_Attributes{}

	// TransportId with format "ip4+tcp" or "ip6+udp+quic".
	res.TransportId = func() []byte {
		tptAddr, _ := peer.SplitAddr(conn.RemoteMultiaddr())
		var str string
		multiaddr.ForEach(tptAddr, func(c multiaddr.Component) bool {
			str += c.Protocol().Name + "+"
			return true
		})
		return []byte(str[0 : len(str)-1])
	}()

	// TODO there's the ping protocol, but that's higher than this layer.
	//  How do we source this? We may need some kind of latency manager.
	res.LatencyNs = 0

	streams := conn.GetStreams()
	res.Streams = &pb.StreamList{
		Streams: make([]*pb.Stream, 0, len(streams)),
	}

	for _, stream := range streams {
		s, err := d.IntrospectStream(stream)
		if err != nil {
			return nil, err
		}
		res.Streams.Streams = append(res.Streams.Streams, s)
	}

	return res, nil
}

func (d *DefaultIntrospector) IntrospectStream(stream network.Stream) (*pb.Stream, error) {
	stat := stream.Stat()
	openTs := uint64(stat.Opened.UnixNano() / 1000000)

	res := &pb.Stream{
		Id:     []byte(stream.ID()),
		Status: pb.Status_ACTIVE,
		Conn: &pb.Stream_ConnectionRef{
			Connection: &pb.Stream_ConnectionRef_ConnId{
				ConnId: []byte(stream.Conn().ID()),
			},
		},
		Protocol: string(stream.Protocol()),
		Role:     translateRole(stat),
		Timeline: &pb.Stream_Timeline{
			OpenTs: openTs,
			// TODO CloseTs.
		},
		// TODO Traffic: we are not tracking per-stream traffic stats at the moment.
		Traffic: &pb.Traffic{TrafficIn: &pb.DataGauge{}, TrafficOut: &pb.DataGauge{}},
	}
	return res, nil
}

func translateRole(stat network.Stat) pb.Role {
	switch stat.Direction {
	case network.DirInbound:
		return pb.Role_RESPONDER
	case network.DirOutbound:
		return pb.Role_INITIATOR
	default:
		return 99 // TODO placeholder value
	}
}
