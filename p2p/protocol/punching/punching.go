package punching

import (
	"context"
	"strings"
	"time"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	swarm "github.com/libp2p/go-libp2p-swarm"
	identify "github.com/libp2p/go-libp2p/p2p/protocol/identify"
	identify_pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
	punching_pb "github.com/libp2p/go-libp2p/p2p/protocol/punching/pb"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("punching")

const PunchingProtocol = "/libp2p/punching/1.0.0"

type PunchingService struct {
	host      host.Host
	idService *identify.IDService
}

func NewPunchingService(h host.Host, idService *identify.IDService) *PunchingService {
	punchingService := &PunchingService{
		host:      h,
		idService: idService,
	}
	h.SetStreamHandler(PunchingProtocol, punchingService.PunchingHandler)
	h.Network().Notify(punchingService)
	return punchingService
}

func (p *PunchingService) PunchingHandler(s inet.Stream) {
	log.Debug("Incoming Punching Request", s.Protocol(), s.Conn().RemoteMultiaddr().String())

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	var incoming punching_pb.Punching
	err := r.ReadMsg(&incoming)
	if err != nil {
		log.Debug("err read punching_pb.Punching:", err)
		return
	}
	log.Debug("Incoming peer addresses:", incoming.Addresses)
	if len(incoming.Addresses) == 0 {
		return
	}

	go func() {
		maddrs := make([]ma.Multiaddr, 0, len(incoming.Addresses))
		for _, a := range incoming.Addresses {
			maddr, err := ma.NewMultiaddr(a)
			if err != nil {
				log.Debug("Cannot convert", a, "to multiaddress:", err)
				continue
			}
			maddrs = append(maddrs, maddr)
		}
		if len(maddrs) == 0 {
			return
		}
		peerInfo := pstore.PeerInfo{
			ID:    s.Conn().RemotePeer(),
			Addrs: maddrs,
		}

		timeCount := 0
		for timeCount < 5 {
			timeCount += 1

			// copied from go-ipfs/core/coreapi/swarm
			if swrm, ok := p.host.Network().(*swarm.Swarm); ok {
				swrm.Backoff().Clear(peerInfo.ID)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := p.host.Connect(ctx, peerInfo)
			cancel()
			if err != nil {
				log.Info("Failed to connect", peerInfo.ID, "err:", err)
			} else {
				log.Info("Punch-connect to", incoming.Addresses, "successful")
				return
			}
			time.Sleep(10 * time.Second)
		}
	}()
}

func (p *PunchingService) punch(peerID peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := p.host.NewStream(ctx, peerID, PunchingProtocol)
	cancel()
	if err != nil {
		log.Debug("Failed to create new stream to", peerID)
		return
	}
	allAddresses := p.host.Addrs()
	quicAddress := make([]string, 0, len(allAddresses))
	for _, a := range allAddresses {
		s := a.String()
		if strings.Contains(s, "/quic") &&
			!strings.Contains(s, "/ip6") &&
			!strings.Contains(s, "/127.0.0.1") &&
			!strings.Contains(s, "/::") &&
			!strings.Contains(s, "/192.168.") &&
			!strings.Contains(s, "/10.") &&
			!strings.Contains(s, "/172.") {
			quicAddress = append(quicAddress, s)
		}
	}
	if len(quicAddress) == 0 {
		return
	}
	log.Debug("QUIC addresses:", quicAddress)
	msg := punching_pb.Punching{
		Addresses: quicAddress,
	}

	w := ggio.NewDelimitedWriter(stream)
	err = w.WriteMsg(&msg)
	if err != nil {
		log.Debug("err write punching_pb.Punching:", err)
		return
	}
}

//
// Implement inet.Notifiee
//
func (p *PunchingService) Listen(inet.Network, ma.Multiaddr)      {} // called when network starts listening on an addr
func (p *PunchingService) ListenClose(inet.Network, ma.Multiaddr) {} // called when network starts listening on an addr
func (p *PunchingService) Connected(net inet.Network, conn inet.Conn) { // called when a connection opened
	go func() {
		time.Sleep(5 * time.Second)
		log.Debug("Connected", conn.LocalMultiaddr().String(), "<==>", conn.RemoteMultiaddr().String())

		peerID := conn.RemotePeer()
		protos, err := p.host.Peerstore().SupportsProtocols(peerID, PunchingProtocol)
		if err != nil {
			log.Debug("error retrieving supported protocols for peer %s: %s\n", peerID, err)
			return
		}

		if len(protos) > 0 {
			selfNatStatus := p.idService.GetNatStatus()
			var peerNatStatus identify_pb.Identify_NATStatus
			peerNatStatusRaw, err := p.host.Peerstore().Get(peerID, "natStatus")
			if err == nil {
				peerNatStatus = peerNatStatusRaw.(identify_pb.Identify_NATStatus)
			}
			log.Info("Discovered punching-support peer", peerID.String(),
				"self NAT status", selfNatStatus,
				"peer NAT status", peerNatStatus)
			if selfNatStatus == identify_pb.Identify_NATStatusPublic ||
				peerNatStatus == identify_pb.Identify_NATStatusPublic {
				return
			}

			p.punch(peerID)
		}
	}()
}
func (p *PunchingService) Disconnected(inet.Network, inet.Conn) {} // called when a connection closed
func (p *PunchingService) OpenedStream(net inet.Network, stream inet.Stream) { // called when a stream opened
	go func() {
		time.Sleep(5 * time.Second)
		log.Debug("Connected",
			stream.Conn().LocalMultiaddr().String(), "<==>", stream.Conn().RemoteMultiaddr().String(),
			"protocol", stream.Protocol())
	}()
}
func (p *PunchingService) ClosedStream(inet.Network, inet.Stream) {} // called when a stream closed
