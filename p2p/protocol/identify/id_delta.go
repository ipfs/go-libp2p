package identify

import (
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	ggio "github.com/gogo/protobuf/io"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
)

const IDDelta = "/p2p/id/delta/1.0.0"

// deltaHandler handles incoming delta updates from peers.
func (ids *IDService) deltaHandler(s network.Stream) {
	c := s.Conn()

	r := ggio.NewDelimitedReader(s, 2048)
	mes := pb.Identify{}
	if err := r.ReadMsg(&mes); err != nil {
		log.Warn("error reading identify message: ", err)
		s.Reset()
		return
	}

	defer func() { go helpers.FullClose(s) }()
	log.Debugf("%s received message from %s %s", s.Protocol(), c.RemotePeer(), c.RemoteMultiaddr())

	delta := mes.GetDelta()
	if delta == nil {
		return
	}

	p := s.Conn().RemotePeer()
	if err := ids.consumeDelta(p, delta); err != nil {
		log.Warnf("delta update from peer %s failed: %s", p, err)
	}
}

// fireProtocolDelta fires a delta message to all connected peers to signal a local protocol table update.
func (ids *IDService) fireProtocolDelta(evt event.EvtLocalProtocolsUpdated) {
	mes := pb.Identify{
		Delta: &pb.Delta{
			AddedProtocols: protocol.ConvertToStrings(evt.Added),
			RmProtocols:    protocol.ConvertToStrings(evt.Removed),
		},
	}
	deltaWriter := func(s network.Stream) {
		defer helpers.FullClose(s)
		c := s.Conn()
		err := ggio.NewDelimitedWriter(s).WriteMsg(&mes)
		if err != nil {
			log.Warnf("%s error while sending delta update to %s: %s", IDDelta, c.RemotePeer(), c.RemoteMultiaddr())
			return
		}
		log.Debugf("%s sent delta update to %s: %s", IDDelta, c.RemotePeer(), c.RemoteMultiaddr())
	}
	ids.broadcast(IDDelta, deltaWriter)
}

// consumeDelta processes an incoming delta from a peer, updating the peerstore
// and emitting the appropriate events.
func (ids *IDService) consumeDelta(id peer.ID, delta *pb.Delta) error {
	err := ids.Host.Peerstore().AddProtocols(id, delta.GetAddedProtocols()...)
	if err != nil {
		return err
	}

	err = ids.Host.Peerstore().RemoveProtocols(id, delta.GetRmProtocols()...)
	if err != nil {
		return err
	}

	evt := event.EvtPeerProtocolsUpdated{
		Peer:    id,
		Added:   protocol.ConvertFromStrings(delta.GetAddedProtocols()),
		Removed: protocol.ConvertFromStrings(delta.GetRmProtocols()),
	}
	ids.emitters.evtPeerProtocolsUpdated.Emit(evt)
	return nil
}
