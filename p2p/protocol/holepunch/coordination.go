package holepunch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// TODO Should we have options for these ?
var (
	// Protocol is the libp2p protocol for Hole Punching.
	Protocol protocol.ID = "/libp2p/holepunch/1.0.0"
	// HolePunchTimeout is the timeout for the hole punch protocol stream.
	HolePunchTimeout = 1 * time.Minute
	// ErrNATHolePunchingUnsupported means hole punching is NOT supported by the NAT on either or both peers.
	ErrNATHolePunchingUnsupported = errors.New("NAT does NOT support hole punching")

	maxMsgSize  = 4 * 1024 // 4K
	dialTimeout = 5 * time.Second
	maxRetries  = 5
	retryWait   = 2 * time.Second
)

var (
	log = logging.Logger("p2p-holepunch")
)

// TODO Find a better name for this protocol.
// HolePunchService is used to make direct connections with a peer via hole-punching.
type HolePunchService struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	ids  *identify.IDService
	host host.Host

	// ensure we shutdown ONLY once
	closeSync sync.Once
	refCount  sync.WaitGroup

	// active hole punches for deduplicating
	activeMx sync.Mutex
	active   map[peer.ID]struct{}

	isTest        bool
	handlerErrsMu sync.Mutex
	handlerErrs   []error
}

// NewHolePunchService creates a new service that can be used for hole punching
// The `isTest` should ONLY be turned ON for testing.
func NewHolePunchService(h host.Host, ids *identify.IDService, isTest bool) (*HolePunchService, error) {
	if ids == nil {
		return nil, errors.New("Identify service can't be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	hs := &HolePunchService{
		ctx:       ctx,
		ctxCancel: cancel,
		host:      h,
		ids:       ids,
		active:    make(map[peer.ID]struct{}),
		isTest:    isTest,
	}

	sub, err := h.EventBus().Subscribe(new(event.EvtNATDeviceTypeChanged))
	if err != nil {
		return nil, err
	}

	h.SetStreamHandler(Protocol, hs.handleNewStream)
	h.Network().Notify((*netNotifiee)(hs))

	hs.refCount.Add(1)
	go hs.loop(sub)

	return hs, nil
}

func (hs *HolePunchService) loop(sub event.Subscription) {
	defer hs.refCount.Done()
	defer sub.Close()

	for {
		select {
		// Our local NAT device types are intialized in the peerstore when the Host is created
		//	and updated in the peerstore by the Observed Address Manager.
		case _, ok := <-sub.Out():
			if !ok {
				return
			}

			if hs.PeerSupportsHolePunching(hs.host.ID(), hs.host.Addrs()) {
				hs.host.SetStreamHandler(Protocol, hs.handleNewStream)
			} else {
				hs.host.RemoveStreamHandler(Protocol)
			}

		case <-hs.ctx.Done():
			return
		}
	}
}

func hasProtoAddr(protocCode int, addrs []ma.Multiaddr) bool {
	for _, a := range addrs {
		if _, err := a.ValueForProtocol(protocCode); err == nil {
			return true
		}
	}

	return false
}

// Close closes the Hole Punch Service.
func (hs *HolePunchService) Close() error {
	hs.closeSync.Do(func() {
		hs.ctxCancel()
		hs.refCount.Wait()
	})

	return nil
}

// attempts to make a direct connection with the remote peer of `relayConn` by co-ordinating a hole punch over
// the given relay connection `relayConn`.
func (hs *HolePunchService) HolePunch(rp peer.ID) error {
	// short-circuit hole punching if a direct dial works.
	// attempt a direct connection ONLY if we have a public address for the remote peer
	for _, a := range hs.host.Peerstore().Addrs(rp) {
		if manet.IsPublicAddr(a) && !isRelayAddress(a) {
			forceDirectConnCtx := network.WithForceDirectDial(hs.ctx, "hole-punching")
			dialCtx, cancel := context.WithTimeout(forceDirectConnCtx, dialTimeout)
			defer cancel()
			if err := hs.host.Connect(dialCtx, peer.AddrInfo{ID: rp}); err == nil {
				log.Debugf("direct connection to peer %s successful, no need for a hole punch", rp.Pretty())
				return nil
			}
			break
		}
	}

	// return if either peer does NOT support hole punching
	if !hs.PeerSupportsHolePunching(rp, hs.host.Peerstore().Addrs(rp)) ||
		!hs.PeerSupportsHolePunching(hs.host.ID(), hs.host.Addrs()) {
		return ErrNATHolePunchingUnsupported
	}

	// hole punch
	hpCtx := network.WithUseTransient(hs.ctx, "hole-punch")
	sCtx := network.WithNoDial(hpCtx, "hole-punch")
	s, err := hs.host.NewStream(sCtx, rp, Protocol)
	if err != nil {
		msg := fmt.Sprintf("failed to open hole-punching stream with peer %s, err: %s", rp, err)
		log.Error(msg)
		return errors.New(msg)
	}
	log.Infof("will attempt hole punch with peer %s", rp.Pretty())
	_ = s.SetDeadline(time.Now().Add(HolePunchTimeout))
	w := protoio.NewDelimitedWriter(s)

	// send a CONNECT and start RTT measurement.
	msg := new(pb.HolePunch)
	msg.Type = pb.HolePunch_CONNECT.Enum()
	msg.ObsAddrs = addrsToBytes(hs.ids.OwnObservedAddrs())

	tstart := time.Now()
	if err := w.WriteMsg(msg); err != nil {
		s.Reset()
		msg := fmt.Sprintf("failed to send hole punch CONNECT, err: %s", err)

		log.Error(msg)
		return errors.New(msg)
	}

	// wait for a CONNECT message from the remote peer
	rd := protoio.NewDelimitedReader(s, maxMsgSize)
	msg.Reset()
	if err := rd.ReadMsg(msg); err != nil {
		s.Reset()

		msg := fmt.Sprintf("failed to read HolePunch_CONNECT message from remote peer, err: %s", err)
		log.Error(msg)
		return errors.New(msg)
	}
	rtt := time.Since(tstart)

	if msg.GetType() != pb.HolePunch_CONNECT {
		s.Reset()
		msg := fmt.Sprintf("expected HolePunch_CONNECT message, got %s", msg.GetType())

		log.Debug(msg)
		return errors.New(msg)
	}

	obsRemote := addrsFromBytes(msg.ObsAddrs)

	// send a SYNC message and attempt a direct connect after half the RTT
	msg.Reset()
	msg.Type = pb.HolePunch_SYNC.Enum()
	if err := w.WriteMsg(msg); err != nil {
		s.Reset()
		msg := fmt.Sprintf("failed to send SYNC message for hole punching, err: %s", err)
		log.Error(msg)
		return errors.New(msg)
	}
	defer s.Close()

	synTime := rtt / 2
	log.Debugf("peer RTT is %s; starting hole punch in %s", rtt, synTime)

	// wait for sync to reach the other peer and then punch a hole for it in our NAT
	// by attempting a connect to it.
	select {
	case <-time.After(synTime):
		pi := peer.AddrInfo{
			ID:    rp,
			Addrs: obsRemote,
		}
		return hs.holePunchConnectWithRetry(pi)

	case <-hs.ctx.Done():
		return hs.ctx.Err()
	}
}

// HandlerErrors returns the errors accumulated by the Stream Handler.
// This is ONLY for testing.
func (hs *HolePunchService) HandlerErrors() []error {
	hs.handlerErrsMu.Lock()
	defer hs.handlerErrsMu.Unlock()
	return hs.handlerErrs
}

func (hs *HolePunchService) appendHandlerErr(err error) {
	hs.handlerErrsMu.Lock()
	defer hs.handlerErrsMu.Unlock()

	if hs.isTest {
		hs.handlerErrs = append(hs.handlerErrs, err)
	}
}

// PeerSupportsHolePunching returns true if the given peer with the given addresses supports hole punching.
// It uses the peer's NAT device type detected via Identify.
// We can hole punch with a peer ONLY if it is NOT behind a symmetric NAT for all the transport protocol it supports.
func (hs *HolePunchService) PeerSupportsHolePunching(p peer.ID, addrs []ma.Multiaddr) bool {
	udpSupported := hasProtoAddr(ma.P_UDP, addrs)
	tcpSupported := hasProtoAddr(ma.P_TCP, addrs)

	if udpSupported {
		udpNAT, err := hs.host.Peerstore().Get(p, identify.UDPNATDeviceTypeKey)
		if err != nil {
			return false
		}
		udpNatType := udpNAT.(network.NATDeviceType)

		if udpNatType == network.NATDeviceTypeCone || udpNatType == network.NATDeviceTypeUnknown {
			return true
		}
	}

	if tcpSupported {
		tcpNAT, err := hs.host.Peerstore().Get(p, identify.TCPNATDeviceTypeKey)
		if err != nil {
			return false
		}

		tcpNATType := tcpNAT.(network.NATDeviceType)
		if tcpNATType == network.NATDeviceTypeCone || tcpNATType == network.NATDeviceTypeUnknown {
			return true
		}
	}

	return false
}

func (hs *HolePunchService) handleNewStream(s network.Stream) {
	log.Infof("got hole punch request from peer %s", s.Conn().RemotePeer().Pretty())
	_ = s.SetDeadline(time.Now().Add(HolePunchTimeout))
	rp := s.Conn().RemotePeer()
	wr := protoio.NewDelimitedWriter(s)
	rd := protoio.NewDelimitedReader(s, maxMsgSize)

	// Read Connect message
	msg := new(pb.HolePunch)
	if err := rd.ReadMsg(msg); err != nil {
		s.Reset()
		hs.appendHandlerErr(fmt.Errorf("failed to read message from initator, err: %s", err))
		return
	}
	if msg.GetType() != pb.HolePunch_CONNECT {
		s.Reset()
		hs.appendHandlerErr(errors.New("did not get expected HolePunch_CONNECT message from initiator"))
		return
	}
	obsDial := addrsFromBytes(msg.ObsAddrs)

	// Write CONNECT message
	msg.Reset()
	msg.Type = pb.HolePunch_CONNECT.Enum()
	msg.ObsAddrs = addrsToBytes(hs.ids.OwnObservedAddrs())
	if err := wr.WriteMsg(msg); err != nil {
		s.Reset()
		hs.appendHandlerErr(fmt.Errorf("failed to write HolePunch_CONNECT message to initator, err: %s", err))
		return
	}

	// Read SYNC message
	msg.Reset()
	if err := rd.ReadMsg(msg); err != nil {
		s.Reset()
		hs.appendHandlerErr(fmt.Errorf("failed to read message from initator, err: %s", err))
		return
	}
	if msg.GetType() != pb.HolePunch_SYNC {
		s.Reset()
		hs.appendHandlerErr(errors.New("did not get expected HolePunch_SYNC message from initiator"))
		return
	}
	defer s.Close()

	// Hole punch now by forcing a connect
	pi := peer.AddrInfo{
		ID:    rp,
		Addrs: obsDial,
	}
	_ = hs.holePunchConnectWithRetry(pi)
}

func (hs *HolePunchService) holePunchConnectWithRetry(pi peer.AddrInfo) error {
	log.Debugf("starting hole punch with %s", pi.ID)
	holePunchCtx := network.WithSimultaneousConnect(hs.ctx, "hole-punching")
	forceDirectConnCtx := network.WithForceDirectDial(holePunchCtx, "hole-punching")
	dialCtx, cancel := context.WithTimeout(forceDirectConnCtx, dialTimeout)
	defer cancel()
	err := hs.host.Connect(dialCtx, pi)
	if err == nil {
		log.Infof("hole punch with peer %s successful, direct conns to peer are:", pi.ID.Pretty())
		for _, c := range hs.host.Network().ConnsToPeer(pi.ID) {
			if !isRelayAddress(c.RemoteMultiaddr()) {
				log.Info(c)
			}
		}
		return nil
	} else {
		log.Infof("first hole punch attempt with peer %s failed, error: %s, will retry now...", pi.ID.Pretty(), err)
	}

	for i := 1; i <= maxRetries; i++ {
		time.Sleep(retryWait)

		dialCtx, cancel := context.WithTimeout(forceDirectConnCtx, dialTimeout)
		defer cancel()

		err = hs.host.Connect(dialCtx, pi)
		if err == nil {
			log.Infof("hole punch with peer %s successful after %d retries, direct conns to peer are:", pi.ID.Pretty(), i)
			for _, c := range hs.host.Network().ConnsToPeer(pi.ID) {
				if !isRelayAddress(c.RemoteMultiaddr()) {
					log.Info(c)
				}
			}
			return nil
		}
	}
	log.Errorf("all retries for hole punch with peer %s failed, err: %s", pi.ID.Pretty(), err)

	return err
}

func isRelayAddress(a ma.Multiaddr) bool {
	_, err := a.ValueForProtocol(ma.P_CIRCUIT)

	return err == nil
}

func addrsToBytes(as []ma.Multiaddr) [][]byte {
	bzs := make([][]byte, 0, len(as))
	for _, a := range as {
		bzs = append(bzs, a.Bytes())
	}

	return bzs
}

func addrsFromBytes(bzs [][]byte) []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, 0, len(bzs))
	for _, bz := range bzs {
		a, err := ma.NewMultiaddrBytes(bz)
		if err == nil {
			addrs = append(addrs, a)
		}
	}

	return addrs
}

type netNotifiee HolePunchService

func (nn *netNotifiee) HolePunchService() *HolePunchService {
	return (*HolePunchService)(nn)
}

func (nn *netNotifiee) Connected(_ network.Network, v network.Conn) {
	hs := nn.HolePunchService()
	dir := v.Stat().Direction

	// Hole punch if it's an inbound proxy connection.
	// If we already have a direct connection with the remote peer, this will be a no-op.
	if dir == network.DirInbound && isRelayAddress(v.RemoteMultiaddr()) {
		// short-circuit check to see if we already have a direct connection
		for _, c := range hs.host.Network().ConnsToPeer(v.RemotePeer()) {
			if !isRelayAddress(c.RemoteMultiaddr()) {
				return
			}
		}

		p := v.RemotePeer()
		hs.activeMx.Lock()
		_, active := hs.active[p]
		if !active {
			hs.active[p] = struct{}{}
		}
		hs.activeMx.Unlock()

		if active {
			return
		}

		log.Debugf("got inbound proxy conn from peer %s, connectionID is %s", v.RemotePeer().String(), v.ID())
		hs.refCount.Add(1)
		go func() {
			defer hs.refCount.Done()
			defer func() {
				hs.activeMx.Lock()
				delete(hs.active, p)
				hs.activeMx.Unlock()
			}()
			select {
			// waiting for Identify here will allow us to access the peer's public and observed addresses
			// that we can dial to for a hole punch.
			case <-hs.ids.IdentifyWait(v):
			case <-hs.ctx.Done():
				return
			}

			hs.HolePunch(v.RemotePeer())
		}()
		return
	}
}

// NO-OPS
func (nn *netNotifiee) Disconnected(_ network.Network, v network.Conn)   {}
func (nn *netNotifiee) OpenedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee) ListenClose(n network.Network, a ma.Multiaddr)    {}
