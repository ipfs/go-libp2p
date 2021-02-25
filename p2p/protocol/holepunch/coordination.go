package holepunch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	logging "github.com/ipfs/go-log"
	protoio "github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// TODO Should we have options for these ?
var (
	// Protocol is the libp2p protocol for Hole Punching.
	Protocol protocol.ID = "/libp2p/holepunch/1.0.0"
	// HolePunchTimeout is the timeout for the hole punch protocol stream.
	HolePunchTimeout = 1 * time.Minute

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

	tracer *Tracer

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

type Option func(*HolePunchService) error

// NewHolePunchService creates a new service that can be used for hole punching
// The `isTest` should ONLY be turned ON for testing.
func NewHolePunchService(h host.Host, ids *identify.IDService, opts ...Option) (*HolePunchService, error) {
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
	}

	for _, opt := range opts {
		err := opt(hs)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	h.SetStreamHandler(Protocol, hs.handleNewStream)
	h.Network().Notify((*netNotifiee)(hs))
	return hs, nil
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

			tstart := time.Now()
			err := hs.host.Connect(dialCtx, peer.AddrInfo{ID: rp})
			dt := time.Since(tstart)
			cancel()

			if err == nil {
				hs.tracer.DirectDialSuccessful(rp, dt)
				log.Debugf("direct connection to peer %s successful, no need for a hole punch", rp.Pretty())
				return nil
			}
			hs.tracer.DirectDialFailed(rp, dt, err)
			break
		}
	}

	// hole punch
	hpCtx := network.WithUseTransient(hs.ctx, "hole-punch")
	sCtx := network.WithNoDial(hpCtx, "hole-punch")
	s, err := hs.host.NewStream(sCtx, rp, Protocol)
	if err != nil {
		err = fmt.Errorf("failed to open hole-punching stream with peer %s: %w", rp, err)
		hs.tracer.ProtocolError(rp, err)
		return err
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
		err = fmt.Errorf("failed to send hole punch CONNECT: %w", err)
		hs.tracer.ProtocolError(rp, err)
		return err
	}

	// wait for a CONNECT message from the remote peer
	rd := protoio.NewDelimitedReader(s, maxMsgSize)
	msg.Reset()
	if err := rd.ReadMsg(msg); err != nil {
		s.Reset()
		err = fmt.Errorf("failed to read CONNECT message from remote peer: %w", err)
		hs.tracer.ProtocolError(rp, err)
		return err
	}
	rtt := time.Since(tstart)

	if t := msg.GetType(); t != pb.HolePunch_CONNECT {
		s.Reset()
		err = fmt.Errorf("expected CONNECT message but got %d", t)
		hs.tracer.ProtocolError(rp, err)
		return err
	}

	obsRemote := addrsFromBytes(msg.ObsAddrs)

	// send a SYNC message and attempt a direct connect after half the RTT
	msg.Reset()
	msg.Type = pb.HolePunch_SYNC.Enum()
	if err := w.WriteMsg(msg); err != nil {
		s.Reset()
		err = fmt.Errorf("failed to send SYNC message for hole punching: %w", err)
		hs.tracer.ProtocolError(rp, err)
		return err
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
		tstart = time.Now()
		hs.tracer.StartHolePunch(rp, obsRemote, rtt)
		err = hs.holePunchConnectWithRetry(pi)
		dt := time.Since(tstart)
		hs.tracer.EndHolePunch(rp, dt, err)
		if err == nil {
			log.Infof("hole punching with %s successful after %s", rp, dt)
		}
		return err

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

func (hs *HolePunchService) handlerError(p peer.ID, err error) {
	if !hs.isTest {
		hs.tracer.ProtocolError(p, err)
		log.Warn(err)
		return
	}

	hs.handlerErrsMu.Lock()
	defer hs.handlerErrsMu.Unlock()

	hs.handlerErrs = append(hs.handlerErrs, err)
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
		hs.handlerError(rp, fmt.Errorf("failed to read message from initator: %w", err))
		return
	}
	if t := msg.GetType(); t != pb.HolePunch_CONNECT {
		s.Reset()
		hs.handlerError(rp, fmt.Errorf("expected CONNECT message from initiator but got %d", t))
		return
	}
	obsDial := addrsFromBytes(msg.ObsAddrs)

	// Write CONNECT message
	msg.Reset()
	msg.Type = pb.HolePunch_CONNECT.Enum()
	msg.ObsAddrs = addrsToBytes(hs.ids.OwnObservedAddrs())
	tstart := time.Now()
	if err := wr.WriteMsg(msg); err != nil {
		s.Reset()
		hs.handlerError(rp, fmt.Errorf("failed to write CONNECT message to initator:: %w", err))
		return
	}

	// Read SYNC message
	msg.Reset()
	if err := rd.ReadMsg(msg); err != nil {
		s.Reset()
		hs.handlerError(rp, fmt.Errorf("failed to read message from initator: %w", err))
		return
	}
	rtt := time.Since(tstart)

	if t := msg.GetType(); t != pb.HolePunch_SYNC {
		s.Reset()
		hs.handlerError(rp, fmt.Errorf("expected SYNC message from initiator but got %d", t))
		return
	}
	defer s.Close()

	// Hole punch now by forcing a connect
	pi := peer.AddrInfo{
		ID:    rp,
		Addrs: obsDial,
	}

	hs.tracer.StartHolePunch(rp, obsDial, rtt)
	tstart = time.Now()
	err := hs.holePunchConnectWithRetry(pi)
	dt := time.Since(tstart)
	hs.tracer.EndHolePunch(rp, dt, err)
	if err != nil {
		log.Warnf("hole punching with %s failed after %s: %s", rp, dt, err)
	} else {
		log.Infof("hole punching with %s successful after %s", rp, dt)
	}
}

func (hs *HolePunchService) holePunchConnectWithRetry(pi peer.AddrInfo) error {
	log.Debugf("starting hole punch with %s", pi.ID)
	holePunchCtx := network.WithSimultaneousConnect(hs.ctx, "hole-punching")
	forceDirectConnCtx := network.WithForceDirectDial(holePunchCtx, "hole-punching")

	doConnect := func(attempt int) error {
		dialCtx, cancel := context.WithTimeout(forceDirectConnCtx, dialTimeout)
		defer cancel()

		hs.tracer.HolePunchAttempt(pi.ID, attempt)
		err := hs.host.Connect(dialCtx, pi)
		if err == nil {
			log.Infof("hole punch with peer %s successful after %d retries; direct conns to peer are:", pi.ID, attempt)
			for _, c := range hs.host.Network().ConnsToPeer(pi.ID) {
				if !isRelayAddress(c.RemoteMultiaddr()) {
					log.Info(c)
				}
			}
		}

		return err
	}

	err := doConnect(0)
	if err == nil {
		return nil
	}

	log.Infof("first hole punch attempt with peer %s failed: %s; will retry in %s...", pi.ID, err, retryWait)

	for i := 1; i <= maxRetries; i++ {
		time.Sleep(retryWait)

		err = doConnect(i)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("all retries for hole punch with peer %s failed: %w", pi.ID, err)
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

			err := hs.HolePunch(v.RemotePeer())
			if err != nil {
				log.Warnf("hole punching attempt with %s failed: %s", v.RemotePeer(), err)
			}
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
