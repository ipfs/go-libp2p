package holepunch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	logging "github.com/libp2p/go-libp2p/gologshim"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-msgio/pbio"

	ma "github.com/multiformats/go-multiaddr"
)

const defaultDirectDialTimeout = 10 * time.Second

// Protocol is the libp2p protocol for Hole Punching.
const Protocol protocol.ID = "/libp2p/dcutr"

var log = logging.Logger("p2p-holepunch")

// StreamTimeout is the timeout for the hole punch protocol stream.
var StreamTimeout = 1 * time.Minute

const (
	ServiceName = "libp2p.holepunch"

	maxMsgSize = 4 * 1024 // 4K
)

// ErrClosed is returned when the hole punching is closed
var ErrClosed = errors.New("hole punching service closing")

type Option func(*Service) error

func DirectDialTimeout(timeout time.Duration) Option {
	return func(s *Service) error {
		s.directDialTimeout = timeout
		return nil
	}
}

// The Service runs on every node that supports the DCUtR protocol.
type Service struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	host host.Host
	// ids helps with connection reversal. We wait for identify to complete and attempt
	// a direct connection to the peer if it's publicly reachable.
	ids identify.IDService
	// listenAddrs provides the addresses for the host to be used for hole punching. We use this
	// and not host.Addrs because host.Addrs might remove public unreachable address and only advertise
	// publicly reachable relay addresses.
	listenAddrs func() []ma.Multiaddr

	directDialTimeout time.Duration
	holePuncherMx     sync.Mutex
	holePuncher       *holePuncher

	hasPublicAddrsChan chan struct{}

	tracer *tracer
	filter AddrFilter

	refCount sync.WaitGroup
}

// NewService creates a new service that can be used for hole punching
// The Service runs on all hosts that support the DCUtR protocol,
// no matter if they are behind a NAT / firewall or not.
// The Service handles DCUtR streams (which are initiated from the node behind
// a NAT / Firewall once we establish a connection to them through a relay.
//
// listenAddrs MUST only return public addresses.
func NewService(h host.Host, ids identify.IDService, listenAddrs func() []ma.Multiaddr, opts ...Option) (*Service, error) {
	if ids == nil {
		return nil, errors.New("identify service can't be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		ctx:                ctx,
		ctxCancel:          cancel,
		host:               h,
		ids:                ids,
		listenAddrs:        listenAddrs,
		hasPublicAddrsChan: make(chan struct{}),
		directDialTimeout:  defaultDirectDialTimeout,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			cancel()
			return nil, err
		}
	}
	s.tracer.Start()

	s.refCount.Add(1)
	go s.waitForPublicAddr()

	return s, nil
}

func (s *Service) waitForPublicAddr() {
	defer s.refCount.Done()

	log.Debug("waiting until we have at least one public address", "peer", s.host.ID())

	// TODO: We should have an event here that fires when identify discovers a new
	// address.
	// As we currently don't have an event like this, just check our observed addresses
	// regularly (exponential backoff starting at 250 ms, capped at 5s).
	duration := 250 * time.Millisecond
	const maxDuration = 5 * time.Second
	t := time.NewTimer(duration)
	defer t.Stop()
	for {
		if len(s.listenAddrs()) > 0 {
			log.Debug("Host now has a public address", "hostID", s.host.ID(), "addresses", s.host.Addrs())
			s.host.SetStreamHandler(Protocol, s.handleNewStream)
			break
		}

		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			duration *= 2
			if duration > maxDuration {
				duration = maxDuration
			}
			t.Reset(duration)
		}
	}

	s.holePuncherMx.Lock()
	defer s.holePuncherMx.Unlock()
	if s.ctx.Err() != nil {
		// service is closed
		return
	}
	s.holePuncher = newHolePuncher(s.host, s.ids, s.listenAddrs, s.directDialTimeout, s.tracer, s.filter)
	close(s.hasPublicAddrsChan)
}

// Close closes the Hole Punch Service.
func (s *Service) Close() error {
	var err error
	s.ctxCancel()
	s.holePuncherMx.Lock()
	if s.holePuncher != nil {
		err = s.holePuncher.Close()
	}
	s.holePuncherMx.Unlock()
	s.tracer.Close()
	s.host.RemoveStreamHandler(Protocol)
	s.refCount.Wait()
	return err
}

func (s *Service) incomingHolePunch(str network.Stream) (rtt time.Duration, remoteAddrs []ma.Multiaddr, ownAddrs []ma.Multiaddr, err error) {
	// sanity check: a hole punch request should only come from peers behind a relay
	if !isRelayAddress(str.Conn().RemoteMultiaddr()) {
		return 0, nil, nil, fmt.Errorf("received hole punch stream: %s", str.Conn().RemoteMultiaddr())
	}
	ownAddrs = s.listenAddrs()
	if s.filter != nil {
		ownAddrs = s.filter.FilterLocal(str.Conn().RemotePeer(), ownAddrs)
	}

	// If we can't tell the peer where to dial us, there's no point in starting the hole punching.
	if len(ownAddrs) == 0 {
		return 0, nil, nil, errors.New("rejecting hole punch request, as we don't have any public addresses")
	}

	if err := str.Scope().ReserveMemory(maxMsgSize, network.ReservationPriorityAlways); err != nil {
		log.Debug("error reserving memory for stream", "err", err)
		return 0, nil, nil, err
	}
	defer str.Scope().ReleaseMemory(maxMsgSize)

	wr := pbio.NewDelimitedWriter(str)
	rd := pbio.NewDelimitedReader(str, maxMsgSize)

	// Read Connect message
	msg := new(pb.HolePunch)

	str.SetDeadline(time.Now().Add(StreamTimeout))

	if err := rd.ReadMsg(msg); err != nil {
		return 0, nil, nil, fmt.Errorf("failed to read message from initiator: %w", err)
	}
	if t := msg.GetType(); t != pb.HolePunch_CONNECT {
		return 0, nil, nil, fmt.Errorf("expected CONNECT message from initiator but got %d", t)
	}

	obsDial := removeRelayAddrs(addrsFromBytes(msg.ObsAddrs))
	if s.filter != nil {
		obsDial = s.filter.FilterRemote(str.Conn().RemotePeer(), obsDial)
	}
	// Dial only the public IPs the peer sent, after any AddrFilter.
	obsDial = filterHolePunchAddrs(obsDial)

	log.Debug("received hole punch request", "peer", str.Conn().RemotePeer(), "addrs", obsDial)
	if len(obsDial) == 0 {
		return 0, nil, nil, errors.New("expected CONNECT message to contain at least one address")
	}

	// Write CONNECT message
	msg.Reset()
	msg.Type = pb.HolePunch_CONNECT.Enum()
	msg.ObsAddrs = addrsToBytes(ownAddrs)
	tstart := time.Now()
	if err := wr.WriteMsg(msg); err != nil {
		return 0, nil, nil, fmt.Errorf("failed to write CONNECT message to initiator: %w", err)
	}

	// Read SYNC message
	msg.Reset()
	if err := rd.ReadMsg(msg); err != nil {
		return 0, nil, nil, fmt.Errorf("failed to read message from initiator: %w", err)
	}
	if t := msg.GetType(); t != pb.HolePunch_SYNC {
		return 0, nil, nil, fmt.Errorf("expected SYNC message from initiator but got %d", t)
	}
	return time.Since(tstart), obsDial, ownAddrs, nil
}

func (s *Service) handleNewStream(str network.Stream) {
	// Check directionality of the underlying connection.
	// Peer A receives an inbound connection from peer B.
	// Peer A opens a new hole punch stream to peer B.
	// Peer B receives this stream, calling this function.
	// Peer B sees the underlying connection as an outbound connection.
	if str.Conn().Stat().Direction == network.DirInbound {
		str.Reset()
		return
	}

	if err := str.Scope().SetService(ServiceName); err != nil {
		log.Debug("error attaching stream to holepunch service", "err", err)
		str.Reset()
		return
	}

	rp := str.Conn().RemotePeer()
	rtt, addrs, ownAddrs, err := s.incomingHolePunch(str)
	if err != nil {
		s.tracer.ProtocolError(rp, err)
		log.Debug("error handling holepunching stream", "peer", rp, "err", err)
		str.Reset()
		return
	}
	str.Close()

	// Hole punch now by forcing a connect
	pi := peer.AddrInfo{
		ID:    rp,
		Addrs: addrs,
	}
	s.tracer.StartHolePunch(rp, addrs, rtt)
	log.Debug("starting hole punch", "peer", rp)
	start := time.Now()
	s.tracer.HolePunchAttempt(pi.ID)
	ctx, cancel := context.WithTimeout(s.ctx, s.directDialTimeout)
	err = holePunchConnect(ctx, s.host, pi, true) // true (Client)
	cancel()
	dt := time.Since(start)
	s.tracer.EndHolePunch(rp, dt, err)
	s.tracer.HolePunchFinished("receiver", 1, addrs, ownAddrs, getDirectConnection(s.host, rp))
}

// DirectConnect is only exposed for testing purposes.
// TODO: find a solution for this.
func (s *Service) DirectConnect(p peer.ID) error {
	<-s.hasPublicAddrsChan
	s.holePuncherMx.Lock()
	holePuncher := s.holePuncher
	s.holePuncherMx.Unlock()
	return holePuncher.DirectConnect(p)
}
