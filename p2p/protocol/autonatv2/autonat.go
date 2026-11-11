package autonatv2

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	logging "github.com/libp2p/go-libp2p/gologshim"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	ServiceName      = "libp2p.autonatv2"
	DialBackProtocol = "/libp2p/autonat/2/dial-back"
	DialProtocol     = "/libp2p/autonat/2/dial-request"

	maxMsgSize            = 8192
	streamTimeout         = 15 * time.Second
	dialBackStreamTimeout = 5 * time.Second
	dialBackDialTimeout   = 10 * time.Second
	dialBackMaxMsgSize    = 1024
	minHandshakeSizeBytes = 30_000 // for amplification attack prevention
	maxHandshakeSizeBytes = 100_000
	// maxPeerAddresses is the number of addresses in a dial request the server
	// will inspect, rest are ignored.
	maxPeerAddresses = 50

	defaultThrottlePeerDuration = 2 * time.Minute
)

var (
	// ErrNoPeers is returned when the client knows no autonatv2 servers.
	ErrNoPeers = errors.New("no peers for autonat v2")
	// ErrPrivateAddrs is returned when the request has private IP addresses.
	ErrPrivateAddrs = errors.New("private addresses cannot be verified with autonatv2")

	log = logging.Logger("autonatv2")
)

// Request is the request to verify reachability of a single address
type Request struct {
	// Addr is the multiaddr to verify
	Addr ma.Multiaddr
	// SendDialData indicates whether to send dial data if the server requests it for Addr
	SendDialData bool
}

// Result is the result of the CheckReachability call
type Result struct {
	// Addr is the dialed address
	Addr ma.Multiaddr
	// Idx is the index of the address that was dialed
	Idx int
	// Reachability is the reachability for `Addr`
	Reachability network.Reachability
	// AllAddrsRefused is true when the server refused to dial all the addresses in the request.
	AllAddrsRefused bool
}

// AutoNAT implements the AutoNAT v2 client and server.
// Users can check reachability for their addresses using the CheckReachability method.
// The server provides amplification attack prevention and rate limiting.
type AutoNAT struct {
	host host.Host

	// for cleanly closing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	srv *server
	cli *client

	mx           sync.Mutex
	peers        *peersMap
	throttlePeer map[peer.ID]time.Time
	// throttlePeerDuration is the duration to wait before making another dial request to the
	// same server.
	throttlePeerDuration time.Duration
	// allowPrivateAddrs enables using private and localhost addresses for reachability checks.
	// This is only useful for testing.
	allowPrivateAddrs bool
	forceReachability *network.Reachability
}

// New returns a new AutoNAT instance.
// host and dialerHost should have the same dialing capabilities. In case the host doesn't support
// a transport, dial back requests for address for that transport will be ignored.
func New(dialerHost host.Host, opts ...AutoNATOption) (*AutoNAT, error) {
	s := defaultSettings()
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	an := &AutoNAT{
		ctx:                  ctx,
		cancel:               cancel,
		srv:                  newServer(dialerHost, s),
		cli:                  newClient(s),
		allowPrivateAddrs:    s.allowPrivateAddrs,
		forceReachability:    s.forceReachability,
		peers:                newPeersMap(),
		throttlePeer:         make(map[peer.ID]time.Time),
		throttlePeerDuration: s.throttlePeerDuration,
	}
	return an, nil
}

func (an *AutoNAT) background(sub event.Subscription) {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-an.ctx.Done():
			sub.Close()
			an.wg.Done()
			return
		case e := <-sub.Out():
			switch evt := e.(type) {
			case event.EvtPeerProtocolsUpdated:
				an.updatePeer(evt.Peer)
			case event.EvtPeerConnectednessChanged:
				an.updatePeer(evt.Peer)
			case event.EvtPeerIdentificationCompleted:
				an.updatePeer(evt.Peer)
			default:
				log.Error("unexpected event", "event_type", fmt.Sprintf("%T", e))
			}
		case <-ticker.C:
			now := time.Now()
			an.mx.Lock()
			for p, t := range an.throttlePeer {
				if t.Before(now) {
					delete(an.throttlePeer, p)
				}
			}
			an.mx.Unlock()
		}
	}
}

func (an *AutoNAT) Start(h host.Host) error {
	an.host = h
	// Listen on event.EvtPeerProtocolsUpdated, event.EvtPeerConnectednessChanged
	// event.EvtPeerIdentificationCompleted to maintain our set of autonat supporting peers.
	sub, err := an.host.EventBus().Subscribe([]any{
		new(event.EvtPeerProtocolsUpdated),
		new(event.EvtPeerConnectednessChanged),
		new(event.EvtPeerIdentificationCompleted),
	})
	if err != nil {
		return fmt.Errorf("event subscription failed: %w", err)
	}
	an.cli.Start(h)
	an.srv.Start(h)

	an.wg.Add(1)
	go an.background(sub)
	return nil
}

func (an *AutoNAT) Close() {
	an.cancel()
	an.wg.Wait()
	an.srv.Close()
	an.cli.Close()
	an.mx.Lock()
	an.peers = nil
	an.mx.Unlock()
}

// GetReachability makes a single dial request for checking reachability for requested addresses
func (an *AutoNAT) GetReachability(ctx context.Context, reqs []Request) (Result, error) {
	if an.forceReachability != nil && len(reqs) > 0 {
		return Result{Addr: reqs[0].Addr, Idx: 0, Reachability: *an.forceReachability}, nil
	}
	var filteredReqs []Request
	if !an.allowPrivateAddrs {
		filteredReqs = make([]Request, 0, len(reqs))
		for _, r := range reqs {
			if manet.IsPublicAddr(r.Addr) {
				filteredReqs = append(filteredReqs, r)
			} else {
				log.Error("private address in reachability check", "address", r.Addr)
			}
		}
		if len(filteredReqs) == 0 {
			return Result{}, ErrPrivateAddrs
		}
	} else {
		filteredReqs = reqs
	}
	p, err := an.pickServer()
	if err != nil {
		return Result{}, err
	}
	res, err := an.cli.GetReachability(ctx, p, filteredReqs)
	if err != nil {
		log.Debug("reachability check failed", "peer", p, "err", err)
		return res, fmt.Errorf("reachability check with %s failed: %w", p, err)
	}
	// restore the correct index in case we'd filtered private addresses
	for i, r := range reqs {
		if r.Addr.Equal(res.Addr) {
			res.Idx = i
			break
		}
	}
	log.Debug("reachability check successful", "peer", p)
	return res, nil
}

// pickServer returns an autonatv2 server that isn't currently throttled, and throttles
// it for throttlePeerDuration. It returns ErrNoPeers if there is no such server, or if
// autonat has been closed.
func (an *AutoNAT) pickServer() (peer.ID, error) {
	an.mx.Lock()
	defer an.mx.Unlock()

	// nil after Close; host shutdown can have in-flight reachability checks
	if an.peers == nil {
		return "", ErrNoPeers
	}
	now := time.Now()
	for p := range an.peers.Shuffled() {
		if t := an.throttlePeer[p]; t.After(now) {
			continue
		}
		an.throttlePeer[p] = time.Now().Add(an.throttlePeerDuration)
		return p, nil
	}
	return "", ErrNoPeers
}

func (an *AutoNAT) updatePeer(p peer.ID) {
	an.mx.Lock()
	defer an.mx.Unlock()

	// There are no ordering gurantees between identify and swarm events. Check peerstore
	// and swarm for the current state
	protos, err := an.host.Peerstore().SupportsProtocols(p, DialProtocol)
	connectedness := an.host.Network().Connectedness(p)
	if err == nil && connectedness == network.Connected && slices.Contains(protos, DialProtocol) {
		an.peers.Put(p)
	} else {
		an.peers.Delete(p)
	}
}

// peersMap provides random access to a set of peers. This is useful when the map iteration order is
// not sufficiently random.
type peersMap struct {
	peerIdx map[peer.ID]int
	peers   []peer.ID
}

func newPeersMap() *peersMap {
	return &peersMap{
		peerIdx: make(map[peer.ID]int),
		peers:   make([]peer.ID, 0),
	}
}

// Shuffled iterates over the map in random order
func (p *peersMap) Shuffled() iter.Seq[peer.ID] {
	n := len(p.peers)
	start := 0
	if n > 0 {
		start = rand.IntN(n)
	}
	return func(yield func(peer.ID) bool) {
		for i := range n {
			if !yield(p.peers[(i+start)%n]) {
				return
			}
		}
	}
}

func (p *peersMap) Put(id peer.ID) {
	if _, ok := p.peerIdx[id]; ok {
		return
	}
	p.peers = append(p.peers, id)
	p.peerIdx[id] = len(p.peers) - 1
}

func (p *peersMap) Delete(id peer.ID) {
	idx, ok := p.peerIdx[id]
	if !ok {
		return
	}
	n := len(p.peers)
	lastPeer := p.peers[n-1]
	p.peers[idx] = lastPeer
	p.peerIdx[lastPeer] = idx
	p.peers[n-1] = ""
	p.peers = p.peers[:n-1]
	delete(p.peerIdx, id)
}
