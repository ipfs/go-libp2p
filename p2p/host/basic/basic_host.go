package basichost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/introspection"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/record"

	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"

	addrutil "github.com/libp2p/go-addr-util"
	"github.com/libp2p/go-eventbus"
	inat "github.com/libp2p/go-libp2p-nat"
	"github.com/libp2p/go-netroute"

	logging "github.com/ipfs/go-log"

	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	manet "github.com/multiformats/go-multiaddr-net"
	msmux "github.com/multiformats/go-multistream"
)

// The maximum number of address resolution steps we'll perform for a single
// peer (for all addresses).
const maxAddressResolution = 32

var _ host.IntrospectableHost = (*BasicHost)(nil)

// addrChangeTickrInterval is the interval between two address change ticks.
var addrChangeTickrInterval = 5 * time.Second

var log = logging.Logger("basichost")

var (
	// DefaultNegotiationTimeout is the default value for HostOpts.NegotiationTimeout.
	DefaultNegotiationTimeout = time.Second * 60

	// DefaultAddrsFactory is the default value for HostOpts.AddrsFactory.
	DefaultAddrsFactory = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }
)

// AddrsFactory functions can be passed to New in order to override
// addresses returned by Addrs.
type AddrsFactory func([]ma.Multiaddr) []ma.Multiaddr

// Option is a type used to pass in options to the host.
//
// Deprecated in favor of HostOpts and NewHost.
type Option int

// NATPortMap makes the host attempt to open port-mapping in NAT devices
// for all its listeners. Pass in this option in the constructor to
// asynchronously a) find a gateway, b) open port mappings, c) republish
// port mappings periodically. The NATed addresses are included in the
// Host's Addrs() list.
//
// This option is deprecated in favor of HostOpts and NewHost.
const NATPortMap Option = iota

// BasicHost is the basic implementation of the host.Host interface. This
// particular host implementation:
//  * uses a protocol muxer to mux per-protocol streams
//  * uses an identity service to send + receive node information
//  * uses a nat service to establish NAT port mappings
type BasicHost struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	// ensures we shutdown ONLY once
	closeSync sync.Once
	// keep track of resources we need to wait on before shutting down
	refCount sync.WaitGroup

	network    network.Network
	mux        *msmux.MultistreamMuxer
	ids        *identify.IDService
	pings      *ping.PingService
	natmgr     NATManager
	maResolver *madns.Resolver
	cmgr       connmgr.ConnManager
	eventbus   event.Bus

	AddrsFactory AddrsFactory

	negtimeout time.Duration

	emitters struct {
		evtLocalProtocolsUpdated event.Emitter
		evtLocalAddrsUpdated     event.Emitter
	}

	addrChangeChan chan struct{}

	introspector          introspection.Introspector
	introspectionEndpoint introspection.Endpoint

	lipMu         sync.RWMutex
	localIPv4Addr ma.Multiaddr
	localIPv6Addr ma.Multiaddr

	disableSignedPeerRecord bool
	signKey                 crypto.PrivKey
	caBook                  peerstore.CertifiedAddrBook
}

var _ host.Host = (*BasicHost)(nil)

// HostOpts holds options that can be passed to NewHost in order to
// customize construction of the *BasicHost.
type HostOpts struct {
	// MultistreamMuxer is essential for the *BasicHost and will use a sensible default value if omitted.
	MultistreamMuxer *msmux.MultistreamMuxer

	// NegotiationTimeout determines the read and write timeouts on streams.
	// If 0 or omitted, it will use DefaultNegotiationTimeout.
	// If below 0, timeouts on streams will be deactivated.
	NegotiationTimeout time.Duration

	// AddrsFactory holds a function which can be used to override or filter the result of Addrs.
	// If omitted, there's no override or filtering, and the results of Addrs and AllAddrs are the same.
	AddrsFactory AddrsFactory

	// MultiaddrResolves holds the go-multiaddr-dns.Resolver used for resolving
	// /dns4, /dns6, and /dnsaddr addresses before trying to connect to a peer.
	MultiaddrResolver *madns.Resolver

	// NATManager takes care of setting NAT port mappings, and discovering external addresses.
	// If omitted, this will simply be disabled.
	NATManager func(network.Network) NATManager

	// ConnManager is a libp2p connection manager
	ConnManager connmgr.ConnManager

	// EnablePing indicates whether to instantiate the ping service
	EnablePing bool

	// UserAgent sets the user-agent for the host. Defaults to ClientVersion.
	UserAgent string

	// Introspector is used by host subsystems to register themselves as metrics
	// providers and fetch the current state.
	Introspector introspection.Introspector

	// IntrospectionEndpoint is the introspect endpoint through which
	// introspect data is served to clients.
	IntrospectionEndpoint introspection.Endpoint

	// DisableSignedPeerRecord disables the generation of Signed Peer Records on this host.
	DisableSignedPeerRecord bool
}

// NewHost constructs a new *BasicHost and activates it by attaching its stream and connection handlers to the given inet.Network.
func NewHost(ctx context.Context, n network.Network, opts *HostOpts) (*BasicHost, error) {
	hostCtx, cancel := context.WithCancel(ctx)

	h := &BasicHost{
		network:                 n,
		mux:                     msmux.NewMultistreamMuxer(),
		negtimeout:              DefaultNegotiationTimeout,
		AddrsFactory:            DefaultAddrsFactory,
		maResolver:              madns.DefaultResolver,
		eventbus:                eventbus.NewBus(),
		introspector:            opts.Introspector,
		addrChangeChan:          make(chan struct{}, 1),
		ctx:                     hostCtx,
		ctxCancel:               cancel,
		disableSignedPeerRecord: opts.DisableSignedPeerRecord,
	}

	h.updateLocalIpAddr()

	var err error
	if h.emitters.evtLocalProtocolsUpdated, err = h.eventbus.Emitter(&event.EvtLocalProtocolsUpdated{}); err != nil {
		return nil, err
	}
	if h.emitters.evtLocalAddrsUpdated, err = h.eventbus.Emitter(&event.EvtLocalAddressesUpdated{}); err != nil {
		return nil, err
	}

	if !h.disableSignedPeerRecord {
		cab, ok := peerstore.GetCertifiedAddrBook(n.Peerstore())
		if !ok {
			return nil, errors.New("peerstore should also be a certified address book")
		}
		h.caBook = cab

		h.signKey = h.Peerstore().PrivKey(h.ID())
		if h.signKey == nil {
			return nil, errors.New("unable to access host key")
		}

		// persist a signed peer record for self to the peerstore.
		rec := peer.PeerRecordFromAddrInfo(peer.AddrInfo{h.ID(), h.Addrs()})
		ev, err := record.Seal(rec, h.signKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create signed record for self: %w", err)
		}
		if _, err := cab.ConsumePeerRecord(ev, peerstore.PermanentAddrTTL); err != nil {
			return nil, fmt.Errorf("failed to persist signed record to peerstore: %w", err)
		}
	}

	if opts.MultistreamMuxer != nil {
		h.mux = opts.MultistreamMuxer
	}

	// Start ID Service
	// we can't set this as a default above because it depends on the *BasicHost.
	if h.disableSignedPeerRecord {
		h.ids = identify.NewIDService(h, identify.UserAgent(opts.UserAgent), identify.DisableSignedPeerRecord())
	} else {
		h.ids = identify.NewIDService(h, identify.UserAgent(opts.UserAgent))
	}

	if uint64(opts.NegotiationTimeout) != 0 {
		h.negtimeout = opts.NegotiationTimeout
	}

	if opts.AddrsFactory != nil {
		h.AddrsFactory = opts.AddrsFactory
	}

	if opts.NATManager != nil {
		h.natmgr = opts.NATManager(n)
	}

	if opts.MultiaddrResolver != nil {
		h.maResolver = opts.MultiaddrResolver
	}

	if opts.ConnManager == nil {
		h.cmgr = &connmgr.NullConnMgr{}
	} else {
		h.cmgr = opts.ConnManager
		n.Notify(h.cmgr.Notifee())
	}

	if opts.EnablePing {
		h.pings = ping.NewPingService(h)
	}

	n.SetStreamHandler(h.newStreamHandler)

	// register to be notified when the network's listen addrs change,
	// so we can update our address set and push events if needed
	listenHandler := func(network.Network, ma.Multiaddr) {
		h.SignalAddressChange()
	}
	n.Notify(&network.NotifyBundle{
		ListenF:      listenHandler,
		ListenCloseF: listenHandler,
	})

	return h, nil
}

func (h *BasicHost) SetIntrospection(introspector introspection.Introspector, endpoint introspection.Endpoint) error {
	if h.introspectionEndpoint != nil {
		if err := h.introspectionEndpoint.Close(); err != nil {
			return fmt.Errorf("failed to close existing introspection endpoint: %w", err)
		}
	}
	if h.introspector != nil {
		if err := h.introspector.Close(); err != nil {
			return fmt.Errorf("failed to close existing introspector: %w", err)
		}
	}
	h.introspector = introspector
	h.introspectionEndpoint = endpoint
	if h.introspectionEndpoint == nil {
		return nil
	}
	return h.introspectionEndpoint.Start()
}

func (h *BasicHost) Introspector() introspection.Introspector {
	return h.introspector
}

func (h *BasicHost) IntrospectionEndpoint() introspection.Endpoint {
	return h.introspectionEndpoint
}

func (h *BasicHost) updateLocalIpAddr() {
	h.lipMu.Lock()
	defer h.lipMu.Unlock()

	if r, err := netroute.New(); err != nil {
		log.Debugw("failed to build Router for kernel's routing table", "err", err)
	} else {
		if _, _, localIPv4, err := r.Route(net.IPv4zero); err != nil {
			log.Debugw("failed to fetch local IPv4 address", "err", err)
		} else {
			maddr, err := manet.FromIP(localIPv4)
			if err == nil {
				h.localIPv4Addr = maddr
			}
		}

		if _, _, localIpv6, err := r.Route(net.IPv6unspecified); err != nil {
			log.Debugw("failed to fetch local IPv6 address", "err", err)
		} else {
			maddr, err := manet.FromIP(localIpv6)
			if err == nil {
				h.localIPv6Addr = maddr
			}
		}
	}
}

// New constructs and sets up a new *BasicHost with given Network and options.
// The following options can be passed:
// * NATPortMap
// * AddrsFactory
// * connmgr.ConnManager
// * madns.Resolver
//
// This function is deprecated in favor of NewHost and HostOpts.
func New(net network.Network, opts ...interface{}) *BasicHost {
	hostopts := &HostOpts{}

	for _, o := range opts {
		switch o := o.(type) {
		case Option:
			switch o {
			case NATPortMap:
				hostopts.NATManager = NewNATManager
			}
		case AddrsFactory:
			hostopts.AddrsFactory = o
		case connmgr.ConnManager:
			hostopts.ConnManager = o
		case *madns.Resolver:
			hostopts.MultiaddrResolver = o
		}
	}

	h, err := NewHost(context.Background(), net, hostopts)
	if err != nil {
		// this cannot happen with legacy options
		// plus we want to keep the (deprecated) legacy interface unchanged
		panic(err)
	}
	h.Start()

	return h
}

// Start starts background tasks in the host
func (h *BasicHost) Start() {
	h.refCount.Add(1)
	go h.background()
}

// newStreamHandler is the remote-opened stream handler for network.Network
// TODO: this feels a bit wonky
func (h *BasicHost) newStreamHandler(s network.Stream) {
	before := time.Now()

	if h.negtimeout > 0 {
		if err := s.SetDeadline(time.Now().Add(h.negtimeout)); err != nil {
			log.Debug("setting stream deadline: ", err)
			s.Reset()
			return
		}
	}

	lzc, protoID, handle, err := h.Mux().NegotiateLazy(s)
	took := time.Since(before)
	if err != nil {
		if err == io.EOF {
			logf := log.Debugf
			if took > time.Second*10 {
				logf = log.Warningf
			}
			logf("protocol EOF: %s (took %s)", s.Conn().RemotePeer(), took)
		} else {
			log.Debugf("protocol mux failed: %s (took %s)", err, took)
		}
		s.Reset()
		return
	}

	s = &streamWrapper{
		Stream: s,
		rw:     lzc,
	}

	if h.negtimeout > 0 {
		if err := s.SetDeadline(time.Time{}); err != nil {
			log.Debugf("resetting stream deadline: ", err)
			s.Reset()
			return
		}
	}

	s.SetProtocol(protocol.ID(protoID))
	log.Debugf("protocol negotiation took %s", took)

	go handle(protoID, s)
}

// SignalAddressChange signals to the host that it needs to determine whether our listen addresses have recently
// changed.
// Warning: this interface is unstable and may disappear in the future.
func (h *BasicHost) SignalAddressChange() {
	select {
	case h.addrChangeChan <- struct{}{}:
	default:
	}
}

func makeUpdatedAddrEvent(prev, current []ma.Multiaddr) *event.EvtLocalAddressesUpdated {
	prevmap := make(map[string]ma.Multiaddr, len(prev))
	evt := event.EvtLocalAddressesUpdated{Diffs: true}
	addrsAdded := false

	for _, addr := range prev {
		prevmap[string(addr.Bytes())] = addr
	}
	for _, addr := range current {
		_, ok := prevmap[string(addr.Bytes())]
		updated := event.UpdatedAddress{Address: addr}
		if ok {
			updated.Action = event.Maintained
		} else {
			updated.Action = event.Added
			addrsAdded = true
		}
		evt.Current = append(evt.Current, updated)
		delete(prevmap, string(addr.Bytes()))
	}
	for _, addr := range prevmap {
		updated := event.UpdatedAddress{Action: event.Removed, Address: addr}
		evt.Removed = append(evt.Removed, updated)
	}

	if !addrsAdded && len(evt.Removed) == 0 {
		return nil
	}

	return &evt
}

func (h *BasicHost) makeSignedPeerRecord(evt *event.EvtLocalAddressesUpdated) (*record.Envelope, error) {
	current := make([]multiaddr.Multiaddr, 0, len(evt.Current))
	for _, a := range evt.Current {
		current = append(current, a.Address)
	}

	rec := peer.PeerRecordFromAddrInfo(peer.AddrInfo{h.ID(), current})
	return record.Seal(rec, h.signKey)
}

func (h *BasicHost) background() {
	defer h.refCount.Done()
	var lastAddrs []ma.Multiaddr

	emitAddrChange := func(currentAddrs []ma.Multiaddr, lastAddrs []ma.Multiaddr) {
		// nothing to do if both are nil..defensive check
		if currentAddrs == nil && lastAddrs == nil {
			return
		}

		changeEvt := makeUpdatedAddrEvent(lastAddrs, currentAddrs)

		if changeEvt == nil {
			return
		}

		if !h.disableSignedPeerRecord {
			// add signed peer record to the event
			sr, err := h.makeSignedPeerRecord(changeEvt)
			if err != nil {
				log.Errorf("error creating a signed peer record from the set of current addresses, err=%s", err)
				return
			}
			changeEvt.SignedPeerRecord = sr

			// persist the signed record to the peerstore
			if _, err := h.caBook.ConsumePeerRecord(sr, peerstore.PermanentAddrTTL); err != nil {
				log.Errorf("failed to persist signed peer record in peer store, err=%s", err)
				return
			}
		}

		// emit addr change event on the bus
		if err := h.emitters.evtLocalAddrsUpdated.Emit(*changeEvt); err != nil {
			log.Warnf("error emitting event for updated addrs: %s", err)
		}
	}

	// periodically schedules an IdentifyPush to update our peers for changes
	// in our address set (if needed)
	ticker := time.NewTicker(addrChangeTickrInterval)
	defer ticker.Stop()

	for {
		h.updateLocalIpAddr()
		curr := h.Addrs()
		emitAddrChange(curr, lastAddrs)
		lastAddrs = curr

		select {
		case <-ticker.C:
		case <-h.addrChangeChan:
		case <-h.ctx.Done():
			return
		}
	}
}

// ID returns the (local) peer.ID associated with this Host
func (h *BasicHost) ID() peer.ID {
	return h.Network().LocalPeer()
}

// Peerstore returns the Host's repository of Peer Addresses and Keys.
func (h *BasicHost) Peerstore() peerstore.Peerstore {
	return h.Network().Peerstore()
}

// Network returns the Network interface of the Host
func (h *BasicHost) Network() network.Network {
	return h.network
}

// Mux returns the Mux multiplexing incoming streams to protocol handlers
func (h *BasicHost) Mux() protocol.Switch {
	return h.mux
}

// IDService returns
func (h *BasicHost) IDService() *identify.IDService {
	return h.ids
}

func (h *BasicHost) EventBus() event.Bus {
	return h.eventbus
}

// SetStreamHandler sets the protocol handler on the Host's Mux.
// This is equivalent to:
//   host.Mux().SetHandler(proto, handler)
// (Threadsafe)
func (h *BasicHost) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	h.Mux().AddHandler(string(pid), func(p string, rwc io.ReadWriteCloser) error {
		is := rwc.(network.Stream)
		is.SetProtocol(protocol.ID(p))
		handler(is)
		return nil
	})
	h.emitters.evtLocalProtocolsUpdated.Emit(event.EvtLocalProtocolsUpdated{
		Added: []protocol.ID{pid},
	})
}

// SetStreamHandlerMatch sets the protocol handler on the Host's Mux
// using a matching function to do protocol comparisons
func (h *BasicHost) SetStreamHandlerMatch(pid protocol.ID, m func(string) bool, handler network.StreamHandler) {
	h.Mux().AddHandlerWithFunc(string(pid), m, func(p string, rwc io.ReadWriteCloser) error {
		is := rwc.(network.Stream)
		is.SetProtocol(protocol.ID(p))
		handler(is)
		return nil
	})
	h.emitters.evtLocalProtocolsUpdated.Emit(event.EvtLocalProtocolsUpdated{
		Added: []protocol.ID{pid},
	})
}

// RemoveStreamHandler returns ..
func (h *BasicHost) RemoveStreamHandler(pid protocol.ID) {
	h.Mux().RemoveHandler(string(pid))
	h.emitters.evtLocalProtocolsUpdated.Emit(event.EvtLocalProtocolsUpdated{
		Removed: []protocol.ID{pid},
	})
}

// NewStream opens a new stream to given peer p, and writes a p2p/protocol
// header with given protocol.ID. If there is no connection to p, attempts
// to create one. If ProtocolID is "", writes no header.
// (Threadsafe)
func (h *BasicHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	s, err := h.Network().NewStream(ctx, p)
	if err != nil {
		return nil, err
	}

	// Wait for any in-progress identifies on the connection to finish. This
	// is faster than negotiating.
	//
	// If the other side doesn't support identify, that's fine. This will
	// just be a no-op.
	select {
	case <-h.ids.IdentifyWait(s.Conn()):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	pidStrings := protocol.ConvertToStrings(pids)

	pref, err := h.preferredProtocol(p, pidStrings)
	if err != nil {
		_ = s.Reset()
		return nil, err
	}

	if pref != "" {
		s.SetProtocol(pref)
		lzcon := msmux.NewMSSelect(s, string(pref))
		return &streamWrapper{
			Stream: s,
			rw:     lzcon,
		}, nil
	}

	selected, err := msmux.SelectOneOf(pidStrings, s)
	if err != nil {
		s.Reset()
		return nil, err
	}

	selpid := protocol.ID(selected)
	s.SetProtocol(selpid)
	h.Peerstore().AddProtocols(p, selected)
	return s, nil
}

func (h *BasicHost) preferredProtocol(p peer.ID, pids []string) (protocol.ID, error) {
	supported, err := h.Peerstore().SupportsProtocols(p, pids...)
	if err != nil {
		return "", err
	}

	var out protocol.ID
	if len(supported) > 0 {
		out = protocol.ID(supported[0])
	}
	return out, nil
}

// Connect ensures there is a connection between this host and the peer with
// given peer.ID. If there is not an active connection, Connect will issue a
// h.Network.Dial, and block until a connection is open, or an error is returned.
// Connect will absorb the addresses in pi into its internal peerstore.
// It will also resolve any /dns4, /dns6, and /dnsaddr addresses.
func (h *BasicHost) Connect(ctx context.Context, pi peer.AddrInfo) error {
	// absorb addresses into peerstore
	h.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.TempAddrTTL)

	if h.Network().Connectedness(pi.ID) == network.Connected {
		return nil
	}

	resolved, err := h.resolveAddrs(ctx, h.Peerstore().PeerInfo(pi.ID))
	if err != nil {
		return err
	}
	h.Peerstore().AddAddrs(pi.ID, resolved, peerstore.TempAddrTTL)

	return h.dialPeer(ctx, pi.ID)
}

func (h *BasicHost) resolveAddrs(ctx context.Context, pi peer.AddrInfo) ([]ma.Multiaddr, error) {
	proto := ma.ProtocolWithCode(ma.P_P2P).Name
	p2paddr, err := ma.NewMultiaddr("/" + proto + "/" + pi.ID.Pretty())
	if err != nil {
		return nil, err
	}

	resolveSteps := 0

	// Recursively resolve all addrs.
	//
	// While the toResolve list is non-empty:
	// * Pop an address off.
	// * If the address is fully resolved, add it to the resolved list.
	// * Otherwise, resolve it and add the results to the "to resolve" list.
	toResolve := append(([]ma.Multiaddr)(nil), pi.Addrs...)
	resolved := make([]ma.Multiaddr, 0, len(pi.Addrs))
	for len(toResolve) > 0 {
		// pop the last addr off.
		addr := toResolve[len(toResolve)-1]
		toResolve = toResolve[:len(toResolve)-1]

		// if it's resolved, add it to the resolved list.
		if !madns.Matches(addr) {
			resolved = append(resolved, addr)
			continue
		}

		resolveSteps++

		// We've resolved too many addresses. We can keep all the fully
		// resolved addresses but we'll need to skip the rest.
		if resolveSteps >= maxAddressResolution {
			log.Warningf(
				"peer %s asked us to resolve too many addresses: %s/%s",
				pi.ID,
				resolveSteps,
				maxAddressResolution,
			)
			continue
		}

		// otherwise, resolve it
		reqaddr := addr.Encapsulate(p2paddr)
		resaddrs, err := h.maResolver.Resolve(ctx, reqaddr)
		if err != nil {
			log.Infof("error resolving %s: %s", reqaddr, err)
		}

		// add the results to the toResolve list.
		for _, res := range resaddrs {
			pi, err := peer.AddrInfoFromP2pAddr(res)
			if err != nil {
				log.Infof("error parsing %s: %s", res, err)
			}
			toResolve = append(toResolve, pi.Addrs...)
		}
	}

	return resolved, nil
}

// dialPeer opens a connection to peer, and makes sure to identify
// the connection once it has been opened.
func (h *BasicHost) dialPeer(ctx context.Context, p peer.ID) error {
	log.Debugf("host %s dialing %s", h.ID(), p)
	c, err := h.Network().DialPeer(ctx, p)
	if err != nil {
		return err
	}

	// TODO: Consider removing this? On one hand, it's nice because we can
	// assume that things like the agent version are usually set when this
	// returns. On the other hand, we don't _really_ need to wait for this.
	//
	// This is mostly here to preserve existing behavior.
	select {
	case <-h.ids.IdentifyWait(c):
	case <-ctx.Done():
		return ctx.Err()
	}

	log.Debugf("host %s finished dialing %s", h.ID(), p)
	return nil
}

func (h *BasicHost) ConnManager() connmgr.ConnManager {
	return h.cmgr
}

// Addrs returns listening addresses that are safe to announce to the network.
// The output is the same as AllAddrs, but processed by AddrsFactory.
func (h *BasicHost) Addrs() []ma.Multiaddr {
	return h.AddrsFactory(h.AllAddrs())
}

// mergeAddrs merges input address lists, leave only unique addresses
func dedupAddrs(addrs []ma.Multiaddr) (uniqueAddrs []ma.Multiaddr) {
	exists := make(map[string]bool)
	for _, addr := range addrs {
		k := string(addr.Bytes())
		if exists[k] {
			continue
		}
		exists[k] = true
		uniqueAddrs = append(uniqueAddrs, addr)
	}
	return uniqueAddrs
}

// AllAddrs returns all the addresses of BasicHost at this moment in time.
// It's ok to not include addresses if they're not available to be used now.
func (h *BasicHost) AllAddrs() []ma.Multiaddr {
	h.lipMu.RLock()
	localIPv4Addr := h.localIPv4Addr
	localIPv6Addr := h.localIPv6Addr
	h.lipMu.RUnlock()

	var finalAddrs []ma.Multiaddr
	listenAddrs := h.Network().ListenAddresses()
	for _, addr := range listenAddrs {
		if !manet.IsIPUnspecified(addr) {
			finalAddrs = append(finalAddrs, addr)
			continue
		}

		ifaceAddrs := []ma.Multiaddr{manet.IP4Loopback, manet.IP6Loopback}
		if localIPv4Addr != nil {
			ifaceAddrs = append(ifaceAddrs, localIPv4Addr)
		}
		if localIPv6Addr != nil {
			ifaceAddrs = append(ifaceAddrs, localIPv6Addr)
		}

		resolved, err := addrutil.ResolveUnspecifiedAddress(addr, ifaceAddrs)
		if err == nil {
			for _, r := range resolved {
				finalAddrs = append(finalAddrs, r)
			}
		}
	}

	finalAddrs = dedupAddrs(finalAddrs)

	var natMappings []inat.Mapping

	// natmgr is nil if we do not use nat option;
	// h.natmgr.NAT() is nil if not ready, or no nat is available.
	if h.natmgr != nil && h.natmgr.NAT() != nil {
		natMappings = h.natmgr.NAT().Mappings()
	}

	if len(natMappings) > 0 {
		// We have successfully mapped ports on our NAT. Use those
		// instead of observed addresses (mostly).

		// First, generate a mapping table.
		// protocol -> internal port -> external addr
		ports := make(map[string]map[int]net.Addr)
		for _, m := range natMappings {
			addr, err := m.ExternalAddr()
			if err != nil {
				// mapping not ready yet.
				continue
			}
			protoPorts, ok := ports[m.Protocol()]
			if !ok {
				protoPorts = make(map[int]net.Addr)
				ports[m.Protocol()] = protoPorts
			}
			protoPorts[m.InternalPort()] = addr
		}

		// Next, apply this mapping to our addresses.
		for _, listen := range listenAddrs {
			found := false
			transport, rest := ma.SplitFunc(listen, func(c ma.Component) bool {
				if found {
					return true
				}
				switch c.Protocol().Code {
				case ma.P_TCP, ma.P_UDP:
					found = true
				}
				return false
			})
			if !manet.IsThinWaist(transport) {
				continue
			}

			naddr, err := manet.ToNetAddr(transport)
			if err != nil {
				log.Error("error parsing net multiaddr %q: %s", transport, err)
				continue
			}

			var (
				ip       net.IP
				iport    int
				protocol string
			)
			switch naddr := naddr.(type) {
			case *net.TCPAddr:
				ip = naddr.IP
				iport = naddr.Port
				protocol = "tcp"
			case *net.UDPAddr:
				ip = naddr.IP
				iport = naddr.Port
				protocol = "udp"
			default:
				continue
			}

			if !ip.IsGlobalUnicast() {
				// We only map global unicast ports.
				continue
			}

			mappedAddr, ok := ports[protocol][iport]
			if !ok {
				// Not mapped.
				continue
			}

			mappedMaddr, err := manet.FromNetAddr(mappedAddr)
			if err != nil {
				log.Errorf("mapped addr can't be turned into a multiaddr %q: %s", mappedAddr, err)
				continue
			}

			extMaddr := mappedMaddr
			if rest != nil {
				extMaddr = ma.Join(extMaddr, rest)
			}

			// Add in the mapped addr.
			finalAddrs = append(finalAddrs, extMaddr)

			// Did the router give us a routable public addr?
			if manet.IsPublicAddr(mappedMaddr) {
				//well done
				continue
			}

			// No.
			// in case router give us a wrong address.
			// also add observed addresses

			// Now, check if we have any observed addresses that
			// differ from the one reported by the router. Routers
			// don't always give the most accurate information.
			observed := h.ids.ObservedAddrsFor(listen)

			if len(observed) == 0 {
				continue
			}

			// Drop the IP from the external maddr
			_, extMaddrNoIP := ma.SplitFirst(mappedMaddr)

			for _, obsMaddr := range observed {
				// Extract a public observed addr.
				ip, _ := ma.SplitFirst(obsMaddr)
				if ip == nil || !manet.IsPublicAddr(ip) {
					continue
				}

				finalAddrs = append(finalAddrs, ma.Join(ip, extMaddrNoIP))
			}
		}
	} else {
		var observedAddrs []ma.Multiaddr
		if h.ids != nil {
			observedAddrs = h.ids.OwnObservedAddrs()
		}
		finalAddrs = append(finalAddrs, observedAddrs...)
	}

	return dedupAddrs(finalAddrs)
}

// Close shuts down the Host's services (network, etc).
func (h *BasicHost) Close() error {
	h.closeSync.Do(func() {
		h.ctxCancel()
		if h.natmgr != nil {
			h.natmgr.Close()
		}
		if h.cmgr != nil {
			h.cmgr.Close()
		}
		if h.ids != nil {
			h.ids.Close()
		}

		_ = h.emitters.evtLocalProtocolsUpdated.Close()
		_ = h.emitters.evtLocalAddrsUpdated.Close()
		h.Network().Close()

		if h.introspectionEndpoint != nil {
			if err := h.introspectionEndpoint.Close(); err != nil {
				log.Errorf("failed while shutting down introspection endpoint; err: %s", err)
			}
		}

		if h.introspector != nil {
			if err := h.introspector.Close(); err != nil {
				log.Errorf("failed while shutting down introspectir; err: %s", err)
			}
		}

		h.refCount.Wait()
	})

	return nil
}

type streamWrapper struct {
	network.Stream
	rw io.ReadWriter
}

func (s *streamWrapper) Read(b []byte) (int, error) {
	return s.rw.Read(b)
}

func (s *streamWrapper) Write(b []byte) (int, error) {
	return s.rw.Write(b)
}
