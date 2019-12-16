package basichost

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"

	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-eventbus"
	inat "github.com/libp2p/go-libp2p-nat"

	logging "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"

	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	manet "github.com/multiformats/go-multiaddr-net"
	msmux "github.com/multiformats/go-multistream"
)

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

	proc goprocess.Process

	mx        sync.Mutex
	lastAddrs []ma.Multiaddr
	emitters  struct {
		evtLocalProtocolsUpdated event.Emitter
	}
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
}

// NewHost constructs a new *BasicHost and activates it by attaching its stream and connection handlers to the given inet.Network.
func NewHost(ctx context.Context, net network.Network, opts *HostOpts) (*BasicHost, error) {
	h := &BasicHost{
		network:      net,
		mux:          msmux.NewMultistreamMuxer(),
		negtimeout:   DefaultNegotiationTimeout,
		AddrsFactory: DefaultAddrsFactory,
		maResolver:   madns.DefaultResolver,
		eventbus:     eventbus.NewBus(),
	}

	var err error
	if h.emitters.evtLocalProtocolsUpdated, err = h.eventbus.Emitter(&event.EvtLocalProtocolsUpdated{}); err != nil {
		return nil, err
	}

	h.proc = goprocessctx.WithContextAndTeardown(ctx, func() error {
		if h.natmgr != nil {
			h.natmgr.Close()
		}
		if h.cmgr != nil {
			h.cmgr.Close()
		}
		_ = h.emitters.evtLocalProtocolsUpdated.Close()
		return h.Network().Close()
	})

	if opts.MultistreamMuxer != nil {
		h.mux = opts.MultistreamMuxer
	}

	// we can't set this as a default above because it depends on the *BasicHost.
	h.ids = identify.NewIDService(
		goprocessctx.WithProcessClosing(ctx, h.proc),
		h,
		identify.UserAgent(opts.UserAgent),
	)

	if uint64(opts.NegotiationTimeout) != 0 {
		h.negtimeout = opts.NegotiationTimeout
	}

	if opts.AddrsFactory != nil {
		h.AddrsFactory = opts.AddrsFactory
	}

	if opts.NATManager != nil {
		h.natmgr = opts.NATManager(net)
	}

	if opts.MultiaddrResolver != nil {
		h.maResolver = opts.MultiaddrResolver
	}

	if opts.ConnManager == nil {
		h.cmgr = &connmgr.NullConnMgr{}
	} else {
		h.cmgr = opts.ConnManager
		net.Notify(h.cmgr.Notifee())
	}

	if opts.EnablePing {
		h.pings = ping.NewPingService(h)
	}

	net.SetConnHandler(h.newConnHandler)
	net.SetStreamHandler(h.newStreamHandler)

	return h, nil
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

	return h
}

// Start starts background tasks in the host
func (h *BasicHost) Start() {
	h.proc.Go(h.background)
}

// newConnHandler is the remote-opened conn handler for inet.Network
func (h *BasicHost) newConnHandler(c network.Conn) {
	// Clear protocols on connecting to new peer to avoid issues caused
	// by misremembering protocols between reconnects
	h.Peerstore().SetProtocols(c.RemotePeer())
	h.ids.IdentifyConn(c)
}

// newStreamHandler is the remote-opened stream handler for network.Network
// TODO: this feels a bit wonky
func (h *BasicHost) newStreamHandler(s network.Stream) {
	before := time.Now()

	if h.negtimeout > 0 {
		if err := s.SetDeadline(time.Now().Add(h.negtimeout)); err != nil {
			log.Error("setting stream deadline: ", err)
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
				logf = log.Warnf
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
			log.Error("resetting stream deadline: ", err)
			s.Reset()
			return
		}
	}

	s.SetProtocol(protocol.ID(protoID))
	log.Debugf("protocol negotiation took %s", took)

	go handle(protoID, s)
}

// PushIdentify pushes an identify update through the identify push protocol
// Warning: this interface is unstable and may disappear in the future.
func (h *BasicHost) PushIdentify() {
	push := false

	h.mx.Lock()
	addrs := h.Addrs()
	if !sameAddrs(addrs, h.lastAddrs) {
		push = true
		h.lastAddrs = addrs
	}
	h.mx.Unlock()

	if push {
		h.ids.Push()
	}
}

func (h *BasicHost) background(p goprocess.Process) {
	// periodically schedules an IdentifyPush to update our peers for changes
	// in our address set (if needed)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// initialize lastAddrs
	h.mx.Lock()
	if h.lastAddrs == nil {
		h.lastAddrs = h.Addrs()
	}
	h.mx.Unlock()

	for {
		select {
		case <-ticker.C:
			h.PushIdentify()

		case <-p.Closing():
			return
		}
	}
}

func sameAddrs(a, b []ma.Multiaddr) bool {
	if len(a) != len(b) {
		return false
	}

	bmap := make(map[string]struct{}, len(b))
	for _, addr := range b {
		bmap[string(addr.Bytes())] = struct{}{}
	}

	for _, addr := range a {
		_, ok := bmap[string(addr.Bytes())]
		if !ok {
			return false
		}
	}

	return true
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
	pref, err := h.preferredProtocol(p, pids)
	if err != nil {
		return nil, err
	}

	if pref != "" {
		return h.newStream(ctx, p, pref)
	}

	var protoStrs []string
	for _, pid := range pids {
		protoStrs = append(protoStrs, string(pid))
	}

	s, err := h.Network().NewStream(ctx, p)
	if err != nil {
		return nil, err
	}

	selected, err := msmux.SelectOneOf(protoStrs, s)
	if err != nil {
		s.Reset()
		return nil, err
	}
	selpid := protocol.ID(selected)
	s.SetProtocol(selpid)
	h.Peerstore().AddProtocols(p, selected)

	return s, nil
}

func pidsToStrings(pids []protocol.ID) []string {
	out := make([]string, len(pids))
	for i, p := range pids {
		out[i] = string(p)
	}
	return out
}

func (h *BasicHost) preferredProtocol(p peer.ID, pids []protocol.ID) (protocol.ID, error) {
	pidstrs := pidsToStrings(pids)
	supported, err := h.Peerstore().SupportsProtocols(p, pidstrs...)
	if err != nil {
		return "", err
	}

	var out protocol.ID
	if len(supported) > 0 {
		out = protocol.ID(supported[0])
	}
	return out, nil
}

func (h *BasicHost) newStream(ctx context.Context, p peer.ID, pid protocol.ID) (network.Stream, error) {
	s, err := h.Network().NewStream(ctx, p)
	if err != nil {
		return nil, err
	}

	s.SetProtocol(pid)

	lzcon := msmux.NewMSSelect(s, string(pid))
	return &streamWrapper{
		Stream: s,
		rw:     lzcon,
	}, nil
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

	var addrs []ma.Multiaddr
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr)
		if !madns.Matches(addr) {
			continue
		}

		reqaddr := addr.Encapsulate(p2paddr)
		resaddrs, err := h.maResolver.Resolve(ctx, reqaddr)
		if err != nil {
			log.Infof("error resolving %s: %s", reqaddr, err)
		}
		for _, res := range resaddrs {
			pi, err := peer.AddrInfoFromP2pAddr(res)
			if err != nil {
				log.Infof("error parsing %s: %s", res, err)
			}
			addrs = append(addrs, pi.Addrs...)
		}
	}

	return addrs, nil
}

// dialPeer opens a connection to peer, and makes sure to identify
// the connection once it has been opened.
func (h *BasicHost) dialPeer(ctx context.Context, p peer.ID) error {
	log.Debugf("host %s dialing %s", h.ID(), p)
	c, err := h.Network().DialPeer(ctx, p)
	if err != nil {
		return err
	}

	// Clear protocols on connecting to new peer to avoid issues caused
	// by misremembering protocols between reconnects
	h.Peerstore().SetProtocols(p)

	// identify the connection before returning.
	done := make(chan struct{})
	go func() {
		h.ids.IdentifyConn(c)
		close(done)
	}()

	// respect don contexteone
	select {
	case <-done:
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
	listenAddrs, err := h.Network().InterfaceListenAddresses()
	if err != nil {
		log.Debug("error retrieving network interface addrs")
	}
	var natMappings []inat.Mapping

	// natmgr is nil if we do not use nat option;
	// h.natmgr.NAT() is nil if not ready, or no nat is available.
	if h.natmgr != nil && h.natmgr.NAT() != nil {
		natMappings = h.natmgr.NAT().Mappings()
	}

	finalAddrs := listenAddrs
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

			// Did the router give us a routable public addr?
			if manet.IsPublicAddr(mappedMaddr) {
				// Yes, use it.
				extMaddr := mappedMaddr
				if rest != nil {
					extMaddr = ma.Join(extMaddr, rest)
				}

				// Add in the mapped addr.
				finalAddrs = append(finalAddrs, extMaddr)
				continue
			}

			// No. Ok, let's try our observed addresses.

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
	// You're thinking of adding some teardown logic here, right? Well
	// don't! Add any process teardown logic to the teardown function in the
	// constructor.
	//
	// This:
	// 1. May be called multiple times.
	// 2. May _never_ be called if the host is stopped by the context.
	return h.proc.Close()
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
