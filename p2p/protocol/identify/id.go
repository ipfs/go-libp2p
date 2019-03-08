package identify

import (
	"context"
	"sync"
	"time"

	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
	ic "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	lgbl "github.com/libp2p/go-libp2p-loggables"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("net/identify")

// ID is the protocol.ID of the Identify Service.
const ID = "/ipfs/id/1.0.0"

// IDPush is the protocol.ID of the Identify push protocol
const IDPush = "/ipfs/id/push/1.0.0"

// LibP2PVersion holds the current protocol version for a client running this code
const LibP2PVersion = "ipfs/0.1.0"

var ClientVersion = "go-libp2p/3.3.4"

// IDService is a structure that implements ProtocolIdentify.
// It is a trivial service that gives the other peer some
// useful information about the local peer. A sort of hello.
//
// The IDService sends:
//  * Our IPFS Protocol Version
//  * Our IPFS Agent Version
//  * Our public Listen Addresses
type IDService struct {
	Host host.Host

	// connections undergoing identification; for dedup purposes
	currid map[inet.Conn]chan struct{}
	currmu sync.RWMutex

	addrMu sync.Mutex

	// our own observed addresses.
	// TODO: instead of expiring, remove these when we disconnect
	observedAddrs ObservedAddrSet
}

// NewIDService constructs a new *IDService and activates it by
// attaching its stream handler to the given host.Host.
func NewIDService(h host.Host) *IDService {
	s := &IDService{
		Host:   h,
		currid: make(map[inet.Conn]chan struct{}),
	}
	h.SetStreamHandler(ID, s.requestHandler)
	h.SetStreamHandler(IDPush, s.pushHandler)
	h.Network().Notify((*netNotifiee)(s))
	return s
}

// OwnObservedAddrs returns the addresses peers have reported we've dialed from
func (ids *IDService) OwnObservedAddrs() []ma.Multiaddr {
	return ids.observedAddrs.Addrs()
}

func (ids *IDService) IdentifyConn(c inet.Conn) {
	ids.currmu.Lock()
	if wait, found := ids.currid[c]; found {
		ids.currmu.Unlock()
		log.Debugf("IdentifyConn called twice on: %s", c)
		<-wait // already identifying it. wait for it.
		return
	}
	ch := make(chan struct{})
	ids.currid[c] = ch
	ids.currmu.Unlock()

	defer func() {
		close(ch)
		ids.currmu.Lock()
		delete(ids.currid, c)
		ids.currmu.Unlock()
	}()

	// this is racy but there's no other way to open a stream if and only if we are connected.
	if ids.Host.Network().Connectedness(c.RemotePeer()) != inet.Connected {
		return
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()

	s, err := ids.Host.NewStream(ctx, c.RemotePeer(), ID)
	if err != nil {
		log.Debugf("error opening initial stream for %s: %s", ID, err)
		log.Event(context.TODO(), "IdentifyOpenFailed", c.RemotePeer())
		c.Close()
		return
	}

	ids.responseHandler(s)
}

func (ids *IDService) requestHandler(s inet.Stream) {
	defer inet.FullClose(s)
	c := s.Conn()

	w := ggio.NewDelimitedWriter(s)
	mes := pb.Identify{}
	ids.populateMessage(&mes, s.Conn())
	w.WriteMsg(&mes)

	log.Debugf("%s sent message to %s %s", ID, c.RemotePeer(), c.RemoteMultiaddr())
}

func (ids *IDService) responseHandler(s inet.Stream) {
	c := s.Conn()

	r := ggio.NewDelimitedReader(s, 2048)
	mes := pb.Identify{}
	if err := r.ReadMsg(&mes); err != nil {
		log.Warning("error reading identify message: ", err)
		s.Reset()
		return
	}
	ids.consumeMessage(&mes, c)
	log.Debugf("%s received message from %s %s", ID, c.RemotePeer(), c.RemoteMultiaddr())

	go inet.FullClose(s)
}

func (ids *IDService) pushHandler(s inet.Stream) {
	ids.responseHandler(s)
}

func (ids *IDService) Push() {
	for _, p := range ids.Host.Network().Peers() {
		go func(p peer.ID) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			s, err := ids.Host.NewStream(ctx, p, IDPush)
			if err != nil {
				log.Debugf("error opening push stream: %s", err.Error())
				return
			}

			ids.requestHandler(s)
		}(p)
	}
}

func (ids *IDService) populateMessage(mes *pb.Identify, c inet.Conn) {
	// set protocols this node is currently handling
	protos := ids.Host.Mux().Protocols()
	mes.Protocols = make([]string, len(protos))
	for i, p := range protos {
		mes.Protocols[i] = string(p)
	}

	// observed address so other side is informed of their
	// "public" address, at least in relation to us.
	mes.ObservedAddr = c.RemoteMultiaddr().Bytes()

	// set listen addrs, get our latest addrs from Host.
	laddrs := ids.Host.Addrs()
	mes.ListenAddrs = make([][]byte, len(laddrs))
	for i, addr := range laddrs {
		mes.ListenAddrs[i] = addr.Bytes()
	}
	log.Debugf("%s sent listen addrs to %s: %s", c.LocalPeer(), c.RemotePeer(), laddrs)

	// set our public key
	ownKey := ids.Host.Peerstore().PubKey(ids.Host.ID())

	// check if we even have a public key.
	if ownKey == nil {
		// public key is nil. We are either using insecure transport or something erratic happened.
		// check if we're even operating in "secure mode"
		if ids.Host.Peerstore().PrivKey(ids.Host.ID()) != nil {
			// private key is present. But NO public key. Something bad happened.
			log.Errorf("did not have own public key in Peerstore")
		}
		// if neither of the key is present it is safe to assume that we are using an insecure transport.
	} else {
		// public key is present. Safe to proceed.
		if kb, err := ownKey.Bytes(); err != nil {
			log.Errorf("failed to convert key to bytes")
		} else {
			mes.PublicKey = kb
		}
	}

	// set protocol versions
	pv := LibP2PVersion
	av := ClientVersion
	mes.ProtocolVersion = &pv
	mes.AgentVersion = &av
}

func (ids *IDService) consumeMessage(mes *pb.Identify, c inet.Conn) {
	p := c.RemotePeer()

	// mes.Protocols
	ids.Host.Peerstore().SetProtocols(p, mes.Protocols...)

	// mes.ObservedAddr
	ids.consumeObservedAddress(mes.GetObservedAddr(), c)

	// mes.ListenAddrs
	laddrs := mes.GetListenAddrs()
	lmaddrs := make([]ma.Multiaddr, 0, len(laddrs))
	for _, addr := range laddrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			log.Debugf("%s failed to parse multiaddr from %s %s", ID,
				p, c.RemoteMultiaddr())
			continue
		}
		lmaddrs = append(lmaddrs, maddr)
	}

	// if the address reported by the connection roughly matches their annoucned
	// listener addresses, its likely to be an external NAT address
	if HasConsistentTransport(c.RemoteMultiaddr(), lmaddrs) {
		lmaddrs = append(lmaddrs, c.RemoteMultiaddr())
	}

	// Extend the TTLs on the known (probably) good addresses.
	// Taking the lock ensures that we don't concurrently process a disconnect.
	ids.addrMu.Lock()
	switch ids.Host.Network().Connectedness(p) {
	case inet.Connected:
		ids.Host.Peerstore().AddAddrs(p, lmaddrs, pstore.ConnectedAddrTTL)
	default:
		ids.Host.Peerstore().AddAddrs(p, lmaddrs, pstore.RecentlyConnectedAddrTTL)
	}
	ids.addrMu.Unlock()

	log.Debugf("%s received listen addrs for %s: %s", c.LocalPeer(), c.RemotePeer(), lmaddrs)

	// get protocol versions
	pv := mes.GetProtocolVersion()
	av := mes.GetAgentVersion()

	ids.Host.Peerstore().Put(p, "ProtocolVersion", pv)
	ids.Host.Peerstore().Put(p, "AgentVersion", av)

	// get the key from the other side. we may not have it (no-auth transport)
	ids.consumeReceivedPubKey(c, mes.PublicKey)
}

func (ids *IDService) consumeReceivedPubKey(c inet.Conn, kb []byte) {
	lp := c.LocalPeer()
	rp := c.RemotePeer()

	if kb == nil {
		log.Debugf("%s did not receive public key for remote peer: %s", lp, rp)
		return
	}

	newKey, err := ic.UnmarshalPublicKey(kb)
	if err != nil {
		log.Warningf("%s cannot unmarshal key from remote peer: %s, %s", lp, rp, err)
		return
	}

	// verify key matches peer.ID
	np, err := peer.IDFromPublicKey(newKey)
	if err != nil {
		log.Debugf("%s cannot get peer.ID from key of remote peer: %s, %s", lp, rp, err)
		return
	}

	if np != rp {
		// if the newKey's peer.ID does not match known peer.ID...

		if rp == "" && np != "" {
			// if local peerid is empty, then use the new, sent key.
			err := ids.Host.Peerstore().AddPubKey(rp, newKey)
			if err != nil {
				log.Debugf("%s could not add key for %s to peerstore: %s", lp, rp, err)
			}

		} else {
			// we have a local peer.ID and it does not match the sent key... error.
			log.Errorf("%s received key for remote peer %s mismatch: %s", lp, rp, np)
		}
		return
	}

	currKey := ids.Host.Peerstore().PubKey(rp)
	if currKey == nil {
		// no key? no auth transport. set this one.
		err := ids.Host.Peerstore().AddPubKey(rp, newKey)
		if err != nil {
			log.Debugf("%s could not add key for %s to peerstore: %s", lp, rp, err)
		}
		return
	}

	// ok, we have a local key, we should verify they match.
	if currKey.Equals(newKey) {
		return // ok great. we're done.
	}

	// weird, got a different key... but the different key MATCHES the peer.ID.
	// this odd. let's log error and investigate. this should basically never happen
	// and it means we have something funky going on and possibly a bug.
	log.Errorf("%s identify got a different key for: %s", lp, rp)

	// okay... does ours NOT match the remote peer.ID?
	cp, err := peer.IDFromPublicKey(currKey)
	if err != nil {
		log.Errorf("%s cannot get peer.ID from local key of remote peer: %s, %s", lp, rp, err)
		return
	}
	if cp != rp {
		log.Errorf("%s local key for remote peer %s yields different peer.ID: %s", lp, rp, cp)
		return
	}

	// okay... curr key DOES NOT match new key. both match peer.ID. wat?
	log.Errorf("%s local key and received key for %s do not match, but match peer.ID", lp, rp)
}

// HasConsistentTransport returns true if the address 'a' shares a
// protocol set with any address in the green set. This is used
// to check if a given address might be one of the addresses a peer is
// listening on.
func HasConsistentTransport(a ma.Multiaddr, green []ma.Multiaddr) bool {
	protosMatch := func(a, b []ma.Protocol) bool {
		if len(a) != len(b) {
			return false
		}

		for i, p := range a {
			if b[i].Code != p.Code {
				return false
			}
		}
		return true
	}

	protos := a.Protocols()

	for _, ga := range green {
		if protosMatch(protos, ga.Protocols()) {
			return true
		}
	}

	return false
}

// IdentifyWait returns a channel which will be closed once
// "ProtocolIdentify" (handshake3) finishes on given conn.
// This happens async so the connection can start to be used
// even if handshake3 knowledge is not necesary.
// Users **MUST** call IdentifyWait _after_ IdentifyConn
func (ids *IDService) IdentifyWait(c inet.Conn) <-chan struct{} {
	ids.currmu.Lock()
	ch, found := ids.currid[c]
	ids.currmu.Unlock()
	if found {
		return ch
	}

	// if not found, it means we are already done identifying it, or
	// haven't even started. either way, return a new channel closed.
	ch = make(chan struct{})
	close(ch)
	return ch
}

func (ids *IDService) consumeObservedAddress(observed []byte, c inet.Conn) {
	if observed == nil {
		return
	}

	maddr, err := ma.NewMultiaddrBytes(observed)
	if err != nil {
		log.Debugf("error parsing received observed addr for %s: %s", c, err)
		return
	}

	// we should only use ObservedAddr when our connection's LocalAddr is one
	// of our ListenAddrs. If we Dial out using an ephemeral addr, knowing that
	// address's external mapping is not very useful because the port will not be
	// the same as the listen addr.
	ifaceaddrs, err := ids.Host.Network().InterfaceListenAddresses()
	if err != nil {
		log.Infof("failed to get interface listen addrs", err)
		return
	}

	log.Debugf("identify identifying observed multiaddr: %s %s", c.LocalMultiaddr(), ifaceaddrs)
	if !addrInAddrs(c.LocalMultiaddr(), ifaceaddrs) {
		// not in our list
		return
	}

	// ok! we have the observed version of one of our ListenAddresses!
	log.Debugf("added own observed listen addr: %s --> %s", c.LocalMultiaddr(), maddr)
	ids.observedAddrs.Add(maddr, c.LocalMultiaddr(), c.RemoteMultiaddr(),
		c.Stat().Direction)
}

func addrInAddrs(a ma.Multiaddr, as []ma.Multiaddr) bool {
	for _, b := range as {
		if a.Equal(b) {
			return true
		}
	}
	return false
}

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IDService

func (nn *netNotifiee) IDService() *IDService {
	return (*IDService)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	// TODO: deprecate the setConnHandler hook, and kick off
	// identification here.
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	// undo the setting of addresses to peer.ConnectedAddrTTL we did
	ids := nn.IDService()
	ids.addrMu.Lock()
	defer ids.addrMu.Unlock()

	if ids.Host.Network().Connectedness(v.RemotePeer()) != inet.Connected {
		// Last disconnect.
		ps := ids.Host.Peerstore()
		ps.UpdateAddrs(v.RemotePeer(), pstore.ConnectedAddrTTL, pstore.RecentlyConnectedAddrTTL)
	}
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}

func logProtocolMismatchDisconnect(c inet.Conn, protocol, agent string) {
	lm := make(lgbl.DeferredMap)
	lm["remotePeer"] = func() interface{} { return c.RemotePeer().Pretty() }
	lm["remoteAddr"] = func() interface{} { return c.RemoteMultiaddr().String() }
	lm["protocolVersion"] = protocol
	lm["agentVersion"] = agent
	log.Event(context.TODO(), "IdentifyProtocolMismatch", lm)
	log.Debugf("IdentifyProtocolMismatch %s %s %s (disconnected)", c.RemotePeer(), protocol, agent)
}
