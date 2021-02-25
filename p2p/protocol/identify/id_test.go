package identify_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/record"
	coretest "github.com/libp2p/go-libp2p-core/test"

	"github.com/libp2p/go-eventbus"
	libp2p "github.com/libp2p/go-libp2p"
	blhost "github.com/libp2p/go-libp2p-blankhost"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"

	"github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func subtestIDService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	h1p := h1.ID()
	h2p := h2.ID()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()

	sub, err := ids1.Host.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted), eventbus.BufSize(16))
	if err != nil {
		t.Fatal(err)
	}

	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{}) // nothing
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{}) // nothing

	// the forgetMe addr represents an address for h1 that h2 has learned out of band
	// (not via identify protocol). During the identify exchange, it will be
	// forgotten and replaced by the addrs h1 sends.
	forgetMe, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")

	h2.Peerstore().AddAddr(h1p, forgetMe, peerstore.RecentlyConnectedAddrTTL)
	time.Sleep(500 * time.Millisecond)

	h2pi := h2.Peerstore().PeerInfo(h2p)
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}

	h1t2c := h1.Network().ConnsToPeer(h2p)
	if len(h1t2c) == 0 {
		t.Fatal("should have a conn here")
	}

	ids1.IdentifyConn(h1t2c[0])

	// the IDService should be opened automatically, by the network.
	// what we should see now is that both peers know about each others listen addresses.
	t.Log("test peer1 has peer2 addrs correctly")
	testKnowsAddrs(t, h1, h2p, h2.Addrs())                       // has them
	testHasCertifiedAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p)) // should have signed addrs also
	testHasProtocolVersions(t, h1, h2p)
	testHasPublicKey(t, h1, h2p, h2.Peerstore().PubKey(h2p)) // h1 should have h2's public key

	// now, this wait we do have to do. it's the wait for the Listening side
	// to be done identifying the connection.
	c := h2.Network().ConnsToPeer(h1.ID())
	if len(c) < 1 {
		t.Fatal("should have connection by now at least.")
	}
	ids2.IdentifyConn(c[0])

	// and the protocol versions.
	t.Log("test peer2 has peer1 addrs correctly")
	testKnowsAddrs(t, h2, h1p, h1.Addrs()) // has them
	testHasCertifiedAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))
	testHasProtocolVersions(t, h2, h1p)
	testHasPublicKey(t, h2, h1p, h1.Peerstore().PubKey(h1p)) // h1 should have h2's public key

	// Need both sides to actually notice that the connection has been closed.
	h1.Network().ClosePeer(h2p)
	h2.Network().ClosePeer(h1p)
	if len(h2.Network().ConnsToPeer(h1.ID())) != 0 || len(h1.Network().ConnsToPeer(h2.ID())) != 0 {
		t.Fatal("should have no connections")
	}

	t.Log("testing addrs just after disconnect")
	// addresses don't immediately expire on disconnect, so we should still have them
	testKnowsAddrs(t, h2, h1p, h1.Addrs())
	testKnowsAddrs(t, h1, h2p, h2.Addrs())
	testHasCertifiedAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p))
	testHasCertifiedAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))

	// the addrs had their TTLs reduced on disconnect, and
	// will be forgotten soon after
	t.Log("testing addrs after TTL expiration")
	time.Sleep(2 * time.Second)
	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{})
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{})
	testHasCertifiedAddrs(t, h1, h2p, []ma.Multiaddr{})
	testHasCertifiedAddrs(t, h2, h1p, []ma.Multiaddr{})

	// test we have the TCP & UDP NAT Device Types on both sides
	udp, err := h1.Peerstore().Get(h2p, identify.UDPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeUnknown, udp.(network.NATDeviceType))
	tcp, err := h1.Peerstore().Get(h2p, identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeUnknown, tcp.(network.NATDeviceType))

	udp, err = h2.Peerstore().Get(h1p, identify.UDPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeUnknown, udp.(network.NATDeviceType))
	tcp, err = h2.Peerstore().Get(h1p, identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeUnknown, tcp.(network.NATDeviceType))

	// test that we received the "identify completed" event.
	select {
	case <-sub.Out():
	case <-time.After(10 * time.Second):
		t.Fatalf("expected EvtPeerIdentificationCompleted event within 10 seconds; none received")
	}
}

func testKnowsAddrs(t *testing.T, h host.Host, p peer.ID, expected []ma.Multiaddr) {
	t.Helper()

	actual := h.Peerstore().Addrs(p)
	checkAddrs(t, expected, actual, fmt.Sprintf("%s did not have addr for %s", h.ID(), p))
}

func testHasCertifiedAddrs(t *testing.T, h host.Host, p peer.ID, expected []ma.Multiaddr) {
	t.Helper()
	cab, ok := peerstore.GetCertifiedAddrBook(h.Peerstore())
	if !ok {
		t.Error("expected peerstore to implement CertifiedAddrBook")
	}
	recordEnvelope := cab.GetPeerRecord(p)
	if recordEnvelope == nil {
		if len(expected) == 0 {
			return
		}
		t.Fatalf("peerstore has no signed record for peer %s", p)
	}
	r, err := recordEnvelope.Record()
	if err != nil {
		t.Error("Error unwrapping signed PeerRecord from envelope", err)
	}
	rec, ok := r.(*peer.PeerRecord)
	if !ok {
		t.Error("unexpected record type")
	}

	checkAddrs(t, expected, rec.Addrs, fmt.Sprintf("%s did not have certified addr for %s", h.ID(), p))
}

func checkAddrs(t *testing.T, expected, actual []ma.Multiaddr, msg string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("expected: %s", expected)
		t.Errorf("actual: %s", actual)
		t.Fatal("dont have the same addresses")
	}

	have := map[string]struct{}{}
	for _, addr := range actual {
		have[addr.String()] = struct{}{}
	}
	for _, addr := range expected {
		if _, found := have[addr.String()]; !found {
			t.Errorf("%s: %s", msg, addr)
		}
	}
}

func testHasProtocolVersions(t *testing.T, h host.Host, p peer.ID) {
	v, err := h.Peerstore().Get(p, "ProtocolVersion")
	if v == nil {
		t.Error("no protocol version")
		return
	}
	if v.(string) != identify.LibP2PVersion {
		t.Error("protocol mismatch", err)
	}
	v, err = h.Peerstore().Get(p, "AgentVersion")
	if v.(string) != identify.ClientVersion {
		t.Error("agent version mismatch", err)
	}
}

func testHasPublicKey(t *testing.T, h host.Host, p peer.ID, shouldBe ic.PubKey) {
	k := h.Peerstore().PubKey(p)
	if k == nil {
		t.Error("no public key")
		return
	}
	if !k.Equals(shouldBe) {
		t.Error("key mismatch")
		return
	}

	p2, err := peer.IDFromPublicKey(k)
	if err != nil {
		t.Error("could not make key")
	} else if p != p2 {
		t.Error("key does not match peerid")
	}
}

func getSignedRecord(t *testing.T, h host.Host, p peer.ID) *record.Envelope {
	cab, ok := peerstore.GetCertifiedAddrBook(h.Peerstore())
	require.True(t, ok)
	rec := cab.GetPeerRecord(p)
	return rec
}

// we're using BlankHost in our tests, which doesn't automatically generate peer records
// and emit address change events on the bus like BasicHost.
// This generates a record, puts it in the peerstore and emits an addr change event
// which will cause the identify service to push it to all peers it's connected to.
func emitAddrChangeEvt(t *testing.T, h host.Host) {
	t.Helper()

	key := h.Peerstore().PrivKey(h.ID())
	if key == nil {
		t.Fatal("no private key for host")
	}

	rec := peer.NewPeerRecord()
	rec.PeerID = h.ID()
	rec.Addrs = h.Addrs()
	signed, err := record.Seal(rec, key)
	if err != nil {
		t.Fatalf("error generating peer record: %s", err)
	}

	cab, ok := peerstore.GetCertifiedAddrBook(h.Peerstore())
	require.True(t, ok)
	_, err = cab.ConsumePeerRecord(signed, peerstore.PermanentAddrTTL)
	require.NoError(t, err)

	evt := event.EvtLocalAddressesUpdated{}
	emitter, err := h.EventBus().Emitter(new(event.EvtLocalAddressesUpdated), eventbus.Stateful)
	if err != nil {
		t.Fatal(err)
	}
	err = emitter.Emit(evt)
	if err != nil {
		t.Fatal(err)
	}
}

// TestIDServiceWait gives the ID service 1s to finish after dialing
// this is because it used to be concurrent. Now, Dial wait till the
// id service is done.
func TestIDService(t *testing.T) {
	oldTTL := peerstore.RecentlyConnectedAddrTTL
	peerstore.RecentlyConnectedAddrTTL = time.Second
	defer func() {
		peerstore.RecentlyConnectedAddrTTL = oldTTL
	}()

	N := 3
	for i := 0; i < N; i++ {
		subtestIDService(t)
	}
}

func TestProtoMatching(t *testing.T) {
	tcp1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	tcp2, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/2345")
	tcp3, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/4567")
	utp, _ := ma.NewMultiaddr("/ip4/1.2.3.4/udp/1234/utp")

	if !identify.HasConsistentTransport(tcp1, []ma.Multiaddr{tcp2, tcp3, utp}) {
		t.Fatal("expected match")
	}

	if identify.HasConsistentTransport(utp, []ma.Multiaddr{tcp2, tcp3}) {
		t.Fatal("expected mismatch")
	}
}

func TestLocalhostAddrFiltering(t *testing.T) {
	t.Skip("need to fix this test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := mocknet.New(ctx)
	id1 := coretest.RandPeerIDFatal(t)
	ps1 := pstoremem.NewPeerstore()
	p1addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	p1addr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/2345")
	ps1.AddAddrs(id1, []ma.Multiaddr{p1addr1, p1addr2}, peerstore.PermanentAddrTTL)
	p1, err := mn.AddPeerWithPeerstore(id1, ps1)
	if err != nil {
		t.Fatal(err)
	}

	id2 := coretest.RandPeerIDFatal(t)
	ps2 := pstoremem.NewPeerstore()
	p2addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.5/tcp/1234")
	p2addr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/3456")
	p2addrs := []ma.Multiaddr{p2addr1, p2addr2}
	ps2.AddAddrs(id2, p2addrs, peerstore.PermanentAddrTTL)
	p2, err := mn.AddPeerWithPeerstore(id2, ps2)
	if err != nil {
		t.Fatal(err)
	}

	id3 := coretest.RandPeerIDFatal(t)
	ps3 := pstoremem.NewPeerstore()
	p3addr1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4567")
	ps3.AddAddrs(id3, []ma.Multiaddr{p3addr1}, peerstore.PermanentAddrTTL)
	p3, err := mn.AddPeerWithPeerstore(id3, ps3)
	if err != nil {
		t.Fatal(err)
	}

	err = mn.LinkAll()
	if err != nil {
		t.Fatal(err)
	}
	p1.Connect(ctx, peer.AddrInfo{
		ID:    id2,
		Addrs: p2addrs[0:1],
	})
	p3.Connect(ctx, peer.AddrInfo{
		ID:    id2,
		Addrs: p2addrs[1:],
	})

	ids1, err := identify.NewIDService(p1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(p2)
	require.NoError(t, err)

	ids3, err := identify.NewIDService(p3)
	require.NoError(t, err)

	defer func() {
		ids1.Close()
		ids2.Close()
		ids3.Close()
	}()

	conns := p2.Network().ConnsToPeer(id1)
	if len(conns) == 0 {
		t.Fatal("no conns")
	}
	conn := conns[0]
	ids2.IdentifyConn(conn)
	addrs := p2.Peerstore().Addrs(id1)
	if len(addrs) != 1 {
		t.Fatalf("expected one addr, found %s", addrs)
	}

	conns = p3.Network().ConnsToPeer(id2)
	if len(conns) == 0 {
		t.Fatal("no conns")
	}
	conn = conns[0]
	ids3.IdentifyConn(conn)
	addrs = p3.Peerstore().Addrs(id2)
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addrs for %s, found %d: %s", id2, len(addrs), addrs)
	}
}

func TestIdentifyDeltaOnProtocolChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	defer h2.Close()
	defer h1.Close()

	h2.SetStreamHandler(protocol.TestingID, func(_ network.Stream) {})

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer func() {
		ids1.Close()
		ids2.Close()
	}()

	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatal(err)
	}

	conn := h1.Network().ConnsToPeer(h2.ID())[0]
	select {
	case <-ids1.IdentifyWait(conn):
	case <-time.After(5 * time.Second):
		t.Fatal("took over 5 seconds to identify")
	}

	protos, err := h1.Peerstore().GetProtocols(h2.ID())
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(protos)
	if sort.SearchStrings(protos, string(protocol.TestingID)) == len(protos) {
		t.Fatalf("expected peer 1 to know that peer 2 speaks the Test protocol amongst others")
	}

	// set up a subscriber to listen to peer protocol updated events in h1. We expect to receive events from h2
	// as protocols are added and removed.
	sub, err := h1.EventBus().Subscribe(&event.EvtPeerProtocolsUpdated{}, eventbus.BufSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// add two new protocols in h2 and wait for identify to send deltas.
	h2.SetStreamHandler(protocol.ID("foo"), func(_ network.Stream) {})
	h2.SetStreamHandler(protocol.ID("bar"), func(_ network.Stream) {})

	// check that h1 now knows about h2's new protocols.
	require.Eventually(t, func() bool {
		protos, err = h1.Peerstore().GetProtocols(h2.ID())
		if err != nil {
			return false
		}
		have := make(map[string]struct{}, len(protos))
		for _, p := range protos {
			have[p] = struct{}{}
		}

		_, okfoo := have["foo"]
		_, okbar := have["bar"]
		return okfoo && okbar
	}, 5*time.Second, 500*time.Millisecond)

	// remove one of the newly added protocols from h2, and wait for identify to send the delta.
	h2.RemoveStreamHandler(protocol.ID("bar"))
	// check that h1 now has forgotten about h2's bar protocol.
	require.Eventually(t, func() bool {
		protos, err = h1.Peerstore().GetProtocols(h2.ID())
		if err != nil {
			return false
		}
		have := make(map[string]struct{}, len(protos))
		for _, p := range protos {
			have[p] = struct{}{}
		}

		_, okfoo := have["foo"]
		_, okbar := have["bar"]
		return okfoo && !okbar
	}, 5*time.Second, 500*time.Millisecond)

	// make sure that h1 emitted events in the eventbus for h2's protocol updates.
	done := make(chan struct{})

	var lk sync.Mutex
	var added []string
	var removed []string
	var success bool

	go func() {
		defer close(done)
		for {
			select {
			case <-time.After(5 * time.Second):
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				evt := e.(event.EvtPeerProtocolsUpdated)
				lk.Lock()
				added = append(added, protocol.ConvertToStrings(evt.Added)...)
				removed = append(removed, protocol.ConvertToStrings(evt.Removed)...)
				sort.Strings(added)
				sort.Strings(removed)
				if reflect.DeepEqual(added, []string{"bar", "foo"}) &&
					reflect.DeepEqual(removed, []string{"bar"}) {
					success = true
					lk.Unlock()
					return
				}
				lk.Unlock()
			}
		}
	}()

	<-done

	lk.Lock()
	defer lk.Unlock()
	require.True(t, success, "did not get correct peer protocol updated events")
}

// TestIdentifyDeltaWhileIdentifyingConn tests that the host waits to push delta updates if an identify is ongoing.
func TestIdentifyDeltaWhileIdentifyingConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	defer h2.Close()
	defer h1.Close()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()

	// replace the original identify handler by one that blocks until we close the block channel.
	// this allows us to control how long identify runs.
	block := make(chan struct{})
	handler := func(s network.Stream) {
		<-block
		w := protoio.NewDelimitedWriter(s)
		w.WriteMsg(&pb.Identify{Protocols: h1.Mux().Protocols()})
		s.Close()
	}
	h1.RemoveStreamHandler(identify.ID)
	h1.SetStreamHandler(identify.ID, handler)

	// from h2 connect to h1.
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatal(err)
	}

	// from h2, identify h1.
	conn := h2.Network().ConnsToPeer(h1.ID())[0]
	go func() {
		ids2.IdentifyConn(conn)
	}()

	<-time.After(500 * time.Millisecond)

	// subscribe to events in h1; after identify h1 should receive the delta from h2 and publish an event in the bus.
	sub, err := h1.EventBus().Subscribe(&event.EvtPeerProtocolsUpdated{}, eventbus.BufSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// add a handler in h2; the delta to h1 will queue until we're done identifying h1.
	h2.SetStreamHandler(protocol.TestingID, func(_ network.Stream) {})
	<-time.After(500 * time.Millisecond)

	// make sure we haven't received any events yet.
	if q := len(sub.Out()); q > 0 {
		t.Fatalf("expected no events yet; queued: %d", q)
	}

	close(block)
	select {
	case evt := <-sub.Out():
		e := evt.(event.EvtPeerProtocolsUpdated)
		if e.Peer != h2.ID() || len(e.Added) != 1 || e.Added[0] != protocol.TestingID {
			t.Fatalf("expected an event for protocol changes in h2, with the testing protocol added; instead got: %v", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out while waiting for an event for the protocol changes in h2")
	}
}

func TestIdentifyPushOnAddrChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	h1p := h1.ID()
	h2p := h2.ID()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)
	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()

	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{}) // nothing
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{}) // nothing

	h2pi := h2.Peerstore().PeerInfo(h2p)
	require.NoError(t, h1.Connect(ctx, h2pi))
	// h1 should immediately see a connection from h2
	require.Len(t, h1.Network().ConnsToPeer(h2p), 1)
	// wait for h2 to Identify itself so we are sure h2 has seen the connection.
	ids1.IdentifyConn(h1.Network().ConnsToPeer(h2p)[0])

	// h2 should now see the connection and we should wait for h1 to Identify itself to h2.
	require.Len(t, h2.Network().ConnsToPeer(h1p), 1)
	ids2.IdentifyConn(h2.Network().ConnsToPeer(h1p)[0])

	testKnowsAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p))
	testKnowsAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))

	// change addr on host 1 and ensure host2 gets a push
	lad := ma.StringCast("/ip4/127.0.0.1/tcp/1234")
	require.NoError(t, h1.Network().Listen(lad))
	require.Contains(t, h1.Addrs(), lad)
	emitAddrChangeEvt(t, h1)

	require.Eventually(t, func() bool {
		addrs := h2.Peerstore().Addrs(h1p)
		for _, ad := range addrs {
			if ad.Equal(lad) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	require.NotNil(t, getSignedRecord(t, h2, h1p))

	// change addr on host2 and ensure host 1 gets a pus
	lad = ma.StringCast("/ip4/127.0.0.1/tcp/1235")
	require.NoError(t, h2.Network().Listen(lad))
	require.Contains(t, h2.Addrs(), lad)
	emitAddrChangeEvt(t, h2)

	require.Eventually(t, func() bool {
		addrs := h1.Peerstore().Addrs(h2p)
		for _, ad := range addrs {
			if ad.Equal(lad) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	require.NotNil(t, getSignedRecord(t, h1, h2p))

	// change addr on host2 again
	lad2 := ma.StringCast("/ip4/127.0.0.1/tcp/1236")
	require.NoError(t, h2.Network().Listen(lad2))
	require.Contains(t, h2.Addrs(), lad2)
	emitAddrChangeEvt(t, h2)

	require.Eventually(t, func() bool {
		addrs := h1.Peerstore().Addrs(h2p)
		for _, ad := range addrs {
			if ad.Equal(lad2) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	require.NotNil(t, getSignedRecord(t, h1, h2p))
}

func TestUserAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, err := libp2p.New(
		ctx,
		libp2p.UserAgent("foo"),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New(
		ctx,
		libp2p.UserAgent("bar"),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	if err != nil {
		t.Fatal(err)
	}
	av, err := h1.Peerstore().Get(h2.ID(), "AgentVersion")
	if err != nil {
		t.Fatal(err)
	}
	if ver, ok := av.(string); !ok || ver != "bar" {
		t.Errorf("expected agent version %q, got %q", "bar", av)
	}
}

func TestNotListening(t *testing.T) {
	// Make sure we don't panic if we're not listening on any addresses.
	//
	// https://github.com/libp2p/go-libp2p/issues/939
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, err := libp2p.New(
		ctx,
		libp2p.NoListenAddrs,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New(
		ctx,
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSendPushIfDeltaNotSupported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	defer h2.Close()
	defer h1.Close()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer func() {
		ids1.Close()
		ids2.Close()
	}()

	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	require.NoError(t, err)

	// wait for them to Identify each other
	ids1.IdentifyConn(h1.Network().ConnsToPeer(h2.ID())[0])
	ids2.IdentifyConn(h2.Network().ConnsToPeer(h1.ID())[0])

	// h1 knows h2 speaks Delta
	sup, err := h1.Peerstore().SupportsProtocols(h2.ID(), []string{identify.IDDelta}...)
	require.NoError(t, err)
	require.Equal(t, []string{identify.IDDelta}, sup)

	// h2 stops supporting Delta and that information flows to h1
	h2.RemoveStreamHandler(identify.IDDelta)

	require.Eventually(t, func() bool {
		sup, err := h1.Peerstore().SupportsProtocols(h2.ID(), []string{identify.IDDelta}...)
		return err == nil && len(sup) == 0
	}, 5*time.Second, 500*time.Millisecond)

	// h1 starts listening on a new protocol and h2 finds out about that through a push
	h1.SetStreamHandler("rand", func(network.Stream) {})
	require.Eventually(t, func() bool {
		sup, err := h2.Peerstore().SupportsProtocols(h1.ID(), []string{"rand"}...)
		return err == nil && len(sup) == 1 && sup[0] == "rand"
	}, 5*time.Second, 500*time.Millisecond)

	// h1 stops listening on a protocol and h2 finds out about it via a push
	h1.RemoveStreamHandler("rand")
	require.Eventually(t, func() bool {
		sup, err := h2.Peerstore().SupportsProtocols(h1.ID(), []string{"rand"}...)
		return err == nil && len(sup) == 0
	}, 5*time.Second, 500*time.Millisecond)
}

func TestLargeIdentifyMessage(t *testing.T) {
	oldTTL := peerstore.RecentlyConnectedAddrTTL
	peerstore.RecentlyConnectedAddrTTL = time.Second
	defer func() {
		peerstore.RecentlyConnectedAddrTTL = oldTTL
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sk1, _, err := coretest.RandTestKeyPair(ic.RSA, 4096)
	require.NoError(t, err)
	sk2, _, err := coretest.RandTestKeyPair(ic.RSA, 4096)
	require.NoError(t, err)

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx, swarmt.OptPeerPrivateKey(sk1)))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx, swarmt.OptPeerPrivateKey(sk2)))

	// add protocol strings to make the message larger
	// about 2K of protocol strings
	for i := 0; i < 500; i++ {
		r := fmt.Sprintf("rand%d", i)
		h1.SetStreamHandler(protocol.ID(r), func(network.Stream) {})
		h2.SetStreamHandler(protocol.ID(r), func(network.Stream) {})
	}

	h1p := h1.ID()
	h2p := h2.ID()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()

	sub, err := ids1.Host.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted), eventbus.BufSize(16))
	if err != nil {
		t.Fatal(err)
	}

	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{}) // nothing
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{}) // nothing

	// the forgetMe addr represents an address for h1 that h2 has learned out of band
	// (not via identify protocol). During the identify exchange, it will be
	// forgotten and replaced by the addrs h1 sends.
	forgetMe, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")

	h2.Peerstore().AddAddr(h1p, forgetMe, peerstore.RecentlyConnectedAddrTTL)
	time.Sleep(500 * time.Millisecond)

	h2pi := h2.Peerstore().PeerInfo(h2p)
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}

	h1t2c := h1.Network().ConnsToPeer(h2p)
	if len(h1t2c) == 0 {
		t.Fatal("should have a conn here")
	}

	ids1.IdentifyConn(h1t2c[0])

	// the IDService should be opened automatically, by the network.
	// what we should see now is that both peers know about each others listen addresses.
	t.Log("test peer1 has peer2 addrs correctly")
	testKnowsAddrs(t, h1, h2p, h2.Addrs())                       // has them
	testHasCertifiedAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p)) // should have signed addrs also
	testHasProtocolVersions(t, h1, h2p)
	testHasPublicKey(t, h1, h2p, h2.Peerstore().PubKey(h2p)) // h1 should have h2's public key

	// now, this wait we do have to do. it's the wait for the Listening side
	// to be done identifying the connection.
	c := h2.Network().ConnsToPeer(h1.ID())
	if len(c) < 1 {
		t.Fatal("should have connection by now at least.")
	}
	ids2.IdentifyConn(c[0])

	// and the protocol versions.
	t.Log("test peer2 has peer1 addrs correctly")
	testKnowsAddrs(t, h2, h1p, h1.Addrs()) // has them
	testHasCertifiedAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))
	testHasProtocolVersions(t, h2, h1p)
	testHasPublicKey(t, h2, h1p, h1.Peerstore().PubKey(h1p)) // h1 should have h2's public key

	// Need both sides to actually notice that the connection has been closed.
	h1.Network().ClosePeer(h2p)
	h2.Network().ClosePeer(h1p)
	if len(h2.Network().ConnsToPeer(h1.ID())) != 0 || len(h1.Network().ConnsToPeer(h2.ID())) != 0 {
		t.Fatal("should have no connections")
	}

	t.Log("testing addrs just after disconnect")
	// addresses don't immediately expire on disconnect, so we should still have them
	testKnowsAddrs(t, h2, h1p, h1.Addrs())
	testKnowsAddrs(t, h1, h2p, h2.Addrs())
	testHasCertifiedAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p))
	testHasCertifiedAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))

	// the addrs had their TTLs reduced on disconnect, and
	// will be forgotten soon after
	t.Log("testing addrs after TTL expiration")
	time.Sleep(2 * time.Second)
	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{})
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{})
	testHasCertifiedAddrs(t, h1, h2p, []ma.Multiaddr{})
	testHasCertifiedAddrs(t, h2, h1p, []ma.Multiaddr{})

	// test that we received the "identify completed" event.
	select {
	case <-sub.Out():
	case <-time.After(10 * time.Second):
		t.Fatalf("expected EvtPeerIdentificationCompleted event within 10 seconds; none received")
	}
}

func TestLargePushMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sk1, _, err := coretest.RandTestKeyPair(ic.RSA, 4096)
	require.NoError(t, err)
	sk2, _, err := coretest.RandTestKeyPair(ic.RSA, 4096)
	require.NoError(t, err)

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx, swarmt.OptPeerPrivateKey(sk1)))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx, swarmt.OptPeerPrivateKey(sk2)))

	// add protocol strings to make the message larger
	// about 2K of protocol strings
	for i := 0; i < 500; i++ {
		r := fmt.Sprintf("rand%d", i)
		h1.SetStreamHandler(protocol.ID(r), func(network.Stream) {})
		h2.SetStreamHandler(protocol.ID(r), func(network.Stream) {})
	}

	h1p := h1.ID()
	h2p := h2.ID()

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()

	testKnowsAddrs(t, h1, h2p, []ma.Multiaddr{}) // nothing
	testKnowsAddrs(t, h2, h1p, []ma.Multiaddr{}) // nothing

	h2pi := h2.Peerstore().PeerInfo(h2p)
	require.NoError(t, h1.Connect(ctx, h2pi))
	// h1 should immediately see a connection from h2
	require.Len(t, h1.Network().ConnsToPeer(h2p), 1)
	// wait for h2 to Identify itself so we are sure h2 has seen the connection.
	ids1.IdentifyConn(h1.Network().ConnsToPeer(h2p)[0])

	// h2 should now see the connection and we should wait for h1 to Identify itself to h2.
	require.Len(t, h2.Network().ConnsToPeer(h1p), 1)
	ids2.IdentifyConn(h2.Network().ConnsToPeer(h1p)[0])

	testKnowsAddrs(t, h1, h2p, h2.Peerstore().Addrs(h2p))
	testKnowsAddrs(t, h2, h1p, h1.Peerstore().Addrs(h1p))

	// change addr on host 1 and ensure host2 gets a push
	lad := ma.StringCast("/ip4/127.0.0.1/tcp/1234")
	require.NoError(t, h1.Network().Listen(lad))
	require.Contains(t, h1.Addrs(), lad)
	emitAddrChangeEvt(t, h1)

	require.Eventually(t, func() bool {
		addrs := h2.Peerstore().Addrs(h1p)
		for _, ad := range addrs {
			if ad.Equal(lad) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	require.NotNil(t, getSignedRecord(t, h2, h1p))

	// change addr on host2 and ensure host 1 gets a pus
	lad = ma.StringCast("/ip4/127.0.0.1/tcp/1235")
	require.NoError(t, h2.Network().Listen(lad))
	require.Contains(t, h2.Addrs(), lad)
	emitAddrChangeEvt(t, h2)

	require.Eventually(t, func() bool {
		addrs := h1.Peerstore().Addrs(h2p)
		for _, ad := range addrs {
			if ad.Equal(lad) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	testHasCertifiedAddrs(t, h1, h2p, h2.Addrs())

	// change addr on host2 again
	lad2 := ma.StringCast("/ip4/127.0.0.1/tcp/1236")
	require.NoError(t, h2.Network().Listen(lad2))
	require.Contains(t, h2.Addrs(), lad2)
	emitAddrChangeEvt(t, h2)

	require.Eventually(t, func() bool {
		addrs := h1.Peerstore().Addrs(h2p)
		for _, ad := range addrs {
			if ad.Equal(lad2) {
				return true
			}
		}
		return false
	}, 5*time.Second, 500*time.Millisecond)
	testHasCertifiedAddrs(t, h2, h1p, h1.Addrs())
}

func TestIdentifyResponseReadTimeout(t *testing.T) {
	ctx := context.Background()
	timeout := identify.StreamReadTimeout
	identify.StreamReadTimeout = 100 * time.Millisecond
	defer func() {
		identify.StreamReadTimeout = timeout
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	defer h1.Close()
	defer h2.Close()

	h2p := h2.ID()
	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)

	defer ids1.Close()
	defer ids2.Close()
	// remote stream handler will just hang and not send back an identify response
	h2.SetStreamHandler(identify.ID, func(s network.Stream) {
		time.Sleep(100 * time.Second)
	})

	sub, err := ids1.Host.EventBus().Subscribe(new(event.EvtPeerIdentificationFailed), eventbus.BufSize(16))
	require.NoError(t, err)

	h2pi := h2.Peerstore().PeerInfo(h2p)
	require.NoError(t, h1.Connect(ctx, h2pi))

	select {
	case ev := <-sub.Out():
		fev := ev.(event.EvtPeerIdentificationFailed)
		require.EqualError(t, fev.Reason, "i/o deadline reached")
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive identify failure event")
	}
}

func TestIncomingIDStreamsTimeout(t *testing.T) {
	ctx := context.Background()
	timeout := identify.StreamReadTimeout
	identify.StreamReadTimeout = 100 * time.Millisecond
	defer func() {
		identify.StreamReadTimeout = timeout
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	protocols := []protocol.ID{identify.IDPush, identify.IDDelta}

	for _, p := range protocols {
		h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
		h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
		defer h1.Close()
		defer h2.Close()

		ids1, err := identify.NewIDService(h1)
		require.NoError(t, err)

		ids2, err := identify.NewIDService(h2)
		require.NoError(t, err)

		defer ids1.Close()
		defer ids2.Close()

		h2p := h2.ID()
		h2pi := h2.Peerstore().PeerInfo(h2p)
		require.NoError(t, h1.Connect(ctx, h2pi))

		_, err = h1.NewStream(ctx, h2p, p)
		require.NoError(t, err)

		// remote peer should eventually reset stream
		require.Eventually(t, func() bool {
			c := h2.Network().ConnsToPeer(h1.ID())[0]
			return len(c.GetStreams()) == 0
		}, 1*time.Second, 200*time.Millisecond)
	}
}

func TestNATDeviceTypeExchangedAndPersisted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	ids1, err := identify.NewIDService(h1)
	require.NoError(t, err)

	ids2, err := identify.NewIDService(h2)
	require.NoError(t, err)
	defer ids1.Close()
	defer ids2.Close()

	h1.SetStreamHandler(identify.ID, func(s network.Stream) {
		msg := new(pb.Identify)
		msg.TcpNATDeviceType = pb.Identify_SYMMETRIC.Enum()
		msg.UdpNATDeviceType = pb.Identify_CONE.Enum()

		wr := protoio.NewDelimitedWriter(s)
		wr.WriteMsg(msg)
		s.Close()
	})

	h2.SetStreamHandler(identify.ID, func(s network.Stream) {
		msg := new(pb.Identify)
		msg.TcpNATDeviceType = pb.Identify_CONE.Enum()
		msg.UdpNATDeviceType = pb.Identify_SYMMETRIC.Enum()

		wr := protoio.NewDelimitedWriter(s)
		wr.WriteMsg(msg)
		s.Close()
	})

	require.NoError(t, h1.Connect(ctx, peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}))

	ids1.IdentifyConn(h1.Network().ConnsToPeer(h2.ID())[0])
	ids2.IdentifyConn(h2.Network().ConnsToPeer(h1.ID())[0])

	// on h1
	tcp, err := h1.Peerstore().Get(h2.ID(), identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeCone, tcp.(network.NATDeviceType))
	udp, err := h1.Peerstore().Get(h2.ID(), identify.UDPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeSymmetric, udp.(network.NATDeviceType))

	// on h2
	tcp, err = h2.Peerstore().Get(h1.ID(), identify.TCPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeSymmetric, tcp.(network.NATDeviceType))
	udp, err = h2.Peerstore().Get(h1.ID(), identify.UDPNATDeviceTypeKey)
	require.NoError(t, err)
	require.Equal(t, network.NATDeviceTypeCone, udp.(network.NATDeviceType))
}
