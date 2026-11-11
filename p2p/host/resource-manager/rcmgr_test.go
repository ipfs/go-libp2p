package rcmgr

import (
	"fmt"
	"net"
	"net/netip"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/libp2p/go-libp2p/x/rate"
	"github.com/stretchr/testify/require"

	"github.com/multiformats/go-multiaddr"
)

var dummyMA = multiaddr.StringCast("/ip4/1.2.3.4/tcp/1234")

func TestResourceManager(t *testing.T) {
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	protoA := protocol.ID("/A")
	protoB := protocol.ID("/B")
	svcA := "A.svc"
	svcB := "B.svc"
	nmgr, err := NewResourceManager(
		NewFixedLimiter(ConcreteLimitConfig{
			system: BaseLimit{
				Memory:          16384,
				StreamsInbound:  3,
				StreamsOutbound: 3,
				Streams:         6,
				ConnsInbound:    3,
				ConnsOutbound:   3,
				Conns:           6,
				FD:              2,
			},
			transient: BaseLimit{
				Memory:          4096,
				StreamsInbound:  1,
				StreamsOutbound: 1,
				Streams:         2,
				ConnsInbound:    1,
				ConnsOutbound:   1,
				Conns:           2,
				FD:              1,
			},
			serviceDefault: BaseLimit{
				Memory:          4096,
				StreamsInbound:  1,
				StreamsOutbound: 1,
				Streams:         2,
				ConnsInbound:    1,
				ConnsOutbound:   1,
				Conns:           2,
				FD:              1,
			},
			servicePeerDefault: BaseLimit{
				Memory:          4096,
				StreamsInbound:  5,
				StreamsOutbound: 5,
				Streams:         10,
			},
			service: map[string]BaseLimit{
				svcA: {
					Memory:          8192,
					StreamsInbound:  2,
					StreamsOutbound: 2,
					Streams:         4,
					ConnsInbound:    2,
					ConnsOutbound:   2,
					Conns:           4,
					FD:              1,
				},
				svcB: {
					Memory:          8192,
					StreamsInbound:  2,
					StreamsOutbound: 2,
					Streams:         4,
					ConnsInbound:    2,
					ConnsOutbound:   2,
					Conns:           4,
					FD:              1,
				},
			},
			servicePeer: map[string]BaseLimit{
				svcB: {
					Memory:          8192,
					StreamsInbound:  1,
					StreamsOutbound: 1,
					Streams:         2,
				},
			},
			protocolDefault: BaseLimit{
				Memory:          4096,
				StreamsInbound:  1,
				StreamsOutbound: 1,
				Streams:         2,
			},
			protocol: map[protocol.ID]BaseLimit{
				protoA: {
					Memory:          8192,
					StreamsInbound:  2,
					StreamsOutbound: 2,
					Streams:         2,
				},
			},
			protocolPeer: map[protocol.ID]BaseLimit{
				protoB: {
					Memory:          8192,
					StreamsInbound:  1,
					StreamsOutbound: 1,
					Streams:         2,
				},
			},
			peerDefault: BaseLimit{
				Memory:          4096,
				StreamsInbound:  1,
				StreamsOutbound: 1,
				Streams:         2,
				ConnsInbound:    1,
				ConnsOutbound:   1,
				Conns:           2,
				FD:              1,
			},
			protocolPeerDefault: BaseLimit{
				Memory:          4096,
				StreamsInbound:  5,
				StreamsOutbound: 5,
				Streams:         10,
			},
			peer: map[peer.ID]BaseLimit{
				peerA: {
					Memory:          8192,
					StreamsInbound:  2,
					StreamsOutbound: 2,
					Streams:         4,
					ConnsInbound:    2,
					ConnsOutbound:   2,
					Conns:           4,
					FD:              1,
				},
			},
			conn: BaseLimit{
				Memory:        4096,
				ConnsInbound:  1,
				ConnsOutbound: 1,
				Conns:         1,
				FD:            1,
			},
			stream: BaseLimit{
				Memory:          4096,
				StreamsInbound:  1,
				StreamsOutbound: 1,
				Streams:         1,
			},
		}),
	)

	if err != nil {
		t.Fatal(err)
	}

	mgr := nmgr.(*resourceManager)
	defer mgr.Close()

	checkRefCnt := func(s *resourceScope, count int) {
		t.Helper()
		if refCnt := s.refCnt; refCnt != count {
			t.Fatalf("expected refCnt of %d, got %d", count, refCnt)
		}
	}
	checkSystem := func(check func(s *resourceScope)) {
		if err := mgr.ViewSystem(func(s network.ResourceScope) error {
			check(s.(*systemScope).resourceScope)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkTransient := func(check func(s *resourceScope)) {
		if err := mgr.ViewTransient(func(s network.ResourceScope) error {
			check(s.(*transientScope).resourceScope)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkService := func(svc string, check func(s *resourceScope)) {
		if err := mgr.ViewService(svc, func(s network.ServiceScope) error {
			check(s.(*serviceScope).resourceScope)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkProtocol := func(p protocol.ID, check func(s *resourceScope)) {
		if err := mgr.ViewProtocol(p, func(s network.ProtocolScope) error {
			check(s.(*protocolScope).resourceScope)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkPeer := func(p peer.ID, check func(s *resourceScope)) {
		if err := mgr.ViewPeer(p, func(s network.PeerScope) error {
			check(s.(*peerScope).resourceScope)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// open an inbound connection, using an fd
	conn, err := mgr.OpenConnection(network.DirInbound, true, dummyMA)
	if err != nil {
		t.Fatal(err)
	}

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})

	// the connection is transient, we shouldn't be able to open a second one
	if _, err := mgr.OpenConnection(network.DirInbound, true, dummyMA); err == nil {
		t.Fatal("expected OpenConnection to fail")
	}
	if _, err := mgr.OpenConnection(network.DirInbound, false, dummyMA); err == nil {
		t.Fatal("expected OpenConnection to fail")
	}

	// close it to check resources are reclaimed
	conn.Done()

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// open another inbound connection, using an fd
	conn1, err := mgr.OpenConnection(network.DirInbound, true, dummyMA)
	if err != nil {
		t.Fatal(err)
	}

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})

	// check nility of current peer scope
	if conn1.PeerScope() != nil {
		t.Fatal("peer scope should be nil")
	}

	// attach to a peer
	if err := conn1.SetPeer(peerA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 4)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// we should be able to open a second transient connection now
	conn2, err := mgr.OpenConnection(network.DirInbound, true, dummyMA)
	if err != nil {
		t.Fatal(err)
	}

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})

	// but we shouldn't be able to attach it to the same peer due to the fd limit
	if err := conn2.SetPeer(peerA); err == nil {
		t.Fatal("expected SetPeer to fail")
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 1})
	})

	// close it and reopen without using an FD -- we should be able to attach now
	conn2.Done()

	conn2, err = mgr.OpenConnection(network.DirInbound, false, dummyMA)
	if err != nil {
		t.Fatal(err)
	}

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 1, NumFD: 0})
	})

	if err := conn2.SetPeer(peerA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// open a stream
	stream, err := mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 4)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 6)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	// the stream is transient we shouldn't be able to open a second one
	if _, err := mgr.OpenStream(peerA, network.DirInbound); err == nil {
		t.Fatal("expected OpenStream to fail")
	}

	// close the stream to check resource reclamation
	stream.Done()

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// open another stream, but this time attach it to a protocol
	stream1, err := mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 4)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 6)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	// check nility of protocol scope
	if stream1.ProtocolScope() != nil {
		t.Fatal("protocol scope should be nil")
	}

	if err := stream1.SetProtocol(protoA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 4)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 7)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// and now we should be able to open another stream and attach it to the protocol
	stream2, err := mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 8)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream2.SetProtocol(protoA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 8)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// open a 3rd stream, and try to attach it to the same protocol
	stream3, err := mgr.OpenStream(peerB, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 10)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream3.SetProtocol(protoA); err == nil {
		t.Fatal("expected SetProtocol to fail")
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 10)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	// but we should be able to set to another protocol
	if err := stream3.SetProtocol(protoB); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 11)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// check nility of current service scope
	if stream1.ServiceScope() != nil {
		t.Fatal("service scope should be nil")
	}

	// we should be able to attach stream1 and stream2 to svcA, but stream3 should fail due to limit
	if err := stream1.SetService(svcA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkService(svcA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 12)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	if err := stream2.SetService(svcA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkService(svcA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 12)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	if err := stream3.SetService(svcA); err == nil {
		t.Fatal("expected SetService to fail")
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2, NumConnsInbound: 2, NumFD: 1})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkService(svcA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 12)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 3, NumConnsInbound: 2, NumFD: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// and now let's reclaim our resources to make sure we can gc unused peer and proto scopes
	// but first check internal refs
	mgr.mx.Lock()
	_, okProtoA := mgr.proto[protoA]
	_, okProtoB := mgr.proto[protoB]
	_, okPeerA := mgr.peer[peerA]
	_, okPeerB := mgr.peer[peerB]
	mgr.mx.Unlock()

	if !okProtoA {
		t.Fatal("protocol scope is not stored")
	}
	if !okProtoB {
		t.Fatal("protocol scope is not stored")
	}
	if !okPeerA {
		t.Fatal("peer scope is not stored")
	}
	if !okPeerB {
		t.Fatal("peer scope is not stored")
	}

	// ok, reclaim
	stream1.Done()
	stream2.Done()
	stream3.Done()
	conn1.Done()
	conn2.Done()

	// check everything released
	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkPeer(peerB, func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkService(svcA, func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 7)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	mgr.gc()

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	mgr.mx.Lock()
	lenProto := len(mgr.proto)
	lenPeer := len(mgr.peer)
	mgr.mx.Unlock()

	if lenProto != 0 {
		t.Fatal("protocols were not gc'ed")
	}
	if lenPeer != 0 {
		t.Fatal("perrs were not gc'ed")
	}

	// check that per protocol peer scopes work as intended
	stream1, err = mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 5)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream1.SetProtocol(protoB); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 6)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	stream2, err = mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 7)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream2.SetProtocol(protoB); err == nil {
		t.Fatal("expected SetProtocol to fail")
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 7)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	stream1.Done()
	stream2.Done()

	// check that per service peer scopes work as intended
	stream1, err = mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 6)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream1.SetProtocol(protoA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 7)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	stream2, err = mgr.OpenStream(peerA, network.DirInbound)
	if err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 8)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})

	if err := stream2.SetProtocol(protoA); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 8)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	if err := stream1.SetService(svcB); err != nil {
		t.Fatal(err)
	}

	checkPeer(peerA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkService(svcB, func(s *resourceScope) {
		checkRefCnt(s, 2)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 1})
	})
	checkProtocol(protoA, func(s *resourceScope) {
		checkRefCnt(s, 3)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 9)
		checkResources(t, &s.rc, network.ScopeStat{NumStreamsInbound: 2})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	// now we should fail to set the service for stream2 to svcB because of the service peer limit
	if err := stream2.SetService(svcB); err == nil {
		t.Fatal("expected SetService to fail")
	}

	// now release resources and check interior gc of per service peer scopes
	stream1.Done()
	stream2.Done()

	mgr.gc()

	checkSystem(func(s *resourceScope) {
		checkRefCnt(s, 4)
		checkResources(t, &s.rc, network.ScopeStat{})
	})
	checkTransient(func(s *resourceScope) {
		checkRefCnt(s, 1)
		checkResources(t, &s.rc, network.ScopeStat{})
	})

	mgr.mx.Lock()
	lenProto = len(mgr.proto)
	lenPeer = len(mgr.peer)
	mgr.mx.Unlock()

	svc := mgr.svc[svcB]
	svc.Lock()
	lenSvcPeer := len(svc.peers)
	svc.Unlock()

	if lenProto != 0 {
		t.Fatal("protocols were not gc'ed")
	}
	if lenPeer != 0 {
		t.Fatal("peers were not gc'ed")
	}
	if lenSvcPeer != 0 {
		t.Fatal("service peers were not gc'ed")
	}

}

func TestResourceManagerWithAllowlist(t *testing.T) {
	peerA := test.RandPeerIDFatal(t)

	limits := DefaultLimits.AutoScale()
	limits.system.Conns = 0
	limits.transient.Conns = 0

	baseLimit := BaseLimit{
		Conns:         2,
		ConnsInbound:  2,
		ConnsOutbound: 1,
	}
	baseLimit.Apply(limits.allowlistedSystem)
	limits.allowlistedSystem = baseLimit

	baseLimit = BaseLimit{
		Conns:         1,
		ConnsInbound:  1,
		ConnsOutbound: 1,
	}
	baseLimit.Apply(limits.allowlistedTransient)
	limits.allowlistedTransient = baseLimit

	rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithAllowlistedMultiaddrs([]multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4"),
		multiaddr.StringCast("/ip4/4.3.2.1/p2p/" + peerA.String()),
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer rcmgr.Close()

	ableToGetAllowlist := GetAllowlist(rcmgr)
	if ableToGetAllowlist == nil {
		t.Fatal("Expected to be able to get the allowlist")
	}

	// A connection comes in from a non-allowlisted ip address
	_, err = rcmgr.OpenConnection(network.DirInbound, true, multiaddr.StringCast("/ip4/1.2.3.5"))
	if err == nil {
		t.Fatalf("Expected this to fail. err=%v", err)
	}

	// A connection comes in from an allowlisted ip address
	connScope, err := rcmgr.OpenConnection(network.DirInbound, true, multiaddr.StringCast("/ip4/1.2.3.4"))
	if err != nil {
		t.Fatal(err)
	}

	err = connScope.SetPeer(test.RandPeerIDFatal(t))
	if err != nil {
		t.Fatal(err)
	}

	// A connection comes in that looks like it should be allowlisted, but then has the wrong peer id.
	connScope, err = rcmgr.OpenConnection(network.DirInbound, true, multiaddr.StringCast("/ip4/4.3.2.1"))
	if err != nil {
		t.Fatal(err)
	}

	err = connScope.SetPeer(test.RandPeerIDFatal(t))
	if err == nil {
		t.Fatalf("Expected this to fail. err=%v", err)
	}

	// A connection comes in that looks like it should be allowlisted, and it has the allowlisted peer id
	connScope, err = rcmgr.OpenConnection(network.DirInbound, true, multiaddr.StringCast("/ip4/4.3.2.1"))
	if err != nil {
		t.Fatal(err)
	}

	err = connScope.SetPeer(peerA)
	if err != nil {
		t.Fatal(err)
	}
}

// TestAllowlistAndConnLimiterPlayNice checks that the connLimiter learns about network prefix limits from the allowlist.
func TestAllowlistAndConnLimiterPlayNice(t *testing.T) {
	limits := DefaultLimits.AutoScale()
	limits.allowlistedSystem.Conns = 8
	limits.allowlistedSystem.ConnsInbound = 8
	limits.allowlistedSystem.ConnsOutbound = 8
	t.Run("IPv4", func(t *testing.T) {
		rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithAllowlistedMultiaddrs([]multiaddr.Multiaddr{
			multiaddr.StringCast("/ip4/1.2.3.0/ipcidr/24"),
		}), WithNetworkPrefixLimit([]NetworkPrefixLimit{}, []NetworkPrefixLimit{}))
		if err != nil {
			t.Fatal(err)
		}
		defer rcmgr.Close()

		// The connLimiter should have the allowlisted network prefix
		require.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].Network)

		// The connLimiter should use the limit from the allowlist
		require.Equal(t, 8, rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].ConnCount)
	})
	t.Run("IPv6", func(t *testing.T) {
		rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithAllowlistedMultiaddrs([]multiaddr.Multiaddr{
			multiaddr.StringCast("/ip6/1:2:3::/ipcidr/58"),
		}), WithNetworkPrefixLimit([]NetworkPrefixLimit{}, []NetworkPrefixLimit{}))
		if err != nil {
			t.Fatal(err)
		}
		defer rcmgr.Close()

		// The connLimiter should have the allowlisted network prefix
		require.Equal(t, netip.MustParsePrefix("1:2:3::/58"), rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV6[0].Network)

		// The connLimiter should use the limit from the allowlist
		require.Equal(t, 8, rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV6[0].ConnCount)
	})

	t.Run("IPv4 with peer ID", func(t *testing.T) {
		peer, err := test.RandPeerID()
		require.NoError(t, err)

		rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithAllowlistedMultiaddrs([]multiaddr.Multiaddr{
			multiaddr.StringCast(fmt.Sprintf("/ip4/1.2.3.0/ipcidr/24/p2p/%s", peer.String())),
		}), WithNetworkPrefixLimit([]NetworkPrefixLimit{}, []NetworkPrefixLimit{}))
		if err != nil {
			t.Fatal(err)
		}
		defer rcmgr.Close()

		// The connLimiter should have the allowlisted network prefix
		require.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].Network)

		// The connLimiter should use the limit from the allowlist
		require.Equal(t, 8, rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].ConnCount)
	})

	t.Run("Does not override if you set a limit directly", func(t *testing.T) {
		rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithAllowlistedMultiaddrs([]multiaddr.Multiaddr{
			multiaddr.StringCast("/ip4/1.2.3.0/ipcidr/24"),
		}), WithNetworkPrefixLimit([]NetworkPrefixLimit{
			{Network: netip.MustParsePrefix("1.2.3.0/24"), ConnCount: 1},
		}, []NetworkPrefixLimit{}))
		if err != nil {
			t.Fatal(err)
		}
		defer rcmgr.Close()

		// The connLimiter should have it because we set it
		require.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].Network)
		// should only have one network prefix limit
		require.Equal(t, 1, len(rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4))

		// The connLimiter should use the limit we defined explicitly
		require.Equal(t, 1, rcmgr.(*resourceManager).connLimiter.networkPrefixLimitV4[0].ConnCount)
	})
}

func TestResourceManagerRateLimiting(t *testing.T) {
	// Create a resource manager with very low rate limits
	limits := DefaultLimits.AutoScale()
	limits.system.Conns = 100 // High enough to not be the limiting factor
	limits.transient.Conns = 100

	// Create limiters with very low RPS
	limiter := &rate.Limiter{
		GlobalLimit: rate.Limit{RPS: 0.00001, Burst: 2},
	}

	rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithConnRateLimiters(limiter))
	if err != nil {
		t.Fatal(err)
	}
	defer rcmgr.Close()

	addr := multiaddr.StringCast("/ip4/1.2.3.4")

	connScope, err := rcmgr.OpenConnection(network.DirInbound, true, addr)
	require.NoError(t, err)
	connScope.Done()

	connScope, err = rcmgr.OpenConnection(network.DirInbound, true, addr)
	require.NoError(t, err)
	connScope.Done()

	_, err = rcmgr.OpenConnection(network.DirInbound, true, addr)
	require.ErrorContains(t, err, "rate limit exceeded")
}

func TestVerifySourceAddressRateLimiter(t *testing.T) {
	limits := DefaultLimits.AutoScale()
	limits.allowlistedSystem.Conns = 100
	limits.allowlistedSystem.ConnsInbound = 100
	limits.allowlistedSystem.ConnsOutbound = 100

	rcmgr, err := NewResourceManager(NewFixedLimiter(limits), WithLimitPerSubnet([]ConnLimitPerSubnet{
		{PrefixLength: 32, ConnCount: 2},
	}, []ConnLimitPerSubnet{}))
	if err != nil {
		t.Fatal(err)
	}
	defer rcmgr.Close()

	na1 := &net.UDPAddr{
		IP:   net.ParseIP("1.2.3.4"),
		Port: 1234,
	}
	require.False(t, rcmgr.VerifySourceAddress(na1))
	require.True(t, rcmgr.VerifySourceAddress(na1))

	na2 := &net.UDPAddr{
		IP:   net.ParseIP("1.2.3.5"),
		Port: 1234,
	}
	require.False(t, rcmgr.VerifySourceAddress(na2))
	require.True(t, rcmgr.VerifySourceAddress(na2))
}
