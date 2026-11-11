package autorelay_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	circuitv2_proto "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const protoIDv2 = circuitv2_proto.ProtoIDv2Hop

type mockClock struct {
	*test.MockClock
}

func (c mockClock) InstantTimer(when time.Time) autorelay.InstantTimer {
	return c.MockClock.InstantTimer(when)
}

func newMockClock() mockClock {
	return mockClock{MockClock: test.NewMockClock()}
}

var _ autorelay.ClockWithInstantTimer = mockClock{}

func numRelays(h host.Host) int {
	return len(usedRelays(h))
}

func usedRelays(h host.Host) []peer.ID {
	m := make(map[peer.ID]struct{})
	for _, addr := range h.Addrs() {
		addr, comp := ma.SplitLast(addr)
		if comp.Protocol().Code != ma.P_CIRCUIT { // not a relay addr
			continue
		}
		_, comp = ma.SplitLast(addr)
		if comp.Protocol().Code != ma.P_P2P {
			panic("expected p2p component")
		}
		id, err := peer.Decode(comp.Value())
		if err != nil {
			panic(err)
		}
		m[id] = struct{}{}
	}
	peers := make([]peer.ID, 0, len(m))
	for id := range m {
		peers = append(peers, id)
	}
	return peers
}

func newPrivateNode(t *testing.T, peerSource func(context.Context, int) <-chan peer.AddrInfo,
	opts ...autorelay.Option) host.Host {
	t.Helper()
	h, err := libp2p.New(
		libp2p.ForceReachabilityPrivate(),
		libp2p.EnableAutoRelayWithPeerSource(peerSource, opts...),
	)
	require.NoError(t, err)
	return h
}

func newPrivateNodeWithStaticRelays(t *testing.T, static []peer.AddrInfo, opts ...autorelay.Option) host.Host {
	t.Helper()
	h, err := libp2p.New(
		libp2p.ForceReachabilityPrivate(),
		libp2p.EnableAutoRelayWithStaticRelays(static, opts...),
	)
	require.NoError(t, err)
	return h
}

func newRelay(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(
		libp2p.DisableRelay(),
		libp2p.EnableRelayService(),
		libp2p.ForceReachabilityPublic(),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			for i, addr := range addrs {
				saddr := addr.String()
				if strings.HasPrefix(saddr, "/ip4/127.0.0.1/") {
					addrNoIP := strings.TrimPrefix(saddr, "/ip4/127.0.0.1")
					// .internal is classified as a public address as users
					// are free to map this dns to a public ip address for
					// use within a LAN
					addrs[i] = ma.StringCast("/dns/libp2p.internal" + addrNoIP)
				}
			}
			return addrs
		}),
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return slices.Contains(h.Mux().Protocols(), protoIDv2)
	}, time.Second, 10*time.Millisecond)
	return h
}

func TestSingleCandidate(t *testing.T) {
	var counter int
	h := newPrivateNode(t,
		func(_ context.Context, num int) <-chan peer.AddrInfo {
			counter++
			require.Equal(t, 1, num)
			peerChan := make(chan peer.AddrInfo, num)
			defer close(peerChan)
			r := newRelay(t)
			t.Cleanup(func() { r.Close() })
			peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
			return peerChan
		},
		autorelay.WithMaxCandidates(1),
		autorelay.WithNumRelays(99999),
		autorelay.WithBootDelay(0),
		autorelay.WithMinInterval(time.Hour),
	)
	defer h.Close()

	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 10*time.Second, 100*time.Millisecond)
	// test that we don't add any more relays
	require.Never(t, func() bool { return numRelays(h) > 1 }, 200*time.Millisecond, 50*time.Millisecond)
	require.Equal(t, 1, counter, "expected the peer source callback to only have been called once")
}

func TestSingleRelay(t *testing.T) {
	const numCandidates = 3
	var called bool
	peerChan := make(chan peer.AddrInfo, numCandidates)
	for range numCandidates {
		r := newRelay(t)
		t.Cleanup(func() { r.Close() })
		peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
	}
	close(peerChan)

	h := newPrivateNode(t,
		func(_ context.Context, num int) <-chan peer.AddrInfo {
			require.False(t, called, "expected the peer source callback to only have been called once")
			called = true
			require.Equal(t, numCandidates, num)
			return peerChan
		},
		autorelay.WithMaxCandidates(numCandidates),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(0),
		autorelay.WithMinInterval(time.Hour),
	)
	defer h.Close()

	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 5*time.Second, 100*time.Millisecond)
	// test that we don't add any more relays
	require.Never(t, func() bool { return numRelays(h) > 1 }, 200*time.Millisecond, 50*time.Millisecond)
}

func TestWaitForCandidates(t *testing.T) {
	peerChan := make(chan peer.AddrInfo)
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo { return peerChan },
		autorelay.WithMinCandidates(2),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(time.Hour),
		autorelay.WithMinInterval(time.Hour),
	)
	defer h.Close()

	r1 := newRelay(t)
	t.Cleanup(func() { r1.Close() })
	peerChan <- peer.AddrInfo{ID: r1.ID(), Addrs: r1.Addrs()}

	// make sure we don't add any relays yet
	// We need to wait until we have at least 2 candidates before we connect.
	require.Never(t, func() bool { return numRelays(h) > 0 }, 200*time.Millisecond, 50*time.Millisecond)

	r2 := newRelay(t)
	t.Cleanup(func() { r2.Close() })
	peerChan <- peer.AddrInfo{ID: r2.ID(), Addrs: r2.Addrs()}
	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 10*time.Second, 100*time.Millisecond)
}

func TestBackoff(t *testing.T) {
	const backoff = 20 * time.Second
	cl := newMockClock()
	r, err := libp2p.New(
		libp2p.DisableRelay(),
		libp2p.ForceReachabilityPublic(),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			for i, addr := range addrs {
				saddr := addr.String()
				if strings.HasPrefix(saddr, "/ip4/127.0.0.1/") {
					addrNoIP := strings.TrimPrefix(saddr, "/ip4/127.0.0.1")
					addrs[i] = ma.StringCast("/dns4/localhost" + addrNoIP)
				}
			}
			return addrs
		}),
	)
	require.NoError(t, err)
	defer r.Close()
	var reservations atomic.Int32
	r.SetStreamHandler(protoIDv2, func(str network.Stream) {
		defer reservations.Add(1)
		str.Close()
	})

	var counter atomic.Int32
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo {
			// always return the same node, and make sure we don't try to connect to it too frequently
			counter.Add(1)
			peerChan := make(chan peer.AddrInfo, 1)
			peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
			close(peerChan)
			return peerChan
		},
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(0),
		autorelay.WithBackoff(backoff),
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidateAge(1),
		autorelay.WithClock(cl),
		autorelay.WithMinInterval(0),
	)
	defer h.Close()

	require.Eventually(t, func() bool {
		return reservations.Load() == 1
	}, 3*time.Second, 20*time.Millisecond, "reservations load should be 1")
	// We need to wait

	cl.AdvanceBy(1) // Increment the time a little so we can make another peer source call
	require.Eventually(t, func() bool {
		// The reservation will fail, and autorelay will ask the peer source for
		// more candidates.  Wait until it does so, this way we know that client
		// knows the relay connection has failed before we advance the time.
		return counter.Load() > 1
	}, 2*time.Second, 100*time.Millisecond, "counter load should be 2")

	// make sure we don't add any relays yet
	for range 2 {
		cl.AdvanceBy(backoff / 3)
		require.Equal(t, 1, int(reservations.Load()))
	}
	cl.AdvanceBy(backoff)
	require.Eventually(t, func() bool {
		return reservations.Load() == 2
	}, 3*time.Second, 100*time.Millisecond, "reservations load should be 2")
	require.Less(t, int(counter.Load()), 10) // just make sure we're not busy-looping
	require.Equal(t, 2, int(reservations.Load()))
}

func TestRemovePeerFromBackoffAfterSuccess(t *testing.T) {
	const backoff = 20 * time.Second
	cl := newMockClock()

	rh := newRelay(t)

	var counter atomic.Int32
	h, err := libp2p.New(
		libp2p.ForceReachabilityPrivate(),
	)
	require.NoError(t, err)
	defer h.Close()

	ar, err := autorelay.NewAutoRelay(h,
		autorelay.WithPeerSource(
			func(context.Context, int) <-chan peer.AddrInfo {
				// always return the same node, and make sure we don't try to connect to it too frequently
				counter.Add(1)
				peerChan := make(chan peer.AddrInfo, 1)
				peerChan <- peer.AddrInfo{ID: rh.ID(), Addrs: rh.Addrs()}
				close(peerChan)
				return peerChan
			}),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(0),
		autorelay.WithBackoff(backoff),
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidateAge(1),
		autorelay.WithClock(cl),
		autorelay.WithMinInterval(0),
	)
	require.NoError(t, err)
	ar.Start()
	defer ar.Close()

	require.Eventually(t, func() bool {
		return numRelays(h) > 0
	}, 5*time.Second, 100*time.Millisecond, "should successfully reserve relay")

	require.False(t, ar.IsPeerInBackoff(rh.ID()), "successfully added relay should not be in backoff list")
}

func TestStaticRelays(t *testing.T) {
	const numStaticRelays = 3
	staticRelays := make([]peer.AddrInfo, 0, numStaticRelays)
	for range numStaticRelays {
		r := newRelay(t)
		t.Cleanup(func() { r.Close() })
		staticRelays = append(staticRelays, peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()})
	}

	h := newPrivateNodeWithStaticRelays(t,
		staticRelays,
		autorelay.WithNumRelays(1),
	)
	defer h.Close()

	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 10*time.Second, 50*time.Millisecond)
}

func TestConnectOnDisconnect(t *testing.T) {
	const num = 3
	peerChan := make(chan peer.AddrInfo, num)
	relays := make([]host.Host, 0, num)
	for range 3 {
		r := newRelay(t)
		t.Cleanup(func() { r.Close() })
		peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
		relays = append(relays, r)
	}
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo { return peerChan },
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidates(num),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(0),
		autorelay.WithMinInterval(time.Hour),
	)
	defer h.Close()

	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 10*time.Second, 100*time.Millisecond)
	relaysInUse := usedRelays(h)
	require.Len(t, relaysInUse, 1)
	oldRelay := relaysInUse[0]

	for _, r := range relays {
		if r.ID() == oldRelay {
			r.Close()
		}
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		relaysInUse = usedRelays(h)
		require.Len(collect, relaysInUse, 1)
		assert.NotEqualf(collect, oldRelay, relaysInUse[0], "old relay should not be used again")
	}, 10*time.Second, 100*time.Millisecond)
}

func TestMaxAge(t *testing.T) {
	cl := newMockClock()

	const num = 4
	peerChan1 := make(chan peer.AddrInfo, num)
	peerChan2 := make(chan peer.AddrInfo, num)
	relays1 := make([]host.Host, 0, num)
	relays2 := make([]host.Host, 0, num)
	for range num {
		r1 := newRelay(t)
		t.Cleanup(func() { r1.Close() })
		peerChan1 <- peer.AddrInfo{ID: r1.ID(), Addrs: r1.Addrs()}
		relays1 = append(relays1, r1)
		r2 := newRelay(t)
		t.Cleanup(func() { r2.Close() })
		relays2 = append(relays2, r2)
	}
	close(peerChan1)
	peerChans := make(chan chan peer.AddrInfo, 2)
	peerChans <- peerChan1
	peerChans <- peerChan2
	close(peerChans)

	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo {
			c, ok := <-peerChans
			if !ok {
				t.Fatal("unexpected call to PeerSource")
			}
			return c
		},
		autorelay.WithNumRelays(1),
		autorelay.WithMaxCandidates(100),
		autorelay.WithBootDelay(0),
		autorelay.WithMaxCandidateAge(20*time.Minute),
		autorelay.WithClock(cl),
		autorelay.WithMinInterval(30*time.Second),
	)
	defer h.Close()

	require.Eventually(t, func() bool {
		return numRelays(h) > 0
	}, 10*time.Second, 100*time.Millisecond)
	relays := usedRelays(h)
	require.Len(t, relays, 1)

	cl.AdvanceBy(time.Minute)
	require.Eventually(t, func() bool {
		return len(peerChans) == 0
	}, 10*time.Second, 100*time.Millisecond)

	cl.AdvanceBy(10 * time.Minute)
	for _, r := range relays2 {
		peerChan2 <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
	}
	cl.AdvanceBy(11 * time.Minute)

	require.Eventually(t, func() bool {
		relays = usedRelays(h)
		return len(relays) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// by now the 3 relays should have been garbage collected
	// And we should only be using a single relay. Lets close it.
	var oldRelay peer.ID
	for _, r := range relays1 {
		if r.ID() == relays[0] {
			oldRelay = r.ID()
			r.Close()
		}
	}
	require.NotEmpty(t, oldRelay)

	require.Eventually(t, func() bool {
		relays = usedRelays(h)
		if len(relays) != 1 {
			return false
		}
		return relays[0] != oldRelay
	}, 10*time.Second, 100*time.Millisecond)

	require.Len(t, relays, 1)
	ids := make([]peer.ID, 0, len(relays2))
	for _, r := range relays2 {
		ids = append(ids, r.ID())
	}

	require.Eventually(t, func() bool {
		if slices.Contains(ids, relays[0]) {
			return true
		}
		fmt.Println("waiting for", ids, "to contain", relays[0])
		return false
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t, ids, relays[0])
}

func TestReconnectToStaticRelays(t *testing.T) {
	cl := newMockClock()
	const numStaticRelays = 1
	staticRelays := make([]peer.AddrInfo, 0, numStaticRelays)
	relays := make([]host.Host, 0, numStaticRelays)
	for range numStaticRelays {
		r := newRelay(t)
		t.Cleanup(func() { r.Close() })
		relays = append(relays, r)
		staticRelays = append(staticRelays, peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()})
	}

	h := newPrivateNodeWithStaticRelays(t,
		staticRelays,
		autorelay.WithClock(cl),
		autorelay.WithBackoff(30*time.Minute),
	)
	defer h.Close()

	cl.AdvanceBy(time.Minute)
	require.Eventually(t, func() bool {
		return numRelays(h) == 1
	}, 10*time.Second, 100*time.Millisecond)

	relaysInUse := usedRelays(h)
	oldRelay := relaysInUse[0]
	for _, r := range relays {
		if r.ID() == oldRelay {
			r.Network().ClosePeer(h.ID())
		}
	}
	require.Eventually(t, func() bool {
		return numRelays(h) == 0
	}, 10*time.Second, 100*time.Millisecond)

	cl.AdvanceBy(time.Hour)
	require.Eventually(t, func() bool {
		return numRelays(h) == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func TestMinInterval(t *testing.T) {
	cl := newMockClock()
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo {
			peerChan := make(chan peer.AddrInfo, 1)
			defer close(peerChan)
			r1 := newRelay(t)
			t.Cleanup(func() { r1.Close() })
			peerChan <- peer.AddrInfo{ID: r1.ID(), Addrs: r1.Addrs()}
			return peerChan
		},
		autorelay.WithClock(cl),
		autorelay.WithMinCandidates(2),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(time.Hour),
		autorelay.WithMinInterval(500*time.Millisecond),
	)
	defer h.Close()

	cl.AdvanceBy(400 * time.Millisecond)
	// The second call to peerSource should happen after 1 second
	require.Never(t, func() bool { return numRelays(h) > 0 }, 500*time.Millisecond, 100*time.Millisecond)
	cl.AdvanceBy(600 * time.Millisecond)
	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 3*time.Second, 100*time.Millisecond)
}

func TestNoBusyLoop0MinInterval(t *testing.T) {
	var calledTimes atomic.Uint64
	cl := newMockClock()
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo {
			calledTimes.Add(1)
			peerChan := make(chan peer.AddrInfo, 1)
			defer close(peerChan)
			r1 := newRelay(t)
			t.Cleanup(func() { r1.Close() })
			peerChan <- peer.AddrInfo{ID: r1.ID(), Addrs: r1.Addrs()}
			return peerChan
		},
		autorelay.WithClock(cl),
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidates(1),
		autorelay.WithNumRelays(0),
		autorelay.WithBootDelay(time.Hour),
		autorelay.WithMinInterval(time.Millisecond),
	)
	defer h.Close()

	require.Never(t, func() bool {
		cl.AdvanceBy(time.Second)
		val := calledTimes.Load()
		return val >= 2
	}, 500*time.Millisecond, 100*time.Millisecond)
	val := calledTimes.Load()
	require.Less(t, val, uint64(2))
}
func TestAutoRelayAddrsEvent(t *testing.T) {
	cl := newMockClock()
	relays := []host.Host{newRelay(t), newRelay(t), newRelay(t), newRelay(t), newRelay(t)}
	t.Cleanup(func() {
		for _, r := range relays {
			r.Close()
		}
	})

	relayIDFromP2PAddr := func(a ma.Multiaddr) peer.ID {
		r, c := ma.SplitLast(a)
		if c.Protocol().Code != ma.P_CIRCUIT {
			return ""
		}
		if id, err := peer.IDFromP2PAddr(r); err == nil {
			return id
		}
		return ""
	}

	checkAddrsContainsPeersAsRelay := func(addrs []ma.Multiaddr, peers ...peer.ID) bool {
		for _, p := range peers {
			if !slices.ContainsFunc(addrs, func(a ma.Multiaddr) bool { return relayIDFromP2PAddr(a) == p }) {
				return false
			}
		}
		return true
	}
	peerChan := make(chan peer.AddrInfo, 5)
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo {
			return peerChan
		},
		autorelay.WithClock(cl),
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidates(10),
		autorelay.WithNumRelays(5),
		autorelay.WithBootDelay(1*time.Second),
		autorelay.WithMinInterval(time.Hour),
	)
	defer h.Close()

	sub, err := h.EventBus().Subscribe(new(event.EvtAutoRelayAddrsUpdated))
	require.NoError(t, err)

	peerChan <- peer.AddrInfo{ID: relays[0].ID(), Addrs: relays[0].Addrs()}
	cl.AdvanceBy(time.Second)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		e := <-sub.Out()
		evt := e.(event.EvtAutoRelayAddrsUpdated)
		if !checkAddrsContainsPeersAsRelay(evt.RelayAddrs, relays[0].ID()) {
			collect.Errorf("expected %s to be in %v", relays[0].ID(), evt.RelayAddrs)
		}
		if checkAddrsContainsPeersAsRelay(evt.RelayAddrs, relays[1].ID()) {
			collect.Errorf("expected %s to not be in %v", relays[1].ID(), evt.RelayAddrs)
		}
	}, 5*time.Second, 50*time.Millisecond)
	for _, r := range relays[1:] {
		peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		e := <-sub.Out()
		evt := e.(event.EvtAutoRelayAddrsUpdated)
		relayIds := make([]peer.ID, 0, len(relays)-1)
		for _, r := range relays[1:] {
			relayIds = append(relayIds, r.ID())
		}
		if !checkAddrsContainsPeersAsRelay(evt.RelayAddrs, relayIds...) {
			c.Errorf("expected %s to be in %v", relayIds, evt.RelayAddrs)
		}
	}, 5*time.Second, 50*time.Millisecond)
	select {
	case e := <-sub.Out():
		t.Fatal("expected no more events after all reservations obtained; got: ", e.(event.EvtAutoRelayAddrsUpdated))
	case <-time.After(1 * time.Second):
	}
}

func TestConnectOnRefreshFailure(t *testing.T) {
	cl := newMockClock()
	// Reservation expirations come from the relay's wall clock, so start the mock clock there.
	cl.AdvanceBy(time.Since(cl.Now()))

	const num = 3
	peerChan := make(chan peer.AddrInfo, num)
	relays := make([]host.Host, 0, num)
	for range num {
		r := newRelay(t)
		t.Cleanup(func() { r.Close() })
		peerChan <- peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
		relays = append(relays, r)
	}
	h := newPrivateNode(t,
		func(context.Context, int) <-chan peer.AddrInfo { return peerChan },
		autorelay.WithMinCandidates(1),
		autorelay.WithMaxCandidates(num),
		autorelay.WithNumRelays(1),
		autorelay.WithBootDelay(0),
		autorelay.WithMinInterval(time.Hour),
		autorelay.WithMaxCandidateAge(2*time.Hour),
		autorelay.WithClock(cl),
	)
	defer h.Close()

	require.Eventually(t, func() bool { return numRelays(h) > 0 }, 10*time.Second, 100*time.Millisecond)
	relaysInUse := usedRelays(h)
	require.Len(t, relaysInUse, 1)
	oldRelay := relaysInUse[0]

	// The old relay stays connected but stops serving reservations, e.g. because
	// its relay service was disabled. The next reservation refresh fails.
	for _, r := range relays {
		if r.ID() == oldRelay {
			r.RemoveStreamHandler(protoIDv2)
		}
	}
	// Reservations are valid for 1 hour and refreshed 2 minutes before expiry.
	for range 65 {
		cl.AdvanceBy(time.Minute)
		time.Sleep(10 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		return !slices.Contains(usedRelays(h), oldRelay)
	}, 10*time.Second, 100*time.Millisecond, "refresh with the old relay should have failed")
	require.Equal(t, network.Connected, h.Network().Connectedness(oldRelay))

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		relaysInUse = usedRelays(h)
		require.Len(collect, relaysInUse, 1)
		assert.NotEqualf(collect, oldRelay, relaysInUse[0], "old relay should not be used again")
	}, 10*time.Second, 100*time.Millisecond, "should have reserved a slot with one of the remaining candidates")
}
