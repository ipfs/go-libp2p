package pstoremem

import (
	"container/heap"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	mockClock "github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestPeerAddrsNextExpiry(t *testing.T) {
	paa := newPeerAddrs()
	pa := &paa
	a1 := ma.StringCast("/ip4/1.2.3.4/udp/1/quic-v1")
	a2 := ma.StringCast("/ip4/1.2.3.4/udp/2/quic-v1")

	// t1 is before t2
	t1 := time.Time{}.Add(1 * time.Second)
	t2 := time.Time{}.Add(2 * time.Second)
	paa.Insert(&expiringAddr{Addr: a1, Expiry: t1, TTL: 10 * time.Second, Peer: "p1"})
	paa.Insert(&expiringAddr{Addr: a2, Expiry: t2, TTL: 10 * time.Second, Peer: "p2"})

	if pa.NextExpiry() != t1 {
		t.Fatal("expiry should be set to t1, got", pa.NextExpiry())
	}
}

func peerAddrsInput(n int) []*expiringAddr {
	expiringAddrs := make([]*expiringAddr, n)
	for i := range n {
		port := i % 65535
		a := ma.StringCast(fmt.Sprintf("/ip4/1.2.3.4/udp/%d/quic-v1", port))
		e := time.Time{}.Add(time.Duration(i) * time.Second)
		p := peer.ID(fmt.Sprintf("p%d", i))
		expiringAddrs[i] = &expiringAddr{Addr: a, Expiry: e, TTL: 10 * time.Second, Peer: p}
	}
	return expiringAddrs
}

func TestPeerAddrsHeapProperty(t *testing.T) {
	paa := newPeerAddrs()
	pa := &paa

	const N = 10000
	expiringAddrs := peerAddrsInput(N)
	for i := range N {
		paa.Insert(expiringAddrs[i])
	}

	for i := range N {
		ea, ok := pa.PopIfExpired(expiringAddrs[i].Expiry)
		require.True(t, ok, "pos: %d", i)
		require.Equal(t, ea.Addr, expiringAddrs[i].Addr)

		ea, ok = pa.PopIfExpired(expiringAddrs[i].Expiry)
		require.False(t, ok)
		require.Nil(t, ea)
	}
}

func TestPeerAddrsHeapPropertyDeletions(t *testing.T) {
	paa := newPeerAddrs()
	pa := &paa

	const N = 10000
	expiringAddrs := peerAddrsInput(N)
	for i := range N {
		paa.Insert(expiringAddrs[i])
	}

	// delete every 3rd element
	for i := 0; i < N; i += 3 {
		paa.Delete(expiringAddrs[i])
	}

	for i := range N {
		ea, ok := pa.PopIfExpired(expiringAddrs[i].Expiry)
		if i%3 == 0 {
			require.False(t, ok)
			require.Nil(t, ea)
		} else {
			require.True(t, ok)
			require.Equal(t, ea.Addr, expiringAddrs[i].Addr)
		}

		ea, ok = pa.PopIfExpired(expiringAddrs[i].Expiry)
		require.False(t, ok)
		require.Nil(t, ea)
	}
}

func TestPeerAddrsHeapPropertyUpdates(t *testing.T) {
	paa := newPeerAddrs()
	pa := &paa

	const N = 10000
	expiringAddrs := peerAddrsInput(N)
	for i := range N {
		heap.Push(pa, expiringAddrs[i])
	}

	// update every 3rd element to expire at the end
	var endElements []ma.Multiaddr
	for i := 0; i < N; i += 3 {
		expiringAddrs[i].Expiry = time.Time{}.Add(1000_000 * time.Second)
		pa.Update(expiringAddrs[i])
		endElements = append(endElements, expiringAddrs[i].Addr)
	}

	for i := range N {
		if i%3 == 0 {
			continue // skip the elements at the end
		}
		ea, ok := pa.PopIfExpired(expiringAddrs[i].Expiry)
		require.True(t, ok, "pos: %d", i)
		require.Equal(t, ea.Addr, expiringAddrs[i].Addr)

		ea, ok = pa.PopIfExpired(expiringAddrs[i].Expiry)
		require.False(t, ok)
		require.Nil(t, ea)
	}

	for len(endElements) > 0 {
		ea, ok := pa.PopIfExpired(time.Time{}.Add(1000_000 * time.Second))
		require.True(t, ok)
		require.Contains(t, endElements, ea.Addr)
		endElements = slices.DeleteFunc(endElements, func(a ma.Multiaddr) bool { return ea.Addr.Equal(a) })
	}
}

// TestPeerAddrsExpiry tests for multiple element expiry with PopIfExpired.
func TestPeerAddrsExpiry(t *testing.T) {
	const T = 100_000
	for range T {
		paa := newPeerAddrs()
		pa := &paa
		// Try a lot of random inputs.
		// T > 5*((5^5)*5) (=15k)
		// So this should test for all possible 5 element inputs.
		const N = 5
		expiringAddrs := peerAddrsInput(N)
		for i := range N {
			expiringAddrs[i].Expiry = time.Time{}.Add(time.Duration(1+rand.Intn(N)) * time.Second)
		}
		for i := range N {
			pa.Insert(expiringAddrs[i])
		}

		expiry := time.Time{}.Add(time.Duration(1+rand.Intn(N)) * time.Second)
		expected := []ma.Multiaddr{}
		for i := range N {
			if !expiry.Before(expiringAddrs[i].Expiry) {
				expected = append(expected, expiringAddrs[i].Addr)
			}
		}
		got := []ma.Multiaddr{}
		for {
			ea, ok := pa.PopIfExpired(expiry)
			if !ok {
				break
			}
			got = append(got, ea.Addr)
		}
		expiries := make([]int, 0, N)
		for i := range N {
			expiries = append(expiries, expiringAddrs[i].Expiry.Second())
		}
		require.ElementsMatch(t, expected, got, "failed for input: element expiries: %v, expiry: %v", expiries, expiry.Second())
	}
}

func TestPeerLimits(t *testing.T) {
	ab := NewAddrBook()
	defer ab.Close()
	ab.maxUnconnectedAddrs = 1024

	peers := peerAddrsInput(2048)
	for _, p := range peers {
		ab.AddAddr(p.Peer, p.Addr, p.TTL)
	}
	require.Equal(t, 1024, ab.addrs.NumUnconnectedAddrs())
}

// TestMaxAddrsPerPeerEvictsNearestExpiry verifies the per-peer cap evicts
// the stored addr with the soonest expiry first, not just the oldest insert.
func TestMaxAddrsPerPeerEvictsNearestExpiry(t *testing.T) {
	ab := NewAddrBook(WithMaxAddressesPerPeer(3))
	defer ab.Close()

	const p = peer.ID("peer-cap")
	a1 := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	a2 := ma.StringCast("/ip4/1.2.3.4/tcp/2")
	a3 := ma.StringCast("/ip4/1.2.3.4/tcp/3")
	a4 := ma.StringCast("/ip4/1.2.3.4/tcp/4")

	ab.AddAddr(p, a1, time.Hour)      // furthest expiry
	ab.AddAddr(p, a2, 30*time.Minute) // middle
	ab.AddAddr(p, a3, 10*time.Minute) // nearest expiry, will be evicted first
	require.ElementsMatch(t, []ma.Multiaddr{a1, a2, a3}, ab.Addrs(p))

	// Adding a fourth addr forces eviction; a3 (nearest expiry) must go.
	ab.AddAddr(p, a4, 45*time.Minute)
	require.ElementsMatch(t, []ma.Multiaddr{a1, a2, a4}, ab.Addrs(p))
}

// TestMaxAddrsPerPeerEnforcedOnSetAddrs verifies the per-peer cap fires on
// the SetAddrs path too, not only AddAddr.
func TestMaxAddrsPerPeerEnforcedOnSetAddrs(t *testing.T) {
	ab := NewAddrBook(WithMaxAddressesPerPeer(2))
	defer ab.Close()

	const p = peer.ID("peer-setaddrs")
	a1 := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	a2 := ma.StringCast("/ip4/1.2.3.4/tcp/2")
	a3 := ma.StringCast("/ip4/1.2.3.4/tcp/3")

	ab.AddAddr(p, a1, time.Hour)      // furthest expiry
	ab.AddAddr(p, a2, 10*time.Minute) // nearest expiry, eviction target
	require.ElementsMatch(t, []ma.Multiaddr{a1, a2}, ab.Addrs(p))

	// SetAddrs with a new addr hits the cap; nearest-expiry a2 must go.
	ab.SetAddrs(p, []ma.Multiaddr{a3}, 30*time.Minute)
	require.ElementsMatch(t, []ma.Multiaddr{a1, a3}, ab.Addrs(p))
}

// TestMaxAddrsPerPeerDoesNotEvictConnected verifies that addrs held by live
// connections (TTL >= ConnectedAddrTTL) are neither counted toward the cap
// nor eligible for eviction.
func TestMaxAddrsPerPeerDoesNotEvictConnected(t *testing.T) {
	ab := NewAddrBook(WithMaxAddressesPerPeer(2))
	defer ab.Close()

	const p = peer.ID("peer-connected")
	live := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	a1 := ma.StringCast("/ip4/1.2.3.4/tcp/2")
	a2 := ma.StringCast("/ip4/1.2.3.4/tcp/3")
	a3 := ma.StringCast("/ip4/1.2.3.4/tcp/4")

	// Pin one addr via ConnectedAddrTTL. It should not count toward the cap.
	ab.AddAddr(p, live, peerstore.ConnectedAddrTTL)
	ab.AddAddr(p, a1, 10*time.Minute)
	ab.AddAddr(p, a2, 20*time.Minute)
	require.ElementsMatch(t, []ma.Multiaddr{live, a1, a2}, ab.Addrs(p))

	// Adding a third unconnected addr must evict an unconnected one
	// (a1 has the soonest expiry), never the connected addr.
	ab.AddAddr(p, a3, 30*time.Minute)
	require.ElementsMatch(t, []ma.Multiaddr{live, a2, a3}, ab.Addrs(p))
}

// TestConsumePeerRecordReplacesStaleAddrs verifies replace-semantics on a
// newer signed peer record: addrs dropped from the new record are evicted,
// while unsigned addrs and addrs held by a live connection are kept.
func TestConsumePeerRecordReplacesStaleAddrs(t *testing.T) {
	ab := NewAddrBook()
	defer ab.Close()

	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	keep := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	drop := ma.StringCast("/ip4/1.2.3.4/tcp/2")
	unsigned := ma.StringCast("/ip4/1.2.3.4/tcp/3")
	connected := ma.StringCast("/ip4/1.2.3.4/tcp/4")

	rec1 := peer.NewPeerRecord()
	rec1.PeerID = id
	rec1.Seq = 1
	rec1.Addrs = []ma.Multiaddr{keep, drop, connected}
	env1, err := record.Seal(rec1, priv)
	require.NoError(t, err)

	accepted, err := ab.ConsumePeerRecord(env1, time.Hour)
	require.NoError(t, err)
	require.True(t, accepted)

	// Pin `connected` via ConnectedAddrTTL and add an unsigned addr.
	ab.AddAddr(id, connected, peerstore.ConnectedAddrTTL)
	ab.AddAddr(id, unsigned, time.Hour)
	require.ElementsMatch(t, []ma.Multiaddr{keep, drop, connected, unsigned}, ab.Addrs(id))

	// Newer record drops `drop` and only mentions `keep`. `drop` must go;
	// `unsigned` (never in a signed record) and `connected` (held by a
	// live connection) must stay.
	rec2 := peer.NewPeerRecord()
	rec2.PeerID = id
	rec2.Seq = 2
	rec2.Addrs = []ma.Multiaddr{keep}
	env2, err := record.Seal(rec2, priv)
	require.NoError(t, err)

	accepted, err = ab.ConsumePeerRecord(env2, time.Hour)
	require.NoError(t, err)
	require.True(t, accepted)

	require.ElementsMatch(t, []ma.Multiaddr{keep, connected, unsigned}, ab.Addrs(id))
}

// TestConnectedAddrDowngradedToFiniteTTLIsCollected covers what identify and
// gossipsub do when a peer's last connection closes: addrs stored with
// ConnectedAddrTTL are moved to a finite TTL. The addr book must then expire
// and collect them, and the signed peer record with them, or a node keeps
// an entry for every peer it ever connected to.
func TestConnectedAddrDowngradedToFiniteTTLIsCollected(t *testing.T) {
	clk := mockClock.NewMock()
	ab := NewAddrBook(WithClock(clk))
	defer ab.Close()

	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	addr := ma.StringCast("/ip4/1.2.3.4/tcp/1")
	rec := peer.NewPeerRecord()
	rec.PeerID = id
	rec.Addrs = []ma.Multiaddr{addr}
	env, err := record.Seal(rec, priv)
	require.NoError(t, err)

	for name, downgrade := range map[string]func(){
		"UpdateAddrs": func() {
			ab.UpdateAddrs(id, peerstore.ConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
		},
		"SetAddrs": func() {
			ab.SetAddrs(id, []ma.Multiaddr{addr}, peerstore.RecentlyConnectedAddrTTL)
		},
	} {
		t.Run(name, func(t *testing.T) {
			accepted, err := ab.ConsumePeerRecord(env, peerstore.ConnectedAddrTTL)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Equal(t, 0, ab.addrs.NumUnconnectedAddrs())

			downgrade()
			require.Equal(t, 1, ab.addrs.NumUnconnectedAddrs(), "a finite-TTL addr must be queued for expiry")

			clk.Add(peerstore.RecentlyConnectedAddrTTL + time.Second)
			ab.gc()
			require.Empty(t, ab.Addrs(id))
			require.Empty(t, ab.addrs.Addrs, "expired addrs must be collected")
			require.Nil(t, ab.GetPeerRecord(id), "a peer with no addrs must not keep its signed record")
		})
	}
}

func BenchmarkPeerAddrs(b *testing.B) {
	sizes := [...]int{1, 10, 100, 1000, 10_000, 100_000, 1000_000}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				paa := newPeerAddrs()
				pa := &paa
				expiringAddrs := peerAddrsInput(sz)
				for i := range sz {
					pa.Insert(expiringAddrs[i])
				}
				b.StartTimer()
				for {
					_, ok := pa.PopIfExpired(expiringAddrs[len(expiringAddrs)-1].Expiry)
					if !ok {
						break
					}
				}
			}
		})
	}

}
