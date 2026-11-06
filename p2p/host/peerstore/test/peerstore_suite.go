package test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var peerstoreSuite = map[string]func(pstore.Peerstore) func(*testing.T){
	"AddrStream":               testAddrStream,
	"GetStreamBeforePeerAdded": testGetStreamBeforePeerAdded,
	"AddStreamDuplicates":      testAddrStreamDuplicates,
	"PeerstoreProtoStore":      testPeerstoreProtoStore,
	"BasicPeerstore":           testBasicPeerstore,
	"Metadata":                 testMetadata,
	"CertifiedAddrBook":        testCertifiedAddrBook,
}

type PeerstoreFactory func() (pstore.Peerstore, func())

func TestPeerstore(t *testing.T, factory PeerstoreFactory) {
	for name, test := range peerstoreSuite {
		// Create a new peerstore.
		ps, closeFunc := factory()

		// Run the test.
		t.Run(name, test(ps))

		// Cleanup.
		if closeFunc != nil {
			closeFunc()
		}
	}
}

func sortProtos(protos []protocol.ID) {
	slices.Sort(protos)
}

func testAddrStream(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 100), peer.ID("testpeer")
		ps.AddAddrs(pid, addrs[:10], time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		addrch := ps.AddrStream(ctx, pid)

		// while that subscription is active, publish ten more addrs
		// this tests that it doesnt hang
		for i := 10; i < 20; i++ {
			ps.AddAddr(pid, addrs[i], time.Hour)
		}

		// now receive them (without hanging)
		timeout := time.After(time.Second * 10)
		for range 20 {
			select {
			case <-addrch:
			case <-timeout:
				t.Fatal("timed out")
			}
		}

		// start a second stream
		ctx2, cancel2 := context.WithCancel(context.Background())
		addrch2 := ps.AddrStream(ctx2, pid)

		done := make(chan struct{})
		go func() {
			defer close(done)
			// now send the rest of the addresses
			for _, a := range addrs[20:80] {
				ps.AddAddr(pid, a, time.Hour)
			}
		}()

		// receive some concurrently with the goroutine
		timeout = time.After(time.Second * 10)
		for range 40 {
			select {
			case <-addrch:
			case <-timeout:
			}
		}

		<-done

		// receive some more after waiting for that goroutine to complete
		timeout = time.After(time.Second * 10)
		for range 20 {
			select {
			case <-addrch:
			case <-timeout:
			}
		}

		// now cancel it
		cancel()

		// now check the *second* subscription. We should see 80 addresses.
		for range 80 {
			<-addrch2
		}

		cancel2()

		// and add a few more addresses it doesnt hang afterwards
		for _, a := range addrs[80:] {
			ps.AddAddr(pid, a, time.Hour)
		}
	}
}

func testGetStreamBeforePeerAdded(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 10), peer.ID("testpeer")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ach := ps.AddrStream(ctx, pid)
		for i := range 10 {
			ps.AddAddr(pid, addrs[i], time.Hour)
		}

		received := make(map[string]bool)
		var count int

		for range 10 {
			a, ok := <-ach
			if !ok {
				t.Fatal("channel shouldnt be closed yet")
			}
			if a == nil {
				t.Fatal("got a nil address, that's weird")
			}
			count++
			if received[a.String()] {
				t.Fatal("received duplicate address")
			}
			received[a.String()] = true
		}

		select {
		case <-ach:
			t.Fatal("shouldnt have received any more addresses")
		default:
		}

		if count != 10 {
			t.Fatal("should have received exactly ten addresses, got ", count)
		}

		for _, a := range addrs {
			if !received[a.String()] {
				t.Log(received)
				t.Fatalf("expected to receive address %s but didnt", a)
			}
		}
	}
}

func testAddrStreamDuplicates(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 10), peer.ID("testpeer")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ach := ps.AddrStream(ctx, pid)
		go func() {
			for i := range 10 {
				ps.AddAddr(pid, addrs[i], time.Hour)
				ps.AddAddr(pid, addrs[rand.Intn(10)], time.Hour)
			}

			// make sure that all addresses get processed before context is cancelled
			time.Sleep(time.Millisecond * 50)
			cancel()
		}()

		received := make(map[string]bool)
		var count int
		for a := range ach {
			if a == nil {
				t.Fatal("got a nil address, that's weird")
			}
			count++
			if received[a.String()] {
				t.Fatal("received duplicate address")
			}
			received[a.String()] = true
		}

		if count != 10 {
			t.Fatal("should have received exactly ten addresses")
		}
	}
}

func testPeerstoreProtoStore(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("adding and removing protocols", func(t *testing.T) {
			p1 := peer.ID("TESTPEER")
			protos := []protocol.ID{"a", "b", "c", "d"}

			require.NoError(t, ps.AddProtocols(p1, protos...))
			out, err := ps.GetProtocols(p1)
			require.NoError(t, err)
			require.Len(t, out, len(protos), "got wrong number of protocols back")

			sortProtos(out)
			for i, p := range protos {
				if out[i] != p {
					t.Fatal("got wrong protocol")
				}
			}

			supported, err := ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
			require.NoError(t, err)
			require.Len(t, supported, 2, "only expected 2 supported")

			if supported[0] != "a" || supported[1] != "b" {
				t.Fatal("got wrong supported array: ", supported)
			}

			b, err := ps.FirstSupportedProtocol(p1, "q", "w", "a", "y", "b")
			require.NoError(t, err)
			require.Equal(t, protocol.ID("a"), b)

			b, err = ps.FirstSupportedProtocol(p1, "q", "x", "z")
			require.NoError(t, err)
			require.Empty(t, b)

			b, err = ps.FirstSupportedProtocol(p1, "a")
			require.NoError(t, err)
			require.Equal(t, protocol.ID("a"), b)

			protos = []protocol.ID{"other", "yet another", "one more"}
			require.NoError(t, ps.SetProtocols(p1, protos...))

			supported, err = ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
			require.NoError(t, err)
			require.Empty(t, supported, "none of those protocols should have been supported")

			supported, err = ps.GetProtocols(p1)
			require.NoError(t, err)

			sortProtos(supported)
			sortProtos(protos)
			if !reflect.DeepEqual(supported, protos) {
				t.Fatalf("expected previously set protos; expected: %v, have: %v", protos, supported)
			}

			require.NoError(t, ps.RemoveProtocols(p1, protos[:2]...))

			supported, err = ps.GetProtocols(p1)
			require.NoError(t, err)
			if !reflect.DeepEqual(supported, protos[2:]) {
				t.Fatal("expected only one protocol to remain")
			}
		})

		t.Run("removing peer", func(t *testing.T) {
			p := peer.ID("foobar")
			protos := []protocol.ID{"a", "b"}

			require.NoError(t, ps.SetProtocols(p, protos...))
			out, err := ps.GetProtocols(p)
			require.NoError(t, err)
			require.Len(t, out, 2)
			ps.RemovePeer(p)
			out, err = ps.GetProtocols(p)
			require.NoError(t, err)
			require.Empty(t, out)
		})
	}
}

func testBasicPeerstore(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs := getAddrs(t, 10)
		pids := make([]peer.ID, 0, len(addrs))

		for _, a := range addrs {
			priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
			if err != nil {
				t.Fatal(err)
			}
			p, err := peer.IDFromPrivateKey(priv)
			if err != nil {
				t.Fatal(err)
			}
			pids = append(pids, p)
			ps.AddAddr(p, a, pstore.PermanentAddrTTL)
		}

		peers := ps.Peers()
		if len(peers) != 10 {
			t.Fatal("expected ten peers, got", len(peers))
		}

		pinfo := ps.PeerInfo(pids[0])
		if !pinfo.Addrs[0].Equal(addrs[0]) {
			t.Fatal("stored wrong address")
		}
	}
}

func testMetadata(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("putting and getting", func(t *testing.T) {
			pids := make([]peer.ID, 3)
			for i := range pids {
				priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
				require.NoError(t, err)
				p, err := peer.IDFromPrivateKey(priv)
				require.NoError(t, err)
				pids[i] = p
			}
			for _, p := range pids {
				require.NoError(t, ps.Put(p, "AgentVersion", "string"), "failed to put AgentVersion")
				require.NoError(t, ps.Put(p, "bar", 1), "failed to put bar")
			}
			for _, p := range pids {
				v, err := ps.Get(p, "AgentVersion")
				require.NoError(t, err)
				require.Equal(t, "string", v)

				v, err = ps.Get(p, "bar")
				require.NoError(t, err)
				require.Equal(t, 1, v)
			}
		})

		t.Run("removing a peer", func(t *testing.T) {
			p := peer.ID("foo")
			otherP := peer.ID("foobar")
			require.NoError(t, ps.Put(otherP, "AgentVersion", "v1"))
			require.NoError(t, ps.Put(p, "AgentVersion", "v1"))
			require.NoError(t, ps.Put(p, "bar", 1))
			ps.RemovePeer(p)
			_, err := ps.Get(p, "AgentVersion")
			require.ErrorIs(t, err, pstore.ErrNotFound)
			_, err = ps.Get(p, "bar")
			require.ErrorIs(t, err, pstore.ErrNotFound)
			// make sure that entries for otherP were not deleted
			val, err := ps.Get(otherP, "AgentVersion")
			require.NoError(t, err)
			require.Equal(t, "v1", val)
		})
	}
}

func testCertifiedAddrBook(ps pstore.Peerstore) func(*testing.T) {
	return func(t *testing.T) {
		_, ok := ps.(pstore.CertifiedAddrBook)
		if !ok {
			t.Error("expected peerstore to implement CertifiedAddrBook interface")
		}
	}
}

func getAddrs(t *testing.T, n int) []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, 0, n)
	for i := range n {
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", i))
		if err != nil {
			t.Fatal(err)
		}

		addrs = append(addrs, a)
	}
	return addrs
}

func TestPeerstoreProtoStoreLimits(t *testing.T, ps pstore.Peerstore, limit int) {
	p := peer.ID("foobar")
	protocols := make([]protocol.ID, limit)
	for i := range limit {
		protocols[i] = protocol.ID(fmt.Sprintf("protocol %d", i))
	}

	t.Run("setting protocols", func(t *testing.T) {
		require.NoError(t, ps.SetProtocols(p, protocols...))
		require.EqualError(t, ps.SetProtocols(p, append(protocols, "proto")...), "too many protocols")
	})
	t.Run("adding protocols", func(t *testing.T) {
		p1 := protocols[:limit/2]
		p2 := protocols[limit/2:]
		require.NoError(t, ps.SetProtocols(p, p1...))
		require.NoError(t, ps.AddProtocols(p, p2...))
		require.EqualError(t, ps.AddProtocols(p, "proto"), "too many protocols")
	})
	t.Run("setting duplicate protocols", func(t *testing.T) {
		duplicates := append(append([]protocol.ID{}, protocols...), protocols...)
		require.NoError(t, ps.SetProtocols(p, duplicates...))
		stored, err := ps.GetProtocols(p)
		require.NoError(t, err)
		require.ElementsMatch(t, protocols, stored)
	})
	t.Run("adding existing protocols at the limit", func(t *testing.T) {
		require.NoError(t, ps.SetProtocols(p, protocols...))
		require.NoError(t, ps.AddProtocols(p, protocols...))
		stored, err := ps.GetProtocols(p)
		require.NoError(t, err)
		require.ElementsMatch(t, protocols, stored)
	})
	t.Run("adding duplicate new protocols", func(t *testing.T) {
		require.NoError(t, ps.SetProtocols(p, protocols[:limit-1]...))
		require.NoError(t, ps.AddProtocols(p, protocols[0], protocols[limit-1], protocols[limit-1]))
		stored, err := ps.GetProtocols(p)
		require.NoError(t, err)
		require.ElementsMatch(t, protocols, stored)
	})
	t.Run("rejected addition preserves protocols", func(t *testing.T) {
		initial := protocols[:limit-1]
		require.NoError(t, ps.SetProtocols(p, initial...))
		require.EqualError(t, ps.AddProtocols(p, protocols[limit-1], "extra"), "too many protocols")
		stored, err := ps.GetProtocols(p)
		require.NoError(t, err)
		require.ElementsMatch(t, initial, stored)
	})
}
