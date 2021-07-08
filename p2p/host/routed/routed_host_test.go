package routedhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p-peer"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

// Test router which implements the Routing interface.
// This makes it easy to manipulate which peers are available
// during testing.
type TestRouter struct {
	addrs map[peer.ID]peerstore.PeerInfo
}

func (router TestRouter) FindPeer(ctx context.Context, pid peer.ID) (peerstore.PeerInfo, error) {
	peerInfo, ok := router.addrs[pid]

	if ok {
		return peerInfo, nil
	}

	return peerInfo, errors.New("Could not find peer")
}

func (router *TestRouter) insertPeerInfo(pid peer.ID, pinfo peerstore.PeerInfo) {
	router.addrs[pid] = pinfo
}

// TestConnectWithOldAddrs tests if a routed hosts uses its router
// to attempt find a functional addrs for a peer if the current addresses
// do not work. Old address refers to the address no longer being correct
// for a given peer.
func TestConnectWithOldAddrs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host1, err := libp2p.New(ctx)
	if err != nil {
		t.Fatal(err)
	}

	host2, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/15001"))
	if err != nil {
		t.Fatal(err)
	}

	testRouter := TestRouter{make(map[peer.ID]peerstore.PeerInfo)}
	routedHost1 := Wrap(host1, testRouter)

	// Add the second hosts multiaddrs to the first and third hosts peerstore.
	routedHost1.Peerstore().SetAddrs(host2.ID(), host2.Addrs(), time.Hour)

	// Check that the hosts can connect, since if they cannot connect in the first place
	// then something else is wrong.
	if err := routedHost1.Connect(ctx, routedHost1.Peerstore().PeerInfo(host2.ID())); err != nil {
		t.Fatal(err)
	}

	routedHost1.Network().ClosePeer(host2.ID())

	// Now we set the addr for host2 in routedhost1's peerstore to an
	// incorrect addr. This will force routedhost1 to search the router
	// for an addr that works.
	routedHost1.Peerstore().ClearAddrs(host2.ID())
	incorrectAddr, _ := ma.NewMultiaddr("/ip4/127.1.0.1/tcp/1701")
	routedHost1.Peerstore().SetAddr(host2.ID(), incorrectAddr, time.Hour)

	// Before adding the correct addr for host2 to the testrouter we check that the hosts cannot
	// connect as routedhost1 should not have access to a correct addr for host2.
	if err := routedHost1.Connect(ctx, routedHost1.Peerstore().PeerInfo(host2.ID())); err == nil {
		t.Fatal(err)
	}

	// The peerinfo for host2 is added to the testrouter which should allow
	// routedhost1 to find host2 through the testrouter.
	testRouter.insertPeerInfo(host2.ID(), host2.Peerstore().PeerInfo(host2.ID()))

	// Now the first host should connect with the second host by getting the correct multiaddrs from the
	// third hosts peerstore.
	if err := routedHost1.Connect(ctx, routedHost1.Peerstore().PeerInfo(host2.ID())); err != nil {
		t.Fatal(err)
	}
}

// TestConnectWithNoAddrs tests that a routed host uses the router
// to search for addresses if the suppplied peerinfo and the routed
// host's own peerstore do not contain any addresses for the
// peer that it is attempting a connection with.
func TestConnectWithNoAddrs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host1, err := libp2p.New(ctx)
	if err != nil {
		t.Fatal(err)
	}

	host2, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/15001"))
	if err != nil {
		t.Fatal(err)
	}

	testRouter := TestRouter{make(map[peer.ID]peerstore.PeerInfo)}
	routedHost1 := Wrap(host1, testRouter)

	// Attempt to connect where routedhost1 does not have the peerinfo for host2
	// and the router doesn't either, so the connection should fail.
	if err := routedHost1.Connect(ctx, peerstore.PeerInfo{ID: host2.ID()}); err == nil {
		t.Fatal(errors.New("The routedhost should not be able to connect at this stage."))
	}

	// Add host2's peerinfo to the router.
	testRouter.insertPeerInfo(host2.ID(), peerstore.PeerInfo{ID: host2.ID(), Addrs: host2.Addrs()})

	// Attempt to connect again, this time the router has the peerinfo for host2
	// so the connection should succeed.
	if err := routedHost1.Connect(ctx, host1.Peerstore().PeerInfo(host2.ID())); err != nil {
		t.Fatal(err)
	}
}
