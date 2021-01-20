package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

type DiscoveryNotifee struct {
	h host.Host
}

func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	n.h.Connect(context.Background(), pi)
}

func TestMdnsDiscovery(t *testing.T) {
	//TODO: re-enable when the new lib will get integrated
	t.Skip("TestMdnsDiscovery fails randomly with current lib")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := bhost.New(swarmt.GenSwarm(t, ctx))
	b := bhost.New(swarmt.GenSwarm(t, ctx))

	interval := func(opts *MdnsServiceInit) {
		opts.Interval = time.Second
	}
	tag := func(opts *MdnsServiceInit) {
		opts.Tag = "someTag"
	}
	// Not necessary, default value being an empty string anyway
	ifaceName := func(opts *MdnsServiceInit) {
		opts.IfaceName = ""
	}

	sa, err := NewMdnsService(ctx, a, interval, tag, ifaceName)
	if err != nil {
		t.Fatal(err)
	}

	// Here we don't add the ifaceName function
	sb, err := NewMdnsService(ctx, b, interval, tag)
	if err != nil {
		t.Fatal(err)
	}

	_ = sb

	n := &DiscoveryNotifee{a}

	sa.RegisterNotifee(n)

	time.Sleep(time.Second * 2)

	err = a.Connect(ctx, peer.AddrInfo{ID: b.ID()})
	if err != nil {
		t.Fatal(err)
	}
}
