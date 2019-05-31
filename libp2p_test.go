package libp2p

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-tcp-transport"
	ma "github.com/multiformats/go-multiaddr"
)

func TestNewHost(t *testing.T) {
	h, err := makeRandomHost(t, 9000)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
}

func TestBadTransportConstructor(t *testing.T) {
	ctx := context.Background()
	h, err := New(ctx, Transport(func() {}))
	if err == nil {
		h.Close()
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "libp2p_test.go") {
		t.Error("expected error to contain debugging info")
	}
}

func TestNoListenAddrs(t *testing.T) {
	ctx := context.Background()
	h, err := New(ctx, NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if len(h.Addrs()) != 0 {
		t.Fatal("expected no addresses")
	}
}

func TestNoTransports(t *testing.T) {
	ctx := context.Background()
	a, err := New(ctx, NoTransports)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b, err := New(ctx, ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	err = a.Connect(ctx, peer.AddrInfo{
		ID:    b.ID(),
		Addrs: b.Addrs(),
	})
	if err == nil {
		t.Error("dial should have failed as no transports have been configured")
	}
}

func TestInsecure(t *testing.T) {
	ctx := context.Background()
	h, err := New(ctx, NoSecurity)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
}

func TestPipeTransport(t *testing.T) {
	ctx := context.Background()
	memAddr, err := ma.NewMultiaddr("/memory/1")
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(ctx, NoSecurity, EnableSelfDial, ListenAddrs(memAddr))
	if err != nil {
		t.Fatal(err)
	}
	info := peer.AddrInfo{
		ID:    h.ID(),
		Addrs: []ma.Multiaddr{memAddr},
	}
	err = h.Connect(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	donech := make(chan struct{})
	h.SetStreamHandler(protocol.TestingID, func(s network.Stream) {
		n, err := s.Write([]byte{0x01, 0x02, 0x03})
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected 3 bytes, wrote %d", n)
		}
		donech <- struct{}{}
	})
	streamContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	s, err := h.NewStream(streamContext, h.ID(), protocol.TestingID)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	n, err := s.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3 bytes, wrote %d", n)
	}
	for i := byte(1); i < 4; i++ {
		if buf[int(i)-1] != i {
			t.Fatalf("bytes didn't match: %x vs %x", buf[int(i)-1], i)
		}
	}
	s.Close()
	h.Close()
}

func TestPipeTransportAcrossHosts(t *testing.T) {
	ctx := context.Background()
	memAddr1, err := ma.NewMultiaddr("/memory/1")
	if err != nil {
		t.Fatal(err)
	}
	h1, err := New(ctx, NoSecurity, EnableSelfDial, ListenAddrs(memAddr1))
	if err != nil {
		t.Fatal(err)
	}
	memAddr2, err := ma.NewMultiaddr("/memory/2")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := New(ctx, NoSecurity, EnableSelfDial, ListenAddrs(memAddr2))
	if err != nil {
		t.Fatal(err)
	}
	info2 := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: []ma.Multiaddr{memAddr2},
	}
	err = h1.Connect(ctx, info2)
	if err != nil {
		t.Fatal(err)
	}
	donech := make(chan struct{})
	h2.SetStreamHandler(protocol.TestingID, func(s network.Stream) {
		n, err := s.Write([]byte{0x01, 0x02, 0x03})
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected 3 bytes, wrote %d", n)
		}
		donech <- struct{}{}
	})
	streamContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	s, err := h1.NewStream(streamContext, h2.ID(), protocol.TestingID)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	n, err := s.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3 bytes, wrote %d", n)
	}
	for i := byte(1); i < 4; i++ {
		if buf[int(i)-1] != i {
			t.Fatalf("bytes didn't match: %x vs %x", buf[int(i)-1], i)
		}
	}
	s.Close()
}

func TestDefaultListenAddrs(t *testing.T) {
	ctx := context.Background()

	re := regexp.MustCompile("/(ip)[4|6]/((0.0.0.0)|(::))/tcp/")
	re2 := regexp.MustCompile("/p2p-circuit")

	// Test 1: Setting the correct listen addresses if userDefined.Transport == nil && userDefined.ListenAddrs == nil
	h, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range h.Network().ListenAddresses() {
		if re.FindStringSubmatchIndex(addr.String()) == nil &&
			re2.FindStringSubmatchIndex(addr.String()) == nil {
			t.Error("expected ip4 or ip6 or relay interface")
		}
	}

	h.Close()

	// Test 2: Listen addr only include relay if user defined transport is passed.
	h, err = New(
		ctx,
		Transport(tcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(h.Network().ListenAddresses()) != 1 {
		t.Error("expected one listen addr with user defined transport")
	}
	if re2.FindStringSubmatchIndex(h.Network().ListenAddresses()[0].String()) == nil {
		t.Error("expected relay address")
	}
	h.Close()
}

func makeRandomHost(t *testing.T, port int) (host.Host, error) {
	ctx := context.Background()
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option{
		ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)),
		Identity(priv),
		DefaultTransports,
		DefaultMuxers,
		DefaultSecurity,
		NATPortMap(),
	}

	return New(ctx, opts...)
}

func TestChainOptions(t *testing.T) {
	var cfg Config
	var optsRun []int
	optcount := 0
	newOpt := func() Option {
		index := optcount
		optcount++
		return func(c *Config) error {
			optsRun = append(optsRun, index)
			return nil
		}
	}

	if err := cfg.Apply(newOpt(), nil, ChainOptions(newOpt(), newOpt(), ChainOptions(), ChainOptions(nil, newOpt()))); err != nil {
		t.Fatal(err)
	}

	// Make sure we ran all options.
	if optcount != 4 {
		t.Errorf("expected to have handled %d options, handled %d", optcount, len(optsRun))
	}

	// Make sure we ran the options in-order.
	for i, x := range optsRun {
		if i != x {
			t.Errorf("expected opt %d, got opt %d", i, x)
		}
	}
}
