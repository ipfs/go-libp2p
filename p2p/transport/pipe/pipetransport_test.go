package pipetransport

import (
	"context"
	"fmt"
	"testing"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	peertest "github.com/libp2p/go-libp2p-peer/test"
	utils "github.com/libp2p/go-libp2p-transport/test"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPipeTransport(t *testing.T) {
	privKey, pubKey, err := peertest.RandTestKeyPair(512)
	if err != nil {
		t.Fatal()
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		t.Fatal(err)
	}
	transport := New(id, pubKey, privKey)
	listenAddrStr := fmt.Sprintf("/memory/1")
	listenAddr, _ := ma.NewMultiaddr(listenAddrStr)
	listener, err := transport.Listen(listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		stream, err := conn.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		n, err := stream.Write([]byte("sup"))
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected to write 3 bytes, wrote %d", n)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	conn, err := transport.Dial(ctx, listenAddr, id)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := conn.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 10)
	n, err := stream.Read(buf)
	if n != 3 {
		t.Fatal("expected 3 bytes, got", n)
	}
	if err != nil {
		t.Fatal(err)
	}
	bufStr := string(buf[0:n])
	if bufStr != "sup" {
		t.Fatal("unexpected message: ", bufStr)
	}
}

// Note: the stress tests don't work because they rely on multiple listeners
// being able to bind the same address, usually by requesting TCP port 0 from
// the kernel. This doesn't apply for the pipe transport. The pipe transport
// supports one per peer ID. Furthermore, the cancel test doesn't apply, since
// there is no latency when opening pipe transports.
func TestPipeTransportFull(t *testing.T) {
	privKey, pubKey, err := peertest.RandTestKeyPair(512)
	if err != nil {
		t.Fatal()
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		t.Fatal(err)
	}
	transport := New(id, pubKey, privKey)
	listenAddr, _ := ma.NewMultiaddr("/memory/1")
	utils.SubtestBasic(t, transport, transport, listenAddr, id)
	utils.SubtestProtocols(t, transport, transport, listenAddr, id)
	utils.SubtestPingPong(t, transport, transport, listenAddr, id)
	utils.SubtestStreamReset(t, transport, transport, listenAddr, id)
}
