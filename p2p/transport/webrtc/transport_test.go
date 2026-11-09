package libp2pwebrtc

import (
	"context"
	"crypto/rand"
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tpt "github.com/libp2p/go-libp2p/core/transport"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	quicproxy "github.com/quic-go/quic-go/integrationtests/tools/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var netListenUDP ListenUDPFn = func(network string, laddr *net.UDPAddr) (net.PacketConn, error) {
	return net.ListenUDP(network, laddr)
}

func getTransport(t *testing.T, opts ...Option) (*WebRTCTransport, peer.ID) {
	t.Helper()
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)
	rcmgr := &network.NullResourceManager{}
	transport, err := New(privKey, nil, nil, rcmgr, netListenUDP, opts...)
	require.NoError(t, err)
	peerID, err := peer.IDFromPrivateKey(privKey)
	require.NoError(t, err)
	t.Cleanup(func() { rcmgr.Close() })
	return transport, peerID
}

func TestNullRcmgrTransport(t *testing.T) {
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)
	transport, err := New(privKey, nil, nil, nil, netListenUDP)
	require.NoError(t, err)

	listenTransport, pid := getTransport(t)
	ln, err := listenTransport.Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct"))
	require.NoError(t, err)
	go func() {
		c, err := ln.Accept()
		if !assert.NoError(t, err) {
			t.Error(err)
		}
		t.Cleanup(func() { c.Close() })
	}()
	c, err := transport.Dial(context.Background(), ln.Multiaddr(), pid)
	require.NoError(t, err)
	c.Close()
}

func TestIsWebRTCDirectMultiaddr(t *testing.T) {
	invalid := []string{
		"/ip4/1.2.3.4/tcp/10/",
		"/ip6/1::3/udp/100/quic-v1/",
		"/ip4/1.2.3.4/udp/1/quic-v1/webrtc-direct",
	}

	valid := []struct {
		addr  string
		count int
	}{
		{
			addr:  "/ip4/1.2.3.4/udp/1234/webrtc-direct",
			count: 0,
		},
		{
			addr:  "/dns/test.test/udp/1234/webrtc-direct",
			count: 0,
		},
		{
			addr:  "/ip4/1.2.3.4/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
			count: 1,
		},
		{
			addr:  "/ip6/0:0:0:0:0:0:0:1/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
			count: 1,
		},
		{
			addr:  "/dns/test.test/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
			count: 1,
		},
		{
			addr:  "/dns/test.test/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7ZGrV4VZ3hpEKTd_zg",
			count: 2,
		},
	}

	for _, addr := range invalid {
		a := ma.StringCast(addr)
		isValid, n := IsWebRTCDirectMultiaddr(a)
		require.Equal(t, 0, n)
		require.False(t, isValid)
	}

	for _, tc := range valid {
		a := ma.StringCast(tc.addr)
		isValid, n := IsWebRTCDirectMultiaddr(a)
		require.Equal(t, tc.count, n)
		require.True(t, isValid)
	}
}

func TestTransportWebRTC_CanDial(t *testing.T) {
	tr, _ := getTransport(t)
	invalid := []string{
		"/ip4/1.2.3.4/udp/1234/webrtc-direct",
		"/dns/test.test/udp/1234/webrtc-direct",
	}

	valid := []string{
		"/ip4/1.2.3.4/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
		"/ip6/0:0:0:0:0:0:0:1/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
		"/ip6/::1/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
		"/dns/test.test/udp/1234/webrtc-direct/certhash/uEiAsGPzpiPGQzSlVHRXrUCT5EkTV7YFrV4VZ3hpEKTd_zg",
	}

	for _, addr := range invalid {
		a := ma.StringCast(addr)
		require.False(t, tr.CanDial(a))
	}

	for _, addr := range valid {
		a := ma.StringCast(addr)
		require.True(t, tr.CanDial(a), addr)
	}
}

func TestTransportAddCertHasher(t *testing.T) {
	tr, _ := getTransport(t)
	addrs := []string{
		"/ip4/1.2.3.4/udp/1/webrtc-direct",
		"/ip6/1::3/udp/2/webrtc-direct",
	}
	for _, a := range addrs {
		addr, added := tr.AddCertHashes(ma.StringCast(a))
		require.True(t, added)
		_, err := addr.ValueForProtocol(ma.P_CERTHASH)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(addr.String(), a))
	}
}

func TestTransportWebRTC_ListenFailsOnNonWebRTCMultiaddr(t *testing.T) {
	tr, _ := getTransport(t)
	testAddrs := []string{
		"/ip4/0.0.0.0/udp/0",
		"/ip4/0.0.0.0/tcp/0/wss",
	}
	for _, addr := range testAddrs {
		listenMultiaddr, err := ma.NewMultiaddr(addr)
		require.NoError(t, err)
		listener, err := tr.Listen(listenMultiaddr)
		require.Error(t, err)
		require.Nil(t, listener)
	}
}

// using assert inside goroutines, refer: https://github.com/stretchr/testify/issues/772#issuecomment-945166599
func TestTransportWebRTC_DialFailsOnUnsupportedHashFunction(t *testing.T) {
	tr, _ := getTransport(t)
	hash := hash.Hash(sha3.New512())
	certhash := func() string {
		_, err := hash.Write([]byte("test-data"))
		require.NoError(t, err)
		mh, err := multihash.Encode(hash.Sum([]byte{}), multihash.SHA3_512)
		require.NoError(t, err)
		certhash, err := multibase.Encode(multibase.Base58BTC, mh)
		require.NoError(t, err)
		return certhash
	}()
	testaddr, err := ma.NewMultiaddr("/ip4/1.2.3.4/udp/1234/webrtc-direct/certhash/" + certhash)
	require.NoError(t, err)
	_, err = tr.Dial(context.Background(), testaddr, "")
	require.ErrorContains(t, err, "unsupported hash function")
}

func TestTransportWebRTC_CanListenSingle(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	tr1, connectingPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")

	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		_, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		assert.NoError(t, err)
		close(done)
	}()

	conn, err := listener.Accept()
	require.NoError(t, err)
	require.NotNil(t, conn)

	require.Equal(t, connectingPeer, conn.RemotePeer())
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.FailNow()
	}
}

// TestTransportWebRTC_v2Dial runs the v2 (no SDP munging) dial flow end to end
// against the standard listener: a full ICE + DTLS + Noise handshake plus a
// two-way stream. The listener detects v2 from the ICE username fragment prefix
// with no extra configuration.
func TestTransportWebRTC_v2Dial(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	tr1, connectingPeer := getTransport(t, WithDialerVersion(2))
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	streamChan := make(chan network.MuxedStream)
	go func() {
		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		assert.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		stream, err := conn.AcceptStream()
		assert.NoError(t, err)
		streamChan <- stream
	}()

	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, connectingPeer, conn.RemotePeer())

	stream, err := conn.OpenStream(context.Background())
	require.NoError(t, err)
	_, err = stream.Write([]byte("test"))
	require.NoError(t, err)

	var str network.MuxedStream
	select {
	case str = <-streamChan:
	case <-time.After(5 * time.Second):
		t.Fatal("stream opening timed out")
	}
	buf := make([]byte, 100)
	str.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := str.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "test", string(buf[:n]))
}

// TestTransportWebRTC_ListenerAcceptsBothVersions confirms a single listener
// accepts both v1 and v2 dialers concurrently, dispatching on the ICE username
// fragment prefix.
func TestTransportWebRTC_ListenerAcceptsBothVersions(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			client, connectingPeer := getTransport(t, WithDialerVersion(version))
			done := make(chan struct{})
			go func() {
				conn, err := client.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
				assert.NoError(t, err)
				if conn != nil {
					t.Cleanup(func() { conn.Close() })
				}
				close(done)
			}()

			conn, err := listener.Accept()
			require.NoError(t, err)
			defer conn.Close()
			require.Equal(t, connectingPeer, conn.RemotePeer())
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("dial timed out")
			}
		})
	}
}

// WithDialerVersion requires an explicit, known version: it accepts 1/2 and
// rejects 0 or unknown values rather than silently defaulting.
func TestWithDialerVersion(t *testing.T) {
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)

	for _, version := range []int{1, 2} {
		_, err := New(privKey, nil, nil, &network.NullResourceManager{}, netListenUDP, WithDialerVersion(version))
		require.NoError(t, err, "version %d should be accepted", version)
	}

	for _, version := range []int{0, 3} {
		_, err := New(privKey, nil, nil, &network.NullResourceManager{}, netListenUDP, WithDialerVersion(version))
		require.Error(t, err, "version %d should be rejected", version)
	}
}

// An unknown dialer version is rejected at dial time, not treated as v1.
func TestTransportWebRTC_DialerRejectsUnknownVersion(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listener, err := tr.Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct"))
	require.NoError(t, err)
	defer listener.Close()

	client, _ := getTransport(t)
	// set directly to simulate a future/unknown version (WithDialerVersion would
	// reject it earlier); the dial must error rather than assume v1
	client.dialerVersion = 3
	_, err = client.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
	require.ErrorContains(t, err, "unsupported WebRTC Direct dialer version")
}

// WithListenerMaxInFlightConnections sets the maximum number of connections that are in-flight, i.e
// they are being negotiated, or are waiting to be accepted.
func WithListenerMaxInFlightConnections(m uint32) Option {
	return func(t *WebRTCTransport) error {
		if m == 0 {
			t.maxInFlightConnections = DefaultMaxInFlightConnections
		} else {
			t.maxInFlightConnections = m
		}
		return nil
	}
}

func TestTransportWebRTC_CanListenMultiple(t *testing.T) {
	count := 3
	tr, listeningPeer := getTransport(t, WithListenerMaxInFlightConnections(uint32(count)))

	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	go func() {
		for range count {
			conn, err := listener.Accept()
			assert.NoError(t, err)
			assert.NotNil(t, conn)
			defer conn.Close()
		}
		wg.Wait()
		cancel()
	}()

	for range count {
		wg.Go(func() {
			ctr, _ := getTransport(t)
			conn, err := ctr.Dial(ctx, listener.Multiaddr(), listeningPeer)
			select {
			case <-ctx.Done():
			default:
				assert.NoError(t, err)
				assert.NotNil(t, conn)
				t.Cleanup(func() { conn.Close() })
			}
		})
	}

	select {
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out")
	}
}

func TestTransportWebRTC_CanCreateSuccessiveConnections(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	count := 2

	var wg sync.WaitGroup
	wg.Add(count)
	go func() {
		for range count {
			ctr, _ := getTransport(t)
			conn, err := ctr.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
			require.NoError(t, err)
			require.Equal(t, conn.RemotePeer(), listeningPeer)
			t.Cleanup(func() { conn.Close() })
			wg.Done()
		}
	}()

	for range count {
		conn, err := listener.Accept()
		require.NoError(t, err)
		defer conn.Close()
	}
	wg.Wait()
}

func TestTransportWebRTC_ListenerCanCreateStreams(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	tr1, connectingPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	streamChan := make(chan network.MuxedStream)
	go func() {
		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		t.Logf("connection opened by dialer")

		stream, err := conn.AcceptStream()
		require.NoError(t, err)
		t.Logf("dialer accepted stream")
		streamChan <- stream
	}()

	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, connectingPeer, conn.RemotePeer())
	t.Logf("listener accepted connection")

	stream, err := conn.OpenStream(context.Background())
	require.NoError(t, err)
	t.Logf("listener opened stream")
	_, err = stream.Write([]byte("test"))
	require.NoError(t, err)

	var str network.MuxedStream
	select {
	case str = <-streamChan:
	case <-time.After(3 * time.Second):
		t.Fatal("stream opening timed out")
	}
	buf := make([]byte, 100)
	stream.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := str.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "test", string(buf[:n]))
}

func TestTransportWebRTC_DialerCanCreateStreams(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, connectingPeer := getTransport(t)
	done := make(chan struct{})

	go func() {
		lconn, err := listener.Accept()
		require.NoError(t, err)
		require.Equal(t, connectingPeer, lconn.RemotePeer())
		defer lconn.Close()

		stream, err := lconn.AcceptStream()
		require.NoError(t, err)
		buf := make([]byte, 100)
		n, err := stream.Read(buf)
		require.NoError(t, err)
		require.Equal(t, "test", string(buf[:n]))

		close(done)
	}()

	go func() {
		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		require.NoError(t, err)
		defer conn.Close()
		t.Logf("dialer opened connection")
		stream, err := conn.OpenStream(context.Background())
		require.NoError(t, err)
		t.Logf("dialer opened stream")
		_, err = stream.Write([]byte("test"))
		require.NoError(t, err)
		<-done
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
	}
}

func TestTransportWebRTC_DialerCanCreateStreamsMultiple(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, connectingPeer := getTransport(t)
	readerDone := make(chan struct{})

	const (
		numListeners = 10
		numStreams   = 100
		numWriters   = 10
		size         = 20 << 10
	)

	go func() {
		lconn, err := listener.Accept()
		require.NoError(t, err)
		require.Equal(t, connectingPeer, lconn.RemotePeer())
		defer lconn.Close()
		var wg sync.WaitGroup
		var doneStreams atomic.Int32
		for range numListeners {
			wg.Go(func() {
				for {
					var nn int32
					if nn = doneStreams.Add(1); nn > int32(numStreams) {
						return
					}
					s, err := lconn.AcceptStream()
					require.NoError(t, err)
					n, err := io.Copy(s, s)
					require.Equal(t, n, int64(size))
					require.NoError(t, err)
					s.Close()
				}
			})
		}
		wg.Wait()
		readerDone <- struct{}{}
	}()

	conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
	require.NoError(t, err)
	defer conn.Close()

	var writerWG sync.WaitGroup
	var cnt atomic.Int32
	var streamsStarted atomic.Int32
	for range numWriters {
		writerWG.Go(func() {
			buf := make([]byte, size)
			for {
				var nn int32
				if nn = streamsStarted.Add(1); nn > int32(numStreams) {
					return
				}
				rand.Read(buf)

				s, err := conn.OpenStream(context.Background())
				require.NoError(t, err)
				n, err := s.Write(buf)
				require.Equal(t, n, size)
				require.NoError(t, err)
				s.CloseWrite()
				resp := make([]byte, size+10)
				n, err = io.ReadFull(s, resp)
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				require.Equal(t, n, size)
				if string(buf) != string(resp[:size]) {
					t.Errorf("bytes not equal: %d %d", len(buf), len(resp))
				}
				s.Close()
				t.Log("completed stream: ", cnt.Add(1), s.(*stream).id)
			}
		})
	}
	writerWG.Wait()
	select {
	case <-readerDone:
	case <-time.After(100 * time.Second):
		t.Fatal("timed out")
	}
}

func TestTransportWebRTC_Deadline(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()
	tr1, connectingPeer := getTransport(t)

	t.Run("SetReadDeadline", func(t *testing.T) {
		go func() {
			lconn, err := listener.Accept()
			require.NoError(t, err)
			t.Cleanup(func() { lconn.Close() })
			require.Equal(t, connectingPeer, lconn.RemotePeer())
			_, err = lconn.AcceptStream()
			require.NoError(t, err)
		}()

		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		require.NoError(t, err)
		defer conn.Close()
		stream, err := conn.OpenStream(context.Background())
		require.NoError(t, err)

		// deadline set to the past
		stream.SetReadDeadline(time.Now().Add(-200 * time.Millisecond))
		_, err = stream.Read([]byte{0, 0})
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)

		// future deadline exceeded
		stream.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = stream.Read([]byte{0, 0})
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	})

	t.Run("SetWriteDeadline", func(t *testing.T) {
		go func() {
			lconn, err := listener.Accept()
			require.NoError(t, err)
			t.Cleanup(func() { lconn.Close() })
			require.Equal(t, connectingPeer, lconn.RemotePeer())
			_, err = lconn.AcceptStream()
			require.NoError(t, err)
		}()

		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		require.NoError(t, err)
		defer conn.Close()
		stream, err := conn.OpenStream(context.Background())
		require.NoError(t, err)

		stream.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		largeBuffer := make([]byte, 20*1024*1024)
		_, err = stream.Write(largeBuffer)
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)

		stream.SetWriteDeadline(time.Now().Add(-200 * time.Millisecond))
		smallBuffer := make([]byte, 1024)
		_, err = stream.Write(smallBuffer)
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	})
}

func TestTransportWebRTC_StreamWriteBufferContention(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, connectingPeer := getTransport(t)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		lconn, err := listener.Accept()
		require.NoError(t, err)
		t.Cleanup(func() { lconn.Close() })
		require.Equal(t, connectingPeer, lconn.RemotePeer())
		for range 2 {
			go func() {
				defer wg.Done()
				_, err := lconn.AcceptStream()
				require.NoError(t, err)
			}()
		}
	}()

	conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
	require.NoError(t, err)
	defer conn.Close()

	errC := make(chan error)
	// writers
	for range 2 {
		go func() {
			stream, err := conn.OpenStream(context.Background())
			require.NoError(t, err)

			stream.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
			largeBuffer := make([]byte, 2*1024*1024)
			_, err = stream.Write(largeBuffer)
			errC <- err
		}()
	}

	require.ErrorIs(t, <-errC, os.ErrDeadlineExceeded)
	require.ErrorIs(t, <-errC, os.ErrDeadlineExceeded)
	wg.Wait()
}

func TestTransportWebRTC_RemoteReadsAfterClose(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, _ := getTransport(t)

	done := make(chan error)
	go func() {
		lconn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		t.Cleanup(func() { lconn.Close() })

		stream, err := lconn.AcceptStream()
		if err != nil {
			done <- err
			return
		}
		_, err = stream.Write([]byte{1, 2, 3, 4})
		if err != nil {
			done <- err
			return
		}
		err = stream.Close()
		if err != nil {
			done <- err
			return
		}
		close(done)
	}()

	conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
	require.NoError(t, err)
	defer conn.Close()
	// create a stream
	stream, err := conn.OpenStream(context.Background())

	require.NoError(t, err)
	// require write and close to complete
	require.NoError(t, <-done)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 10)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
}

func TestTransportWebRTC_RemoteReadsAfterClose2(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, _ := getTransport(t)

	awaitStreamClosure := make(chan struct{})
	readBytesResult := make(chan int)
	done := make(chan error)
	go func() {
		lconn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer lconn.Close()
		stream, err := lconn.AcceptStream()
		if err != nil {
			done <- err
			return
		}

		<-awaitStreamClosure
		buf := make([]byte, 16)
		n, err := stream.Read(buf)
		if err != nil {
			done <- err
			return
		}
		readBytesResult <- n
		close(done)
	}()

	conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
	require.NoError(t, err)
	defer conn.Close()
	// create a stream
	stream, err := conn.OpenStream(context.Background())
	require.NoError(t, err)
	_, err = stream.Write([]byte{1, 2, 3, 4})
	require.NoError(t, err)
	err = stream.Close()
	require.NoError(t, err)
	// signal stream closure
	close(awaitStreamClosure)
	require.Equal(t, 4, <-readBytesResult)
}

func TestTransportWebRTC_Close(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	listener, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer listener.Close()

	tr1, connectingPeer := getTransport(t)

	t.Run("RemoteClosesStream", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Go(func() {
			lconn, err := listener.Accept()
			require.NoError(t, err)
			t.Cleanup(func() { lconn.Close() })
			require.Equal(t, connectingPeer, lconn.RemotePeer())
			stream, err := lconn.AcceptStream()
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
			_ = stream.Close()
		})

		buf := make([]byte, 2)

		conn, err := tr1.Dial(context.Background(), listener.Multiaddr(), listeningPeer)
		require.NoError(t, err)
		defer conn.Close()
		stream, err := conn.OpenStream(context.Background())
		require.NoError(t, err)

		err = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
		require.NoError(t, err)
		_, err = stream.Read(buf)
		require.ErrorIs(t, err, io.EOF)

		wg.Wait()
	})
}

func TestTransportWebRTC_PeerConnectionDTLSFailed(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	ln, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer ln.Close()

	encoded, err := hex.DecodeString("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
	require.NoError(t, err)
	encodedCerthash, err := multihash.Encode(encoded, multihash.SHA2_256)
	require.NoError(t, err)
	badEncodedCerthash, err := multibase.Encode(multibase.Base58BTC, encodedCerthash)
	require.NoError(t, err)
	badCerthash, err := ma.NewMultiaddr(fmt.Sprintf("/certhash/%s", badEncodedCerthash))
	require.NoError(t, err)
	badMultiaddr, _ := ma.SplitFunc(ln.Multiaddr(), func(c ma.Component) bool { return c.Protocol().Code == ma.P_CERTHASH })
	badMultiaddr = badMultiaddr.Encapsulate(badCerthash)

	tr1, _ := getTransport(t)
	conn, err := tr1.Dial(context.Background(), badMultiaddr, listeningPeer)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed")
	require.Nil(t, conn)
}

func newUDPConnLocalhost(t testing.TB) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestConnectionTimeoutOnListener(t *testing.T) {
	tr, listeningPeer := getTransport(t)
	tr.peerConnectionTimeouts.Disconnect = 100 * time.Millisecond
	tr.peerConnectionTimeouts.Failed = 150 * time.Millisecond
	tr.peerConnectionTimeouts.Keepalive = 50 * time.Millisecond

	listenMultiaddr := ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct")
	ln, err := tr.Listen(listenMultiaddr)
	require.NoError(t, err)
	defer ln.Close()

	var drop atomic.Bool
	proxy := quicproxy.Proxy{
		Conn:       newUDPConnLocalhost(t),
		ServerAddr: ln.Addr().(*net.UDPAddr),
		DropPacket: func(_ quicproxy.Direction, _, _ net.Addr, _ []byte) bool { return drop.Load() },
	}
	require.NoError(t, proxy.Start())
	defer proxy.Close()

	tr1, connectingPeer := getTransport(t)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		addr, err := manet.FromNetAddr(proxy.LocalAddr())
		require.NoError(t, err)
		_, webrtcComponent := ma.SplitFunc(ln.Multiaddr(), func(c ma.Component) bool { return c.Protocol().Code == ma.P_WEBRTC_DIRECT })
		addr = addr.Encapsulate(webrtcComponent)
		conn, err := tr1.Dial(ctx, addr, listeningPeer)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		str, err := conn.OpenStream(ctx)
		require.NoError(t, err)
		str.Write([]byte("foobar"))
	}()

	conn, err := ln.Accept()
	require.NoError(t, err)
	require.Equal(t, connectingPeer, conn.RemotePeer())
	defer conn.Close()

	str, err := conn.AcceptStream()
	require.NoError(t, err)
	_, err = str.Write([]byte("test"))
	require.NoError(t, err)
	// start dropping all packets
	drop.Store(true)
	start := time.Now()
	for {
		if _, err := str.Write([]byte("test")); err != nil {
			if os.IsTimeout(err) {
				break
			}
			// If we write when a connection timeout happens, sctp provides
			// a "stream closed" error. This occurs concurrently with the
			// callback we receive for connection timeout.
			// Test once more after sleep that we provide the correct error.
			if strings.Contains(err.Error(), "stream closed") {
				time.Sleep(50 * time.Millisecond)
				_, err = str.Write([]byte("test"))
				require.True(t, os.IsTimeout(err), "invalid error type: %v", err)
			} else {
				t.Fatal("invalid error type", err)
			}
			break
		}

		if time.Since(start) > 5*time.Second {
			t.Fatal("timeout")
		}
		// make sure to not write too often, we don't want to fill the flow control window
		time.Sleep(20 * time.Millisecond)
	}
	// make sure that accepting a stream also returns an error...
	_, err = conn.AcceptStream()
	require.True(t, os.IsTimeout(err))
	// ... as well as opening a new stream
	_, err = conn.OpenStream(context.Background())
	require.True(t, os.IsTimeout(err))
}

func TestMaxInFlightRequests(t *testing.T) {
	const count = 3
	tr, listeningPeer := getTransport(t,
		WithListenerMaxInFlightConnections(count),
	)
	ln, err := tr.Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct"))
	require.NoError(t, err)
	defer ln.Close()

	var wg sync.WaitGroup
	var success, fails atomic.Int32
	for range count + 1 {
		wg.Go(func() {
			dialer, _ := getTransport(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if conn, err := dialer.Dial(ctx, ln.Multiaddr(), listeningPeer); err == nil {
				success.Add(1)
				t.Cleanup(func() { conn.Close() })
			} else {
				t.Log("failed to dial:", err)
				fails.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, count, int(success.Load()), "expected exactly 3 dial successes")
	require.Equal(t, 1, int(fails.Load()), "expected exactly 1 dial failure")
}

func TestGenUfrag(t *testing.T) {
	for range 10 {
		s := genUfrag()
		require.True(t, strings.HasPrefix(s, "libp2p+webrtc+v1/"))
	}
}

func TestManyConnections(t *testing.T) {
	const numListeners = 5
	const dialersPerListener = 5
	const connsPerDialer = 10
	errCh := make(chan error, 10*numListeners*dialersPerListener*connsPerDialer)
	successCh := make(chan struct{}, 10*numListeners*dialersPerListener*connsPerDialer)
	listeners := make([]tpt.Listener, 0, numListeners)
	listenerPeerIDs := make([]peer.ID, 0, numListeners)

	for range numListeners {
		tr, lp := getTransport(t)
		listenerPeerIDs = append(listenerPeerIDs, lp)
		ln, err := tr.Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct"))
		require.NoError(t, err)
		defer ln.Close()
		listeners = append(listeners, ln)
	}

	runListenConn := func(conn tpt.CapableConn) {
		defer conn.Close()
		s, err := conn.AcceptStream()
		if err != nil {
			t.Errorf("accept stream failed for listener: %s", err)
			errCh <- err
			return
		}
		var b [4]byte
		if _, err := s.Read(b[:]); err != nil {
			t.Errorf("read stream failed for listener: %s", err)
			errCh <- err
			return
		}
		s.Write(b[:])
		_, err = s.Read(b[:]) // peer will close the connection after read
		if !assert.Error(t, err) {
			err = errors.New("invalid read: expected conn to close")
			errCh <- err
			return
		}
		successCh <- struct{}{}
	}

	runDialConn := func(conn tpt.CapableConn) {
		defer conn.Close()

		s, err := conn.OpenStream(context.Background())
		if err != nil {
			t.Errorf("accept stream failed for listener: %s", err)
			errCh <- err
			return
		}
		var b [4]byte
		if _, err := s.Write(b[:]); err != nil {
			t.Errorf("write stream failed for dialer: %s", err)
			errCh <- err
			return
		}
		if _, err := s.Read(b[:]); err != nil {
			t.Errorf("read stream failed for dialer: %s", err)
			errCh <- err
			return
		}
		s.Close()
	}

	runListener := func(ln tpt.Listener) {
		for range dialersPerListener * connsPerDialer {
			conn, err := ln.Accept()
			if err != nil {
				t.Errorf("listener failed to accept connection: %s", err)
				return
			}
			go runListenConn(conn)
		}
	}

	runDialer := func(ln tpt.Listener, lp peer.ID) {
		tp, _ := getTransport(t)
		for range connsPerDialer {
			// We want to test for deadlocks, set a high timeout
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			conn, err := tp.Dial(ctx, ln.Multiaddr(), lp)
			if err != nil {
				t.Errorf("dial failed: %s", err)
				errCh <- err
				cancel()
				return
			}
			runDialConn(conn)
			cancel()
		}
	}

	for i := range numListeners {
		go runListener(listeners[i])
	}
	for i := range numListeners {
		for range dialersPerListener {
			go runDialer(listeners[i], listenerPeerIDs[i])
		}
	}

	for i := range numListeners * dialersPerListener * connsPerDialer {
		select {
		case <-successCh:
			t.Log("completed conn: ", i)
		case err := <-errCh:
			t.Fatalf("failed: %s", err)
		case <-time.After(300 * time.Second):
			t.Fatalf("timed out")
		}
	}
}

func TestConnectionClosedWhenRemoteCloses(t *testing.T) {
	listenT, p := getTransport(t)
	listener, err := listenT.Listen(ma.StringCast("/ip4/127.0.0.1/udp/0/webrtc-direct"))
	require.NoError(t, err)
	defer listener.Close()

	accepted := make(chan struct{})
	dialer, _ := getTransport(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		c, err := listener.Accept()
		close(accepted)
		if !assert.NoError(t, err) {
			return
		}
		assert.Eventually(t, func() bool {
			return c.IsClosed()
		}, 5*time.Second, 50*time.Millisecond)
	})

	c, err := dialer.Dial(context.Background(), listener.Multiaddr(), p)
	require.NoError(t, err)
	<-accepted
	c.Close()
	wg.Wait()
}
