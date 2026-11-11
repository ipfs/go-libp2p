package tcpreuse

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/transport"
	tptu "github.com/libp2p/go-libp2p/p2p/net/upgrader"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multistream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func selfSignedTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	certTemplate := x509.Certificate{
		SerialNumber: &big.Int{},
		Subject: pkix.Name{
			Organization: []string{"Test"},
		},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return tlsConfig
}

type maListener struct {
	transport.GatedMaListener
}

var _ manet.Listener = &maListener{}

func (ml *maListener) Accept() (manet.Conn, error) {
	c, _, err := ml.GatedMaListener.Accept()
	return c, err
}

type blockingCloseGatedMaListener struct {
	transport.GatedMaListener
	closeStarted  chan struct{}
	continueClose chan struct{}
}

func (l *blockingCloseGatedMaListener) Close() error {
	close(l.closeStarted)
	<-l.continueClose
	return l.GatedMaListener.Close()
}

func TestConcurrentListenAndCloseDoesNotDeadlock(t *testing.T) {
	cm := NewConnMgr(false, upgrader(t))
	listenAddr := ma.StringCast("/ip4/127.0.0.1/tcp/0")
	gmal, err := cm.gatedMaListen(listenAddr)
	require.NoError(t, err)

	closeStarted := make(chan struct{})
	continueClose := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	ml := &multiplexedListener{
		GatedMaListener: &blockingCloseGatedMaListener{
			GatedMaListener: gmal,
			closeStarted:    closeStarted,
			continueClose:   continueClose,
		},
		listeners: make(map[DemultiplexedConnType]*demultiplexedListener),
		ctx:       ctx,
		cancel:    cancel,
	}
	ml.closeFn = func() error {
		cm.mx.Lock()
		defer cm.mx.Unlock()
		delete(cm.listeners, listenAddr.String())
		delete(cm.listeners, gmal.Multiaddr().String())
		return nil
	}
	cm.mx.Lock()
	cm.listeners[listenAddr.String()] = ml
	cm.listeners[gmal.Multiaddr().String()] = ml
	cm.mx.Unlock()

	closeDone := make(chan error, 1)
	go func() { closeDone <- ml.Close() }()
	<-closeStarted // Pause closure while starting a concurrent listener registration.

	listenDone := make(chan error, 1)
	go func() {
		_, err := cm.DemultiplexedListen(gmal.Multiaddr(), DemultiplexedConnType_HTTP)
		listenDone <- err
	}()

	// On the buggy path, DemultiplexedListen holds cm.mx and blocks on ml.mx.
	// With the fix, it may instead observe the canceled listener and return.
	var listenErr error
	listenReturned := false
	require.Eventually(t, func() bool {
		select {
		case listenErr = <-listenDone:
			listenReturned = true
			return true
		default:
		}
		if cm.mx.TryLock() {
			cm.mx.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	close(continueClose)

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("multiplexed listener Close deadlocked with DemultiplexedListen")
	}
	if !listenReturned {
		select {
		case listenErr = <-listenDone:
		case <-time.After(time.Second):
			t.Fatal("DemultiplexedListen deadlocked with multiplexed listener Close")
		}
	}
	require.True(t, listenErr == nil || errors.Is(listenErr, transport.ErrListenerClosed), listenErr)
}

type wsHandler struct{ conns chan *websocket.Conn }

func (wh wsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u := websocket.Upgrader{}
	c, _ := u.Upgrade(w, r, http.Header{})
	wh.conns <- c
}

func upgrader(t *testing.T) transport.Upgrader {
	t.Helper()
	upd, err := tptu.New(nil, nil, nil, &network.NullResourceManager{}, nil)
	require.NoError(t, err)
	return upd
}

func TestListenerSingle(t *testing.T) {
	listenAddr := ma.StringCast("/ip4/0.0.0.0/tcp/0")
	const N = 64
	for _, enableReuseport := range []bool{true, false} {
		t.Run(fmt.Sprintf("multistream-reuseport:%v", enableReuseport), func(t *testing.T) {
			cm := NewConnMgr(enableReuseport, upgrader(t))
			l, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
			require.NoError(t, err)
			go func() {
				d := net.Dialer{}
				for i := range N {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						conn, err := d.DialContext(ctx, l.Addr().Network(), l.Addr().String())
						if err != nil {
							t.Error("failed to dial", err, i)
							return
						}
						lconn := multistream.NewMSSelect(conn, "a")
						buf := make([]byte, 10)
						_, err = lconn.Write([]byte("hello-multistream"))
						if err != nil {
							t.Error(err)
						}
						_, err = lconn.Read(buf)
						if err == nil {
							t.Error("expected EOF got nil")
						}
					}()
				}
			}()

			var wg sync.WaitGroup
			for range N {
				c, _, err := l.Accept()
				require.NoError(t, err)
				wg.Go(func() {
					cc := multistream.NewMSSelect(c, "a")
					defer cc.Close()
					buf := make([]byte, 30)
					n, err := cc.Read(buf)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, "hello-multistream", string(buf[:n])) {
						return
					}
				})
			}
			wg.Wait()
		})

		t.Run(fmt.Sprintf("WebSocket-reuseport:%v", enableReuseport), func(t *testing.T) {
			cm := NewConnMgr(enableReuseport, upgrader(t))
			l, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
			require.NoError(t, err)
			wh := wsHandler{conns: make(chan *websocket.Conn, acceptQueueSize)}
			go func() {
				http.Serve(manet.NetListener(&maListener{GatedMaListener: l}), wh)
			}()
			go func() {
				d := websocket.Dialer{}
				for i := range N {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						conn, _, err := d.DialContext(ctx, fmt.Sprintf("ws://%s", l.Addr().String()), http.Header{})
						if err != nil {
							t.Error("failed to dial", err, i)
							return
						}
						err = conn.WriteMessage(websocket.TextMessage, []byte("hello"))
						if err != nil {
							t.Error(err)
						}
						_, _, err = conn.ReadMessage()
						if err == nil {
							t.Error("expected EOF got nil")
						}
					}()
				}
			}()
			var wg sync.WaitGroup
			for range N {
				c := <-wh.conns
				wg.Go(func() {
					defer c.Close()
					msgType, buf, err := c.ReadMessage()
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, msgType, websocket.TextMessage) {
						return
					}
					if !assert.Equal(t, "hello", string(buf)) {
						return
					}
				})
			}
			wg.Wait()
		})

		t.Run(fmt.Sprintf("WebSocketTLS-reuseport:%v", enableReuseport), func(t *testing.T) {
			cm := NewConnMgr(enableReuseport, upgrader(t))
			l, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_TLS)
			require.NoError(t, err)
			defer l.Close()
			wh := wsHandler{conns: make(chan *websocket.Conn, acceptQueueSize)}
			go func() {
				s := http.Server{Handler: wh, TLSConfig: selfSignedTLSConfig(t)}
				s.ServeTLS(manet.NetListener(&maListener{GatedMaListener: l}), "", "")
			}()
			go func() {
				d := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
				for i := range N {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						conn, _, err := d.DialContext(ctx, fmt.Sprintf("wss://%s", l.Addr().String()), http.Header{})
						if err != nil {
							t.Error("failed to dial", err, i)
							return
						}
						err = conn.WriteMessage(websocket.TextMessage, []byte("hello"))
						if err != nil {
							t.Error(err)
						}
						_, _, err = conn.ReadMessage()
						if err == nil {
							t.Error("expected EOF got nil")
						}
					}()
				}
			}()
			var wg sync.WaitGroup
			for range N {
				c := <-wh.conns
				wg.Go(func() {
					defer c.Close()
					msgType, buf, err := c.ReadMessage()
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, msgType, websocket.TextMessage) {
						return
					}
					if !assert.Equal(t, "hello", string(buf)) {
						return
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestListenerMultiplexed(t *testing.T) {
	listenAddr := ma.StringCast("/ip4/0.0.0.0/tcp/0")
	const N = 20
	for _, enableReuseport := range []bool{true, false} {
		cm := NewConnMgr(enableReuseport, upgrader(t))
		msl, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
		require.NoError(t, err)
		defer msl.Close()

		wsl, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
		require.NoError(t, err)
		defer wsl.Close()
		require.Equal(t, wsl.Multiaddr(), msl.Multiaddr())
		wh := wsHandler{conns: make(chan *websocket.Conn, acceptQueueSize)}
		go func() {
			http.Serve(manet.NetListener(&maListener{GatedMaListener: wsl}), wh)
		}()

		wssl, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_TLS)
		require.NoError(t, err)
		defer wssl.Close()
		require.Equal(t, wssl.Multiaddr(), wsl.Multiaddr())
		whs := wsHandler{conns: make(chan *websocket.Conn, acceptQueueSize)}
		go func() {
			s := http.Server{Handler: whs, TLSConfig: selfSignedTLSConfig(t)}
			s.ServeTLS(manet.NetListener(&maListener{GatedMaListener: wssl}), "", "")
		}()

		// multistream connections
		go func() {
			d := net.Dialer{}
			for i := range N {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					conn, err := d.DialContext(ctx, msl.Addr().Network(), msl.Addr().String())
					if err != nil {
						t.Error("failed to dial", err, i)
						return
					}
					lconn := multistream.NewMSSelect(conn, "a")
					buf := make([]byte, 10)
					_, err = lconn.Write([]byte("multistream"))
					if err != nil {
						t.Error(err)
					}
					_, err = lconn.Read(buf)
					if err == nil {
						t.Error("expected EOF got nil")
					}
				}()
			}
		}()

		// ws connections
		go func() {
			d := websocket.Dialer{}
			for i := range N {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					conn, _, err := d.DialContext(ctx, fmt.Sprintf("ws://%s", msl.Addr().String()), http.Header{})
					if err != nil {
						t.Error("failed to dial", err, i)
						return
					}
					err = conn.WriteMessage(websocket.TextMessage, []byte("websocket"))
					if err != nil {
						t.Error(err)
					}
					_, _, err = conn.ReadMessage()
					if err == nil {
						t.Error("expected EOF got nil")
					}
				}()
			}
		}()

		// wss connections
		go func() {
			d := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			for i := range N {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					conn, _, err := d.DialContext(ctx, fmt.Sprintf("wss://%s", msl.Addr().String()), http.Header{})
					if err != nil {
						t.Error("failed to dial", err, i)
						return
					}
					err = conn.WriteMessage(websocket.TextMessage, []byte("websocket-tls"))
					if err != nil {
						t.Error(err)
					}
					_, _, err = conn.ReadMessage()
					if err == nil {
						t.Error("expected EOF got nil")
					}
				}()
			}
		}()

		var wg sync.WaitGroup
		wg.Go(func() {
			for range N {
				c, _, err := msl.Accept()
				if !assert.NoError(t, err) {
					return
				}
				wg.Go(func() {
					cc := multistream.NewMSSelect(c, "a")
					defer cc.Close()
					buf := make([]byte, 20)
					n, err := cc.Read(buf)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, "multistream", string(buf[:n])) {
						return
					}
				})
			}
		})

		wg.Go(func() {
			for range N {
				c := <-wh.conns
				wg.Go(func() {
					defer c.Close()
					msgType, buf, err := c.ReadMessage()
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, msgType, websocket.TextMessage) {
						return
					}
					if !assert.Equal(t, "websocket", string(buf)) {
						return
					}
				})
			}
		})

		wg.Go(func() {
			for range N {
				c := <-whs.conns
				wg.Go(func() {
					defer c.Close()
					msgType, buf, err := c.ReadMessage()
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, msgType, websocket.TextMessage) {
						return
					}
					if !assert.Equal(t, "websocket-tls", string(buf)) {
						return
					}
				})
			}
		})
		wg.Wait()
	}
}

func TestListenerClose(t *testing.T) {
	testClose := func(listenAddr ma.Multiaddr) {
		// listen on port 0
		cm := NewConnMgr(false, upgrader(t))
		ml, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
		require.NoError(t, err)
		wl, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
		require.NoError(t, err)
		require.Equal(t, wl.Multiaddr(), ml.Multiaddr())
		wl.Close()

		wl, err = cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
		require.NoError(t, err)
		require.Equal(t, wl.Multiaddr(), ml.Multiaddr())

		ml.Close()

		mll, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
		require.NoError(t, err)
		require.Equal(t, wl.Multiaddr(), ml.Multiaddr())

		mll.Close()
		wl.Close()

		ml, err = cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
		require.NoError(t, err)

		// Now listen on the specific port previously used
		listenAddr = ml.Multiaddr()
		wl, err = cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
		require.NoError(t, err)
		require.Equal(t, wl.Multiaddr(), ml.Multiaddr())
		wl.Close()

		wl, err = cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_HTTP)
		require.NoError(t, err)
		require.Equal(t, wl.Multiaddr(), ml.Multiaddr())

		ml.Close()
		wl.Close()
	}
	listenAddrs := []ma.Multiaddr{ma.StringCast("/ip4/0.0.0.0/tcp/0"), ma.StringCast("/ip6/::/tcp/0")}
	for _, listenAddr := range listenAddrs {
		testClose(listenAddr)
	}
}

func setDeferReset[T any](t testing.TB, ptr *T, val T) {
	t.Helper()
	orig := *ptr
	*ptr = val
	t.Cleanup(func() { *ptr = orig })
}

// TestHitTimeout asserts that we don't panic in case we fail to peek at the connection.
func TestHitTimeout(t *testing.T) {
	setDeferReset(t, &identifyConnTimeout, 100*time.Millisecond)
	// listen on port 0
	cm := NewConnMgr(false, upgrader(t))

	listenAddr := ma.StringCast("/ip4/127.0.0.1/tcp/0")
	ml, err := cm.DemultiplexedListen(listenAddr, DemultiplexedConnType_MultistreamSelect)
	require.NoError(t, err)
	defer ml.Close()

	tcpConn, err := net.Dial(ml.Addr().Network(), ml.Addr().String())
	require.NoError(t, err)

	// Stall tcp conn for over the timeout.
	time.Sleep(identifyConnTimeout + 100*time.Millisecond)

	tcpConn.Close()
}
