package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/network"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"golang.org/x/sync/errgroup"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// listenerHostPort returns a "host:port" string for a listener multiaddr,
// suitable for building an HTTP URL.
func listenerHostPort(t *testing.T, m ma.Multiaddr) string {
	t.Helper()
	host, err := m.ValueForProtocol(ma.P_IP4)
	require.NoError(t, err)
	port, err := m.ValueForProtocol(ma.P_TCP)
	require.NoError(t, err)
	return net.JoinHostPort(host, port)
}

// httpsClient returns an http.Client that trusts self-signed certs and
// negotiates the requested ALPN protocols. Pass {"h2", "http/1.1"} to allow
// HTTP/2 with HTTP/1.1 fallback, or {"http/1.1"} to force HTTP/1.1 only.
//
// Note: the Go http.Transport adds h2 itself when TLSClientConfig.NextProtos
// is empty, which is why we always set NextProtos explicitly here.
func httpsClient(nextProtos []string) *http.Client {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         nextProtos,
	}
	tr := &http.Transport{
		ForceAttemptHTTP2: slices.Contains(nextProtos, "h2"),
		TLSClientConfig:   tlsConf,
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: tr}
}

// startWSSListener spins up a /tls/ws listener bound to 127.0.0.1 with the
// given fallback handler (may be nil). Returns the host:port string of the
// listener; the listener is closed via t.Cleanup.
func startWSSListener(t *testing.T, fallback http.Handler) string {
	t.Helper()
	tlsConf := getTLSConf(t, net.ParseIP("127.0.0.1"), time.Now(), time.Now().Add(time.Hour))
	_, u := newSecureUpgrader(t)
	opts := []Option{WithTLSConfig(tlsConf)}
	if fallback != nil {
		opts = append(opts, WithHTTPHandler(fallback))
	}
	tpt, err := New(u, &network.NullResourceManager{}, nil, opts...)
	require.NoError(t, err)

	l, err := tpt.Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0/tls/ws"))
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	// Drain accepted libp2p conns so they don't deadlock the listener.
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	return listenerHostPort(t, l.Multiaddr())
}

// TestFallbackHTTPHandler_TLS_HTTP1 confirms that a /tls/ws listener serves
// the fallback handler over HTTPS/1.1 too. The transport does not pre-screen
// HTTP versions; maximal interop with curl, legacy clients, and any HTTP-only
// tooling is the priority. Multiplexing-sensitive clients should opt into h2
// by offering it in ALPN; this listener will accept whichever ALPN they pick.
func TestFallbackHTTPHandler_TLS_HTTP1(t *testing.T) {
	const body = "hello from fallback"
	var hits atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Test", "ok")
		fmt.Fprint(w, body)
	})

	hostPort := startWSSListener(t, handler)

	resp, err := httpsClient([]string{"http/1.1"}).Get("https://" + hostPort + "/anything")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "HTTP/1.1", resp.Proto)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", resp.Header.Get("X-Test"))
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(got))
	require.Equal(t, int32(1), hits.Load())
}

// TestFallbackHTTPHandler_HTTP2 confirms ALPN selects "h2" and the fallback
// handler still serves the request. This is the censorship-resistance shape:
// HTTP/2 traffic on the same port as /tls/ws.
func TestFallbackHTTPHandler_HTTP2(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "proto=%s", r.Proto)
	})

	hostPort := startWSSListener(t, handler)

	resp, err := httpsClient([]string{"h2", "http/1.1"}).Get("https://" + hostPort + "/h2-test")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "HTTP/2.0", resp.Proto, "client should negotiate h2 via ALPN")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "proto=HTTP/2.0", string(got))
}

// TestFallbackHTTPHandler_WebSocketStillWorks confirms that installing a
// fallback handler does not break the original /tls/ws traffic.
func TestFallbackHTTPHandler_WebSocketStillWorks(t *testing.T) {
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("fallback should not be called for a websocket upgrade, got %s %s", r.Method, r.URL.Path)
	})

	hostPort := startWSSListener(t, handler)

	dialer := gws.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.Dial("wss://"+hostPort+"/", nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// TestFallbackHTTPHandler_NotSetReturns404 preserves the historical behaviour:
// if no fallback handler is configured, non-upgrade requests get a 404.
func TestFallbackHTTPHandler_NotSetReturns404(t *testing.T) {
	hostPort := startWSSListener(t, nil)

	resp, err := httpsClient([]string{"http/1.1"}).Get("https://" + hostPort + "/")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestFallbackHTTPHandler_PlainWS_H2C confirms that a plain /ws listener
// also serves the fallback handler over HTTP/2 cleartext (h2c). This is
// what reverse proxies that speak h2c to backends (Caddy, Traefik, nginx
// with the right directives) use to get connection multiplexing all the
// way to the kubo node, instead of degrading to HTTP/1.1 just because the
// last hop is plaintext.
func TestFallbackHTTPHandler_PlainWS_H2C(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "proto=%s", r.Proto)
	})

	_, u := newSecureUpgrader(t)
	tpt, err := New(u, &network.NullResourceManager{}, nil, WithHTTPHandler(handler))
	require.NoError(t, err)

	l, err := tpt.Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0/ws"))
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	hostPort := listenerHostPort(t, l.Multiaddr())

	// http2.Transport with AllowHTTP and a custom DialTLSContext that does
	// a plain TCP dial is the canonical way to drive prior-knowledge h2c
	// from a Go client.
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	h2cTransport := &http.Transport{Protocols: &protocols}
	defer h2cTransport.CloseIdleConnections()
	client := &http.Client{Transport: h2cTransport, Timeout: 5 * time.Second}

	resp, err := client.Get("http://" + hostPort + "/h2c-test")
	require.NoError(t, err, "h2c request must reach the fallback handler")
	defer resp.Body.Close()

	require.Equal(t, "HTTP/2.0", resp.Proto, "client should be talking h2c, not h1")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "proto=HTTP/2.0", string(got))
}

// TestFallbackHTTPHandler_PlainWS confirms that on a plain /ws listener
// (no TLS), the fallback handler accepts HTTP/1.1 traffic. The HTTP/2
// requirement only applies to /tls/ws listeners; plain /ws is meant to sit
// behind a reverse proxy that terminates TLS upstream and may forward
// HTTP/1.1.
func TestFallbackHTTPHandler_PlainWS(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "plain")
	})

	_, u := newSecureUpgrader(t)
	tpt, err := New(u, &network.NullResourceManager{}, nil, WithHTTPHandler(handler))
	require.NoError(t, err)

	l, err := tpt.Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0/ws"))
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	hostPort := listenerHostPort(t, l.Multiaddr())
	resp, err := http.Get("http://" + hostPort + "/")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "plain", string(got))
}

// TestModernBrowserWSSFlow is the load-bearing test for "browsers must keep
// being able to open wss:// against this listener". This is a hard
// requirement when the listener also serves an HTTP/2 fallback handler.
//
// What modern browsers (Chrome, Firefox, Safari) do for `wss://`:
//
//  1. Open a TLS handshake offering ALPN ["h2", "http/1.1"].
//  2. If the server picks "h2", read the server's first SETTINGS frame and
//     check for SETTINGS_ENABLE_CONNECT_PROTOCOL=1 (RFC 8441 §3). If absent,
//     close the HTTP/2 connection.
//  3. Open a brand-new TLS handshake offering ALPN ["http/1.1"] only and
//     perform the classic HTTP/1.1 Upgrade dance.
//
// This listener does not (and cannot today, see the comment in newListener)
// advertise ENABLE_CONNECT_PROTOCOL, so the contract is:
//
//   - The h2 SETTINGS frame on the first attempt MUST NOT contain
//     SettingEnableConnectProtocol; this is the signal browsers use to know
//     they should fall back.
//   - The HTTP/1.1 attempt MUST succeed — the WebSocket upgrade goes through
//     gorilla as before.
//
// If either check fails, browser wss:// breaks. Do not relax this test
// without also updating gorilla/websocket and the listener to actually
// handle WS-over-HTTP/2 (RFC 8441 ext-CONNECT) end to end.
func TestModernBrowserWSSFlow(t *testing.T) {
	hostPort := startWSSListener(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "fallback")
	}))

	// Step 1: simulate the browser's first TLS handshake.
	t.Run("h2_does_not_advertise_ext_connect", func(t *testing.T) {
		conn, err := tls.Dial("tcp", hostPort, &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2", "http/1.1"},
		})
		require.NoError(t, err)
		defer conn.Close()
		require.Equal(t, "h2", conn.ConnectionState().NegotiatedProtocol,
			"server must select h2 when client offers it; this is the path browsers take")

		// Send the HTTP/2 client preface plus our own (empty) SETTINGS so
		// the server is free to write its own SETTINGS frame back.
		_, err = conn.Write([]byte(http2.ClientPreface))
		require.NoError(t, err)
		framer := http2.NewFramer(conn, conn)
		require.NoError(t, framer.WriteSettings())

		// Read frames until we see the server's initial (non-ACK) SETTINGS.
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
		var serverSettings *http2.SettingsFrame
		for i := 0; i < 8 && serverSettings == nil; i++ {
			f, err := framer.ReadFrame()
			require.NoError(t, err, "did not receive server SETTINGS in time")
			if sf, ok := f.(*http2.SettingsFrame); ok && !sf.IsAck() {
				serverSettings = sf
			}
		}
		require.NotNil(t, serverSettings, "server never sent a non-ACK SETTINGS frame")

		_, hasExtConnect := serverSettings.Value(http2.SettingEnableConnectProtocol)
		require.False(t, hasExtConnect,
			"server must NOT advertise SETTINGS_ENABLE_CONNECT_PROTOCOL; browsers rely on its absence to fall back to HTTP/1.1 for wss://")
	})

	// Step 2: simulate the browser's second attempt with ALPN restricted to
	// HTTP/1.1, where the WebSocket upgrade actually happens.
	t.Run("http1_fallback_wss_upgrade_succeeds", func(t *testing.T) {
		dialer := gws.Dialer{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"http/1.1"},
			},
			HandshakeTimeout: 5 * time.Second,
		}
		conn, resp, err := dialer.Dial("wss://"+hostPort+"/", nil)
		require.NoError(t, err, "browser HTTP/1.1 fallback must succeed; this is the actual WSS path")
		defer conn.Close()
		require.Equal(t, "HTTP/1.1", resp.Proto)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	})
}

// sendH2ExtendedConnect opens a raw HTTP/2 connection to hostPort and sends one
// RFC 8441 extended CONNECT request (":method = CONNECT", ":protocol =
// websocket"). This is how a browser starts a WebSocket over HTTP/2. The
// net/http client cannot build such a request, so we write the frames
// ourselves. It returns the response ":status" and body. If the server resets
// the stream instead of replying, the request never reached the handler, so
// the test fails.
func sendH2ExtendedConnect(t *testing.T, hostPort string) (status string, body []byte) {
	t.Helper()
	conn, err := tls.Dial("tcp", hostPort, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	})
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, "h2", conn.ConnectionState().NegotiatedProtocol)

	// HTTP/2 client preface + our (empty) SETTINGS, then the request headers.
	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	framer := http2.NewFramer(conn, conn)
	require.NoError(t, framer.WriteSettings())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	var hbuf bytes.Buffer
	enc := hpack.NewEncoder(&hbuf)
	for _, hf := range []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: "websocket"}, // the RFC 8441 marker
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: hostPort},
		{Name: "sec-websocket-version", Value: "13"},
		{Name: "sec-websocket-key", Value: "dGhlIHNhbXBsZSBub25jZQ=="},
	} {
		require.NoError(t, enc.WriteField(hf))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      1,
		BlockFragment: hbuf.Bytes(),
		EndHeaders:    true,
		EndStream:     true,
	}))

	// Read response frames until the stream ends, collecting status and body.
	dec := hpack.NewDecoder(4096, nil)
	for range 30 {
		f, err := framer.ReadFrame()
		require.NoError(t, err)
		switch fr := f.(type) {
		case *http2.RSTStreamFrame:
			t.Fatalf("server reset the stream (%v); the request never reached the handler", fr.ErrCode)
		case *http2.HeadersFrame:
			fields, err := dec.DecodeFull(fr.HeaderBlockFragment())
			require.NoError(t, err)
			for _, hf := range fields {
				if hf.Name == ":status" {
					status = hf.Value
				}
			}
			if fr.StreamEnded() {
				return status, body
			}
		case *http2.DataFrame:
			body = append(body, fr.Data()...)
			if fr.StreamEnded() {
				return status, body
			}
		}
	}
	t.Fatal("server never ended the response stream")
	return status, body
}

// TestFallbackHTTPHandler_HTTP2NeverReachesWSPath checks one rule that lets a
// WebSocket and an HTTP/2 fallback share a single /tls/ws port: a WebSocket
// request sent over HTTP/2 (RFC 8441 extended CONNECT) must go to the fallback
// handler, not to gorilla's WebSocket upgrade path.
//
// Why this matters: listener.ServeHTTP decides using ws.IsWebSocketUpgrade,
// which today only looks at the HTTP/1 Upgrade handshake. So an HTTP/2 extended
// CONNECT falls through to the fallback handler. If a later gorilla/websocket
// release teaches IsWebSocketUpgrade to detect extended CONNECT as well, the
// same request would start taking the upgrade path. That is a real change in
// behaviour, and we want this test to fail when it happens (see the RFC 8441
// note in newListener).
//
// Checking this end to end means a real extended CONNECT has to reach the
// handler. By default Go's HTTP/2 server does not advertise
// SETTINGS_ENABLE_CONNECT_PROTOCOL, so it rejects extended CONNECT before any
// handler runs (golang/go#71128). GODEBUG=http2xconnect=1 turns that
// advertisement on, but Go reads it once at startup, so t.Setenv cannot set it.
// The parent process therefore re-runs just this test in a child with that
// GODEBUG set, and the child does the real work.
func TestFallbackHTTPHandler_HTTP2NeverReachesWSPath(t *testing.T) {
	const childEnv = "GO_LIBP2P_WS_EXTCONNECT_CHILD"
	if os.Getenv(childEnv) != "1" {
		// Parent: re-run just this test in a child that accepts extended
		// CONNECT, so the request reaches listener.ServeHTTP.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v", "-test.count=1")
		// Merge http2xconnect into any inherited GODEBUG rather than
		// shadowing it; the last GODEBUG entry wins, so this keeps whatever
		// the suite/CI already set while still enabling extended CONNECT.
		godebug := "http2xconnect=1"
		if prev := os.Getenv("GODEBUG"); prev != "" {
			godebug = prev + ",http2xconnect=1"
		}
		cmd.Env = append(os.Environ(), "GODEBUG="+godebug, childEnv+"=1")
		out, err := cmd.CombinedOutput()
		if ctx.Err() == context.DeadlineExceeded {
			t.Fatalf("child process timed out after 30s:\n%s", out)
		}
		require.NoError(t, err, "child process failed:\n%s", out)
		// Guard against a silent pass: a -test.run that matches nothing also
		// exits 0. Require the child to report this test as passed.
		require.Contains(t, string(out), "--- PASS: "+t.Name(),
			"child did not run the extended CONNECT test:\n%s", out)
		return
	}

	// Child: the HTTP/2 server now delivers extended CONNECT to ServeHTTP.
	var fallbackHits atomic.Int32
	var gotMethod atomic.Value
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		gotMethod.Store(r.Method)
		fmt.Fprint(w, "fallback")
	})
	hostPort := startWSSListener(t, handler)

	status, body := sendH2ExtendedConnect(t, hostPort)

	require.Equal(t, "200", status, "extended CONNECT must be answered by the fallback handler")
	require.Equal(t, "fallback", string(body))
	require.Equal(t, int32(1), fallbackHits.Load(),
		"WebSocket-over-HTTP/2 must reach the fallback handler, not the WebSocket upgrade path")
	require.Equal(t, "CONNECT", gotMethod.Load(), "the handler must see the original extended CONNECT request")
}

// TestFallbackHTTPHandler_KeepAlive ensures the handshake-timeout AfterFunc
// is disarmed once a fallback request arrives. Otherwise the connection
// would be force-closed at handshakeTimeout (default 15s) regardless of
// active traffic.
//
// We exercise this with two HTTP/2 requests on a single shared connection,
// spaced further apart than the handshake-timeout window. HTTP/2 keeps both
// streams on one TCP connection by design, so if the timer were not
// disarmed, the second request would race against the closer.
func TestFallbackHTTPHandler_KeepAlive(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	tlsConf := getTLSConf(t, net.ParseIP("127.0.0.1"), time.Now(), time.Now().Add(time.Hour))
	_, u := newSecureUpgrader(t)
	tpt, err := New(u, &network.NullResourceManager{}, nil,
		WithTLSConfig(tlsConf),
		WithHandshakeTimeout(200*time.Millisecond),
		WithHTTPHandler(handler),
	)
	require.NoError(t, err)
	l, err := tpt.Listen(ma.StringCast("/ip4/127.0.0.1/tcp/0/tls/ws"))
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	hostPort := listenerHostPort(t, l.Multiaddr())

	tr := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2"},
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get("https://" + hostPort + "/first")
	require.NoError(t, err)
	require.Equal(t, "HTTP/2.0", resp.Proto)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Wait past the handshake-timeout window. If Unwrap had not been
	// called, the AfterFunc would have closed the underlying TCP
	// connection that the HTTP/2 transport pools.
	time.Sleep(400 * time.Millisecond)

	resp, err = client.Get("https://" + hostPort + "/second")
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/2.0", resp.Proto, "second request must reuse the same h2 connection")
}

// TestFallbackHTTPHandler_ConcurrentH2Streams exercises the handshake-timer
// disarm under HTTP/2 multiplexing. All streams on one h2 connection share
// a single *negotiatingConn, so Unwrap must be safe under concurrent calls.
// Run with `go test -race`. Before the sync.Once fix, two goroutines could
// both pass the stopClose nil-check and race on the AfterFunc stop; the
// loser silently dropped its request.
func TestFallbackHTTPHandler_ConcurrentH2Streams(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	hostPort := startWSSListener(t, handler)

	// ALPN-h2-only keeps every Get on a single TCP connection, so the
	// streams really do race for the same *negotiatingConn.
	client := httpsClient([]string{"h2"})

	const streams = 32
	ready := make(chan struct{})
	var g errgroup.Group
	for range streams {
		g.Go(func() error {
			<-ready
			resp, err := client.Get("https://" + hostPort + "/concurrent")
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("status=%d", resp.StatusCode)
			}
			if resp.Proto != "HTTP/2.0" {
				return fmt.Errorf("proto=%s, want HTTP/2.0", resp.Proto)
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				return err
			}
			return nil
		})
	}
	close(ready)
	require.NoError(t, g.Wait())
}

// TestNegotiatingConnUnwrapConcurrent locks down the disarm contract that
// the HTTP/2 fallback path relies on: many goroutines calling Unwrap on the
// same *negotiatingConn must all return nil, and the underlying stopClose
// must fire exactly once. Before the sync.Once fix, only the first caller
// returned nil; every other caller saw "timed out" because the AfterFunc
// stop function returns false after the first stop.
func TestNegotiatingConnUnwrapConcurrent(t *testing.T) {
	var stopCalls atomic.Int32
	nc := &negotiatingConn{
		cancelCtx: func() {},
		stopClose: func() bool {
			// Mirror context.AfterFunc: only the first stop call returns true.
			return stopCalls.Add(1) == 1
		},
	}

	const goroutines = 256
	ready := make(chan struct{})
	var ok atomic.Int32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-ready
			if _, err := nc.Unwrap(); err == nil {
				ok.Add(1)
			}
		}()
	}
	close(ready)
	wg.Wait()

	require.Equal(t, int32(goroutines), ok.Load(), "every concurrent Unwrap must succeed")
	require.Equal(t, int32(1), stopCalls.Load(), "stopClose must fire exactly once")
}
