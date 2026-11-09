package quicreuse

import (
	"bytes"
	"errors"
	"net"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-netroute"
	"github.com/stretchr/testify/require"
)

func (c *refcountedTransport) GetCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.refCount
}

func closeAllConns(reuse *reuse) {
	reuse.mutex.Lock()
	for _, tr := range reuse.globalListeners {
		for tr.GetCount() > 0 {
			tr.DecreaseCount()
		}
	}
	for _, tr := range reuse.globalDialers {
		for tr.GetCount() > 0 {
			tr.DecreaseCount()
		}
	}
	for _, trs := range reuse.unicast {
		for _, tr := range trs {
			for tr.GetCount() > 0 {
				tr.DecreaseCount()
			}
		}
	}
	reuse.mutex.Unlock()
}

func platformHasRoutingTables() bool {
	_, err := netroute.New()
	return err == nil
}

func isGarbageCollectorRunning() bool {
	var b bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&b, 1)
	return strings.Contains(b.String(), "quicreuse.(*reuse).gc")
}

func cleanup(t *testing.T, reuse *reuse) {
	t.Cleanup(func() {
		closeAllConns(reuse)
		reuse.Close()
		require.False(t, isGarbageCollectorRunning(), "reuse gc still running")
	})
}

func TestReuseListenOnAllIPv4(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	require.Eventually(t, isGarbageCollectorRunning, 500*time.Millisecond, 50*time.Millisecond, "expected garbage collector to be running")
	cleanup(t, reuse)

	addr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:0")
	require.NoError(t, err)
	conn, err := reuse.TransportForListen("udp4", addr)
	require.NoError(t, err)
	require.Equal(t, 1, conn.GetCount())
}

func TestReuseListenOnAllIPv6(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	require.Eventually(t, isGarbageCollectorRunning, 500*time.Millisecond, 50*time.Millisecond, "expected garbage collector to be running")
	cleanup(t, reuse)

	addr, err := net.ResolveUDPAddr("udp6", "[::]:1234")
	require.NoError(t, err)
	tr, err := reuse.TransportForListen("udp6", addr)
	require.NoError(t, err)
	defer tr.Close()
	require.Equal(t, 1, tr.GetCount())
}

func TestReuseCreateNewGlobalConnOnDial(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	addr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	conn, err := reuse.TransportWithAssociationForDial(nil, "udp4", addr)
	require.NoError(t, err)
	require.Equal(t, 1, conn.GetCount())
	laddr := conn.LocalAddr().(*net.UDPAddr)
	require.Equal(t, "0.0.0.0", laddr.IP.String())
	require.NotEqual(t, 0, laddr.Port)
}

func TestReuseConnectionWhenDialing(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	addr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:0")
	require.NoError(t, err)
	ltr, err := reuse.TransportForListen("udp4", addr)
	require.NoError(t, err)
	require.Equal(t, 1, ltr.GetCount())
	// dial
	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.Equal(t, 2, tr.GetCount())
}

func TestReuseConnectionWhenListening(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	laddr := &net.UDPAddr{IP: net.IPv4zero, Port: tr.LocalAddr().(*net.UDPAddr).Port}
	lconn, err := reuse.TransportForListen("udp4", laddr)
	require.NoError(t, err)
	require.Equal(t, 2, lconn.GetCount())
	require.Equal(t, 2, tr.GetCount())
}

func TestReuseConnectionWhenDialBeforeListen(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	// dial any address
	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	rTr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)

	// open a listener
	laddr := &net.UDPAddr{IP: net.IPv4zero, Port: 1234}
	lTr, err := reuse.TransportForListen("udp4", laddr)
	require.NoError(t, err)

	// new dials should go via the listener connection
	raddr, err = net.ResolveUDPAddr("udp4", "1.1.1.1:1235")
	require.NoError(t, err)
	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.Equal(t, lTr, tr)
	require.Equal(t, 2, tr.GetCount())

	// a listener on an unspecified port should reuse the dialer
	laddr2 := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	lconn2, err := reuse.TransportForListen("udp4", laddr2)
	require.NoError(t, err)
	require.Equal(t, rTr, lconn2)
	require.Equal(t, 2, lconn2.GetCount())
}

func TestReuseListenOnSpecificInterface(t *testing.T) {
	if !platformHasRoutingTables() {
		t.Skip("this test only works on platforms that support routing tables")
	}
	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	router, err := netroute.New()
	require.NoError(t, err)

	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	_, _, ip, err := router.Route(raddr.IP)
	require.NoError(t, err)
	// listen
	addr, err := net.ResolveUDPAddr("udp4", ip.String()+":0")
	require.NoError(t, err)
	lconn, err := reuse.TransportForListen("udp4", addr)
	require.NoError(t, err)
	require.Equal(t, 1, lconn.GetCount())
	// dial
	conn, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.Equal(t, 2, conn.GetCount())
}

func TestReuseGarbageCollect(t *testing.T) {
	maxUnusedDurationOrig := maxUnusedDuration
	garbageCollectIntervalOrig := garbageCollectInterval
	t.Cleanup(func() {
		maxUnusedDuration = maxUnusedDurationOrig
		garbageCollectInterval = garbageCollectIntervalOrig
	})
	garbageCollectInterval = 50 * time.Millisecond
	maxUnusedDuration = 100 * time.Millisecond
	if os.Getenv("CI") != "" {
		// Increase these timeouts if in CI
		garbageCollectInterval = 10 * garbageCollectInterval
		maxUnusedDuration = 10 * maxUnusedDuration
	}

	reuse := newReuse(nil, nil, defaultListenUDP, defaultSourceIPSelectorFn, nil, nil)
	cleanup(t, reuse)

	numGlobals := func() int {
		reuse.mutex.Lock()
		defer reuse.mutex.Unlock()
		return len(reuse.globalListeners) + len(reuse.globalDialers)
	}

	raddr, err := net.ResolveUDPAddr("udp4", "1.2.3.4:1234")
	require.NoError(t, err)
	dTr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.Equal(t, 1, dTr.GetCount())

	addr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:1234")
	require.NoError(t, err)
	lTr, err := reuse.TransportForListen("udp4", addr)
	require.NoError(t, err)
	require.Equal(t, 1, lTr.GetCount())

	closeTime := time.Now()
	lTr.DecreaseCount()
	dTr.DecreaseCount()

	for {
		num := numGlobals()
		if closeTime.Add(maxUnusedDuration).Before(time.Now()) {
			break
		}
		require.Equal(t, 2, num)
		time.Sleep(2 * time.Millisecond)
	}
	require.Eventually(t, func() bool { return numGlobals() == 0 }, 4*garbageCollectInterval, 10*time.Millisecond)
}

// TestDefaultSourceIPSelectorReturnsNilOnRouterFailure verifies that
// defaultSourceIPSelectorFn returns an untyped nil when netroute.New() fails,
// instead of a non-nil wrapper around a nil router. See issue #3537.
func TestDefaultSourceIPSelectorReturnsNilOnRouterFailure(t *testing.T) {
	// On platforms where netroute.New() succeeds, the result must be non-nil.
	// On platforms where it fails (e.g. unprivileged Android), it must be untyped nil.
	selector, err := defaultSourceIPSelectorFn()
	if err != nil {
		// netroute.New() failed. The selector must be nil (untyped),
		// so that callers guarding with `if selector != nil` skip it correctly.
		require.Nil(t, selector, "expected untyped nil when router is unavailable, got non-nil wrapper")
	} else {
		require.NotNil(t, selector)
	}
}

// TestNetrouteSourceIPSelectorNilRoutesVerifies that the defensive nil check
// in PreferredSourceIPForDestination prevents a nil-pointer panic when the
// struct is constructed with a nil routes field. See issue #3537.
func TestNetrouteSourceIPSelectorNilRoutes(t *testing.T) {
	s := &netrouteSourceIPSelector{routes: nil}
	_, err := s.PreferredSourceIPForDestination(&net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no route table available")
}

// TestTransportWithAssociationForDialNoPanicWhenRoutesNil simulates the Android
// scenario where netroute.New() fails, causing the sourceIPSelectorFn to return
// (nil, err). r.routes must be nil so that the existing nil guard in
// TransportWithAssociationForDial skips the router instead of panicking. See #3537.
func TestTransportWithAssociationForDialNoPanicWhenRoutesNil(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, func() (SourceIPSelector, error) {
		// Simulate netroute.New() failing on Android:
		// return untyped nil so the nil guard works correctly.
		return nil, errors.New("simulated route table failure")
	}, nil, nil)
	cleanup(t, reuse)

	// A unicast (non-unspecified) listener is required so that TransportForListen
	// actually invokes sourceIPSelectorFn and assigns its result to r.routes.
	laddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	_, err = reuse.TransportForListen("udp4", laddr)
	require.NoError(t, err)

	// Before the fix this panicked with a nil pointer dereference on
	// s.routes.Route() because r.routes held a non-nil wrapper around a nil router.
	// With the fix, r.routes is nil, so the guard skips the router.
	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.NotNil(t, tr)

	// Without a usable source IP route, the unicast listener cannot be selected,
	// so a fresh dialer bound to 0.0.0.0 is created. This is the expected
	// degraded (no source-IP affinity) behaviour, not a failure.
}

// TestTransportWithAssociationForDialNoPanicWhenSelectorReturnsNilWrapper
// verifies the defensive check inside PreferredSourceIPForDestination.
// Even if a selector returns a non-nil wrapper around a nil router (the original
// bug scenario, reachable via OverrideSourceIPSelector), the method must return
// an error rather than panicking. See #3537.
func TestTransportWithAssociationForDialNoPanicWhenSelectorReturnsNilWrapper(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, func() (SourceIPSelector, error) {
		// Simulate the original bug: a non-nil wrapper around a nil router.
		return &netrouteSourceIPSelector{routes: nil}, nil
	}, nil, nil)
	cleanup(t, reuse)

	// A unicast (non-unspecified) listener is required so that TransportForListen
	// assigns the bad wrapper to r.routes, exercising the buggy path.
	laddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	_, err = reuse.TransportForListen("udp4", laddr)
	require.NoError(t, err)

	// Before the fix this panicked on s.routes.Route(dst.IP). After the fix,
	// PreferredSourceIPForDestination returns an error, so the router is skipped
	// and a fresh global dialer is used instead of panicking.
	raddr, err := net.ResolveUDPAddr("udp4", "1.1.1.1:1234")
	require.NoError(t, err)
	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", raddr)
	require.NoError(t, err)
	require.NotNil(t, tr)
}
