package autorelay

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	blankhost "github.com/libp2p/go-libp2p/p2p/host/blank"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"

	"github.com/stretchr/testify/require"
)

// blockingCloseBus wraps an event.Bus and holds Close on the relay finder's
// connectedness subscription until release is closed. cleanupDisconnectedPeers
// closes that subscription on its way out, so parking it there pins the
// goroutine in a known state and turns the shutdown race into a deterministic
// assertion: Stop must not return while that goroutine is still alive.
type blockingCloseBus struct {
	event.Bus

	wrapped atomic.Bool // set when the finder's subscription was intercepted

	closingOnce sync.Once
	closing     chan struct{} // closed when the subscription's Close is entered
	release     chan struct{} // close to let that Close return
}

func (b *blockingCloseBus) Subscribe(eventType any, opts ...event.SubscriptionOpt) (event.Subscription, error) {
	sub, err := b.Bus.Subscribe(eventType, opts...)
	if err != nil || sub.Name() != cleanupSubName {
		return sub, err
	}
	b.wrapped.Store(true)
	return &blockingCloseSub{Subscription: sub, bus: b}, nil
}

type blockingCloseSub struct {
	event.Subscription
	bus *blockingCloseBus
}

func (s *blockingCloseSub) Close() error {
	s.bus.closingOnce.Do(func() { close(s.bus.closing) })
	<-s.bus.release
	return s.Subscription.Close()
}

// TestStopWaitsForCleanupDisconnectedPeers asserts that Stop does not return
// until cleanupDisconnectedPeers has exited.
//
// background spawns that goroutine, and unless it is tracked by rf.refCount it
// outlives Stop. Stop runs resetMetrics after refCount.Wait, so a surviving
// goroutine can report to the metrics tracer afterwards; and because
// Start/Stop are restartable over a shared rf.relays, one left over from a
// previous run can delete a reservation the current run just made.
func TestStopWaitsForCleanupDisconnectedPeers(t *testing.T) {
	bus := &blockingCloseBus{
		Bus:     eventbus.NewBus(),
		closing: make(chan struct{}),
		release: make(chan struct{}),
	}
	h := blankhost.NewBlankHost(swarmt.GenSwarm(t), blankhost.WithEventBus(bus))
	defer h.Close()

	conf := defaultConfig
	conf.peerSource = func(context.Context, int) <-chan peer.AddrInfo {
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	}
	rf, err := newRelayFinder(h, &conf)
	require.NoError(t, err)
	require.NoError(t, rf.Start())

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		rf.Stop()
	}()

	// Wait for the cleanup goroutine to leave its loop and enter Close.
	select {
	case <-bus.closing:
	case <-time.After(30 * time.Second):
		close(bus.release)
		require.True(t, bus.wrapped.Load(),
			"no subscription named %q was created; this test is out of date with cleanupDisconnectedPeers", cleanupSubName)
		t.Fatal("cleanupDisconnectedPeers did not shut down after Stop cancelled the context")
	}

	// It is now parked in Close and cannot make progress. If Stop is tracking
	// it, Stop cannot have returned - not "has not yet", but cannot.
	select {
	case <-stopped:
		close(bus.release)
		t.Fatal("Stop returned while cleanupDisconnectedPeers was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(bus.release)
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("Stop did not return after cleanupDisconnectedPeers exited")
	}
}
