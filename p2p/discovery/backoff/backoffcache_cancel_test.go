package backoff

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

// chanDiscovery returns a channel controlled by the test from FindPeers.
type chanDiscovery struct {
	ch chan peer.AddrInfo
}

func (d *chanDiscovery) Advertise(context.Context, string, ...discovery.Option) (time.Duration, error) {
	return time.Hour, nil
}

func (d *chanDiscovery) FindPeers(context.Context, string, ...discovery.Option) (<-chan peer.AddrInfo, error) {
	return d.ch, nil
}

func TestBackoffDiscoveryCancelledReceiverDoesNotBlock(t *testing.T) {
	src := &chanDiscovery{ch: make(chan peer.AddrInfo)}
	d, err := NewBackoffDiscovery(src, NewFixedBackoff(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	const ns = "test"

	// A starts the query and keeps reading.
	chA, err := d.FindPeers(t.Context(), ns, discovery.Limit(100))
	if err != nil {
		t.Fatal(err)
	}
	gotA := make(chan int)
	go func() {
		n := 0
		for range chA {
			n++
		}
		gotA <- n
	}()

	// B joins the ongoing query and then gives up.
	ctxB, cancelB := context.WithCancel(t.Context())
	if _, err := d.FindPeers(ctxB, ns, discovery.Limit(100)); err != nil {
		t.Fatal(err)
	}
	cancelB()
	time.Sleep(50 * time.Millisecond) // let B's receiver goroutine exit

	// Deliver more peers than B's internal buffer (parallelBufSz = 32) can hold.
	const n = 40
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for range n {
			select {
			case src.ch <- peer.AddrInfo{ID: test.RandPeerIDFatal(t)}:
			case <-t.Context().Done():
				return
			}
		}
		close(src.ch)
	}()

	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher stopped reading from the underlying discovery after a receiver's context was cancelled")
	}

	select {
	case got := <-gotA:
		if got != n {
			t.Fatalf("expected A to receive %d peers, got %d", n, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("A's channel was never closed")
	}

	// Later queries on the namespace must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ch, err := d.FindPeers(t.Context(), ns, discovery.Limit(1))
		if err != nil {
			return
		}
		for range ch {
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("FindPeers blocked after a receiver's context was cancelled")
	}
}
