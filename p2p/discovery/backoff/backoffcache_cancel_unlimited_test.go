package backoff

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stretchr/testify/require"
)

type cancellationTestDiscovery struct {
	peers <-chan peer.AddrInfo
}

func (d *cancellationTestDiscovery) Advertise(context.Context, string, ...discovery.Option) (time.Duration, error) {
	return time.Hour, nil
}

func (d *cancellationTestDiscovery) FindPeers(context.Context, string, ...discovery.Option) (<-chan peer.AddrInfo, error) {
	return d.peers, nil
}

func TestBackoffDiscoveryCanceledUnlimitedQuery(t *testing.T) {
	for _, bufferSize := range []int{0, 2} {
		t.Run(fmt.Sprintf("buffer=%d", bufferSize), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := make(chan peer.AddrInfo)
			d, err := NewBackoffDiscovery(&cancellationTestDiscovery{peers: source}, NewFixedBackoff(time.Hour),
				WithBackoffDiscoverySimultaneousQueryBufferSize(bufferSize),
				WithBackoffDiscoveryReturnedChannelSize(0))
			require.NoError(t, err)
			active, err := d.FindPeers(ctx, "test")
			require.NoError(t, err)
			otherCtx, cancelOther := context.WithCancel(ctx)
			defer cancelOther()
			canceled, err := d.FindPeers(otherCtx, "test", discovery.Limit(0))
			require.NoError(t, err)
			cancelOther()
			select {
			case _, ok := <-canceled:
				require.False(t, ok, "canceled receiver must exit before more peers arrive")
			case <-time.After(5 * time.Second):
				t.Fatal("canceled query did not close")
			}

			expected := make([]peer.AddrInfo, bufferSize+5)
			for i := range expected {
				expected[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("peer-%d", i))}
			}
			go func() {
				defer close(source)
				for _, info := range expected {
					select {
					case source <- info:
					case <-ctx.Done():
						return
					}
				}
			}()
			finished := make(chan []peer.AddrInfo, 1)
			go func() {
				var peers []peer.AddrInfo
				for info := range active {
					peers = append(peers, info)
				}
				finished <- peers
			}()
			select {
			case peers := <-finished:
				require.ElementsMatch(t, expected, peers)
			case <-time.After(5 * time.Second):
				t.Fatal("canceled unlimited query blocked the active query")
			}

			cached, err := d.FindPeers(ctx, "test")
			require.NoError(t, err)
			var peers []peer.AddrInfo
			for info := range cached {
				peers = append(peers, info)
			}
			require.ElementsMatch(t, expected, peers)
		})
	}
}
