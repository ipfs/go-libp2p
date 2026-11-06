package backoff

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stretchr/testify/require"
)

type limitTestDiscovery struct {
	peers []peer.AddrInfo
	calls int
}

func (d *limitTestDiscovery) Advertise(context.Context, string, ...discovery.Option) (time.Duration, error) {
	return time.Hour, nil
}

func (d *limitTestDiscovery) FindPeers(context.Context, string, ...discovery.Option) (<-chan peer.AddrInfo, error) {
	d.calls++
	ch := make(chan peer.AddrInfo, len(d.peers))
	for _, info := range d.peers {
		ch <- info
	}
	close(ch)
	return ch, nil
}

func TestBackoffDiscoveryUnlimitedPeers(t *testing.T) {
	peers := []peer.AddrInfo{{ID: "one"}, {ID: "two"}, {ID: "three"}}
	for _, tc := range []struct {
		name  string
		opts  []discovery.Option
		count int
	}{
		{name: "default", count: len(peers)},
		{name: "explicit zero", opts: []discovery.Option{discovery.Limit(0)}, count: len(peers)},
		{name: "finite limit", opts: []discovery.Option{discovery.Limit(2)}, count: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &limitTestDiscovery{peers: peers}
			cached, err := NewBackoffDiscovery(provider, NewFixedBackoff(time.Hour))
			require.NoError(t, err)
			for _, phase := range []string{"fresh", "cached"} {
				t.Run(phase, func(t *testing.T) {
					ch, err := cached.FindPeers(t.Context(), "test", tc.opts...)
					require.NoError(t, err)
					var received []peer.AddrInfo
					for info := range ch {
						received = append(received, info)
					}
					require.Len(t, received, tc.count)
					if tc.count == len(peers) {
						require.ElementsMatch(t, peers, received)
					}
				})
			}
			require.Equal(t, 1, provider.calls)
		})
	}
}
