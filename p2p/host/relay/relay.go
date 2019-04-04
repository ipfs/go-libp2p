package relay

import (
	"context"
	"time"

	basic "github.com/libp2p/go-libp2p/p2p/host/basic"

	discovery "github.com/libp2p/go-libp2p-discovery"
	host "github.com/libp2p/go-libp2p-host"
	ma "github.com/multiformats/go-multiaddr"
)

var (
	AdvertiseBootDelay = 30 * time.Second
)

// RelayHost is a Host that provides Relay services.
type RelayHost struct {
	*basic.BasicHost
	advertise discovery.Advertiser
	addrsF    basic.AddrsFactory
}

// Warning:  This function should not be used directly by clients activating
// relay. NewRelayHost is used internally by the libp2p.New generator. To enable
// relay, use the EnableRelay Option. https://godoc.org/github.com/libp2p/go-libp2p#EnableRelay
func NewRelayHost(ctx context.Context, bhost *basic.BasicHost, advertise discovery.Advertiser) *RelayHost {
	h := &RelayHost{
		BasicHost: bhost,
		addrsF:    bhost.AddrsFactory,
		advertise: advertise,
	}
	bhost.AddrsFactory = h.hostAddrs
	go func() {
		select {
		case <-time.After(AdvertiseBootDelay):
			discovery.Advertise(ctx, advertise, RelayRendezvous)
		case <-ctx.Done():
		}
	}()
	return h
}

func (h *RelayHost) hostAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	return filterUnspecificRelay(h.addrsF(addrs))
}

var _ host.Host = (*RelayHost)(nil)
