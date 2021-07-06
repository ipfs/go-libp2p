package discovery

import (
	"context"
	"io"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Notifee interface {
	HandlePeerFound(peer.AddrInfo)
}

type Service interface {
	io.Closer
	RegisterNotifee(Notifee)
	UnregisterNotifee(Notifee)
}

func NewMdnsService(ctx context.Context, peerhost host.Host, interval time.Duration) (Service, error) {
	legacy, err := NewMdnsServiceLegacy(ctx, peerhost, interval)
	if err != nil {
		return nil, err
	}
	return &mdnsServiceMuxer{
		s1: legacy,
		s2: NewMdnsServiceNew(peerhost),
	}, nil
}

type mdnsServiceMuxer struct {
	s1, s2 Service
}

var _ Service = &mdnsServiceMuxer{}

func (m *mdnsServiceMuxer) Close() error {
	m.s1.Close()
	return m.s2.Close()
}

func (m *mdnsServiceMuxer) RegisterNotifee(notifee Notifee) {
	m.s1.RegisterNotifee(notifee)
	m.s2.RegisterNotifee(notifee)
}

func (m *mdnsServiceMuxer) UnregisterNotifee(notifee Notifee) {
	m.s1.UnregisterNotifee(notifee)
	m.s2.UnregisterNotifee(notifee)
}
