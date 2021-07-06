package discovery

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/stretchr/testify/assert"
)

func setupMDNS(t *testing.T, notifee Notifee) (host.Host, *mdnsService) {
	t.Helper()
	host, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	s := NewMdnsServiceNew(host)
	s.RegisterNotifee(notifee)
	return host, s
}

type notif struct {
	mutex sync.Mutex
	infos []peer.AddrInfo
}

var _ Notifee = &notif{}

func (n *notif) HandlePeerFound(info peer.AddrInfo) {
	n.mutex.Lock()
	n.infos = append(n.infos, info)
	fmt.Printf("Discovered %s at %v.\n", info.ID, info.Addrs)
	n.mutex.Unlock()
}

func (n *notif) GetPeers() []peer.AddrInfo {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	infos := make([]peer.AddrInfo, 0, len(n.infos))
	infos = append(infos, n.infos...)
	return infos
}

func TestSelfDiscovery(t *testing.T) {
	notif := &notif{}
	host, s := setupMDNS(t, notif)
	defer s.Close()
	assert.Eventuallyf(
		t,
		func() bool {
			var found bool
			for _, info := range notif.GetPeers() {
				if info.ID == host.ID() {
					found = true
					break
				}
			}
			return found
		},
		5*time.Second,
		5*time.Millisecond,
		"expected peer to find itself",
	)
}

func TestOtherDiscovery(t *testing.T) {
	const n = 4

	notifs := make([]*notif, n)
	hostIDs := make([]peer.ID, n)
	for i := 0; i < n; i++ {
		notif := &notif{}
		notifs[i] = notif
		var s *mdnsService
		host, s := setupMDNS(t, notif)
		hostIDs[i] = host.ID()
		defer s.Close()
		time.Sleep(2 * time.Second)
	}
	// fmt.Println(hostIDs)

	containsAllHostIDs := func(ids []peer.ID) bool {
		for _, id := range hostIDs {
			var found bool
			for _, i := range ids {
				if id == i {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	assert.Eventuallyf(
		t,
		func() bool {
			for _, notif := range notifs {
				infos := notif.GetPeers()
				ids := make([]peer.ID, 0, len(infos))
				for _, info := range infos {
					ids = append(ids, info.ID)
				}
				if !containsAllHostIDs(ids) {
					return false
				}
			}
			return true
		},
		25*time.Second,
		5*time.Millisecond,
		"expected peer to find itself",
	)
}
