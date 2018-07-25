package discovery

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	golog "log"
	"net"
	"sync"
	"time"

	mdns "github.com/grandcat/zeroconf"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-host"
	"github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

var log = logging.Logger("mdns")

const ServiceTag = "_ipfs-discovery._udp"

type Service interface {
	io.Closer
	RegisterNotifee(Notifee)
	UnregisterNotifee(Notifee)
}

type Notifee interface {
	HandlePeerFound(pstore.PeerInfo)
}

type mdnsService struct {
	server  *mdns.Server
	service *mdns.Resolver
	host    host.Host
	tag     string

	lk       sync.Mutex
	notifees []Notifee
	interval time.Duration
}

// Tries to pick the best port.
func getBestPort(addrs []ma.Multiaddr) (int, error) {
	var best *net.TCPAddr
	for _, addr := range addrs {
		na, err := manet.ToNetAddr(addr)
		if err != nil {
			continue
		}
		tcp, ok := na.(*net.TCPAddr)
		if !ok {
			continue
		}
		// Don't bother with multicast and
		if tcp.IP.IsMulticast() {
			continue
		}
		// We don't yet support link-local
		if tcp.IP.IsLinkLocalUnicast() {
			continue
		}
		// Unspecified listeners are *always* the best choice.
		if tcp.IP.IsUnspecified() {
			return tcp.Port, nil
		}
		// If we don't have a best choice, use this addr.
		if best == nil {
			best = tcp
			continue
		}
		// If the best choice is a loopback address, replace it.
		if best.IP.IsLoopback() {
			best = tcp
		}
	}
	if best == nil {
		return 0, errors.New("failed to find good external addr from peerhost")
	}
	return best.Port, nil
}

func NewMdnsService(ctx context.Context, peerhost host.Host, interval time.Duration, serviceTag string) (Service, error) {

	// TODO: dont let mdns use logging...
	golog.SetOutput(ioutil.Discard)

	port, err := getBestPort(peerhost.Network().ListenAddresses())
	if err != nil {
		return nil, err
	}
	myid := peerhost.ID().Pretty()

	info := []string{myid}
	if serviceTag == "" {
		serviceTag = ServiceTag
	}

	resolver, err := mdns.NewResolver(nil)
	if err != nil {
		log.Error("Failed to initialize resolver:", err)
	}

	// Create the mDNS server, defer shutdown
	server, err := mdns.Register(myid, serviceTag, "", port, info, nil)
	if err != nil {
		return nil, err
	}

	s := &mdnsService{
		server:   server,
		service:  resolver,
		host:     peerhost,
		interval: interval,
		tag:      serviceTag,
	}

	go s.pollForEntries(ctx)

	return s, nil
}

func (m *mdnsService) Close() error {
	m.server.Shutdown()
	// grandcat/zerconf swallows error, satisfy interface
	return nil
}

func (m *mdnsService) pollForEntries(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	for {
		//execute mdns query right away at method call and then with every tick
		entriesCh := make(chan *mdns.ServiceEntry, 16)
		go func(results <-chan *mdns.ServiceEntry) {
			for entry := range results {
				m.handleEntry(entry)
			}
		}(entriesCh)

		log.Debug("starting mdns query")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		if err := m.service.Browse(ctx, m.tag, "local", entriesCh); err != nil {
			log.Error("mdns lookup error: ", err)
		}
		close(entriesCh)

		log.Debug("mdns query complete")

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			log.Debug("mdns service halting")
			return
		}
	}
}

func (m *mdnsService) handleEntry(e *mdns.ServiceEntry) {
	if len(e.Text) != 1 {
		log.Warningf("Expected exactly one TXT record, got: %v", e.Text)
		return
	}
	// pull out the txt
	mpeer, err := peer.IDB58Decode(e.Text[0])
	if err != nil {
		log.Warning("Error parsing peer ID from mdns entry: ", err)
		return
	}

	if mpeer == m.host.ID() {
		log.Debug("got our own mdns entry, skipping")
		return
	}

	addrs := make([]net.IP, len(e.AddrIPv4)+len(e.AddrIPv6))
	copy(addrs, e.AddrIPv4)
	copy(addrs[len(e.AddrIPv4):], e.AddrIPv6)

	var pi pstore.PeerInfo
	for _, ip := range addrs {
		log.Debugf("Handling MDNS entry: %s:%d %s", ip, e.Port, e.Text[0])

		maddr, err := manet.FromNetAddr(&net.TCPAddr{
			IP:   ip,
			Port: e.Port,
		})
		if err != nil {
			log.Errorf("error creating multiaddr from mdns entry (%s:%d): %s", ip, e.Port, err)
			return
		}
		pi.Addrs = append(pi.Addrs, maddr)
	}

	m.lk.Lock()
	for _, n := range m.notifees {
		go n.HandlePeerFound(pi)
	}
	m.lk.Unlock()
}

func (m *mdnsService) RegisterNotifee(n Notifee) {
	m.lk.Lock()
	m.notifees = append(m.notifees, n)
	m.lk.Unlock()
}

func (m *mdnsService) UnregisterNotifee(n Notifee) {
	m.lk.Lock()
	found := -1
	for i, notif := range m.notifees {
		if notif == n {
			found = i
			break
		}
	}
	if found != -1 {
		m.notifees = append(m.notifees[:found], m.notifees[found+1:]...)
	}
	m.lk.Unlock()
}
