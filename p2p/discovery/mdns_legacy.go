package discovery

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/whyrusleeping/mdns"
)

func init() {
	// don't let mdns use logging...
	mdns.DisableLogging = true
}

var log = logging.Logger("mdns")

const ServiceTag = "_ipfs-discovery._udp"

type Service interface {
	io.Closer
	RegisterNotifee(Notifee)
	UnregisterNotifee(Notifee)
}

type Notifee interface {
	HandlePeerFound(peer.AddrInfo)
}

type mdnsServiceLegacy struct {
	server  *mdns.Server
	service *mdns.MDNSService
	host    host.Host
	tag     string

	lk       sync.Mutex
	notifees []Notifee
	interval time.Duration
}

func getDialableListenAddrs(ph host.Host) ([]*net.TCPAddr, error) {
	var out []*net.TCPAddr
	addrs, err := ph.Network().InterfaceListenAddresses()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		na, err := manet.ToNetAddr(addr)
		if err != nil {
			continue
		}
		tcp, ok := na.(*net.TCPAddr)
		if ok {
			out = append(out, tcp)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("failed to find good external addr from peerhost")
	}
	return out, nil
}

func NewMdnsService(ctx context.Context, peerhost host.Host, interval time.Duration, serviceTag string) (Service, error) {

	var ipaddrs []net.IP
	port := 4001

	addrs, err := getDialableListenAddrs(peerhost)
	if err != nil {
		log.Warn(err)
	} else {
		port = addrs[0].Port
		for _, a := range addrs {
			ipaddrs = append(ipaddrs, a.IP)
		}
	}

	myid := peerhost.ID().Pretty()

	info := []string{myid}
	if serviceTag == "" {
		serviceTag = ServiceTag
	}
	service, err := mdns.NewMDNSService(myid, serviceTag, "", "", port, ipaddrs, info)
	if err != nil {
		return nil, err
	}

	// Create the mDNS server, defer shutdown
	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, err
	}

	s := &mdnsServiceLegacy{
		server:   server,
		service:  service,
		host:     peerhost,
		interval: interval,
		tag:      serviceTag,
	}

	go s.pollForEntries(ctx)

	return s, nil
}

func (m *mdnsServiceLegacy) Close() error {
	return m.server.Shutdown()
}

func (m *mdnsServiceLegacy) pollForEntries(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		//execute mdns query right away at method call and then with every tick
		entriesCh := make(chan *mdns.ServiceEntry, 16)
		go func() {
			for entry := range entriesCh {
				m.handleEntry(entry)
			}
		}()

		log.Debug("starting mdns query")
		qp := &mdns.QueryParam{
			Domain:  "local",
			Entries: entriesCh,
			Service: m.tag,
			Timeout: time.Second * 5,
		}

		err := mdns.Query(qp)
		if err != nil {
			log.Warnw("mdns lookup error", "error", err)
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

func (m *mdnsServiceLegacy) handleEntry(e *mdns.ServiceEntry) {
	log.Debugf("Handling MDNS entry: [IPv4 %s][IPv6 %s]:%d %s", e.AddrV4, e.AddrV6, e.Port, e.Info)
	mpeer, err := peer.Decode(e.Info)
	if err != nil {
		log.Warn("Error parsing peer ID from mdns entry: ", err)
		return
	}

	if mpeer == m.host.ID() {
		log.Debug("got our own mdns entry, skipping")
		return
	}

	var addr net.IP
	if e.AddrV4 != nil {
		addr = e.AddrV4
	} else if e.AddrV6 != nil {
		addr = e.AddrV6
	} else {
		log.Warn("Error parsing multiaddr from mdns entry: no IP address found")
		return
	}

	maddr, err := manet.FromNetAddr(&net.TCPAddr{
		IP:   addr,
		Port: e.Port,
	})
	if err != nil {
		log.Warn("Error parsing multiaddr from mdns entry: ", err)
		return
	}

	pi := peer.AddrInfo{
		ID:    mpeer,
		Addrs: []ma.Multiaddr{maddr},
	}

	m.lk.Lock()
	for _, n := range m.notifees {
		go n.HandlePeerFound(pi)
	}
	m.lk.Unlock()
}

func (m *mdnsServiceLegacy) RegisterNotifee(n Notifee) {
	m.lk.Lock()
	m.notifees = append(m.notifees, n)
	m.lk.Unlock()
}

func (m *mdnsServiceLegacy) UnregisterNotifee(n Notifee) {
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
