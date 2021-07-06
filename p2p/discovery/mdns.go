package discovery

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/grandcat/zeroconf"
)

const (
	mdnsServiceName = "_p2p._udp"
	mdnsDomain      = "local"
	dnsaddrPrefix   = "dnsaddr="
)

type mdnsService struct {
	host host.Host

	// This ctx is passed to the resolver.
	// It is closed when Close() is called.
	ctx       context.Context
	ctxCancel context.CancelFunc

	server *zeroconf.Server

	mutex    sync.Mutex
	notifees []Notifee
}

func NewMdnsServiceNew(host host.Host) *mdnsService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &mdnsService{
		ctx:       ctx,
		ctxCancel: cancel,
		host:      host,
	}
	s.startServer()
	s.startResolver()
	return s
}

func (s *mdnsService) Close() error {
	s.ctxCancel()
	if s.server != nil {
		s.server.Shutdown()
	}
	return nil
}

// We don't really care about the IP addresses, but the spec (and various routers / firewalls) require us
// to send A and AAAA records.
func (s *mdnsService) getIPs(addrs []ma.Multiaddr) ([]string, error) {
	var ip4, ip6 string
	for _, addr := range addrs {
		network, hostport, err := manet.DialArgs(addr)
		if err != nil {
			continue
		}
		host, _, err := net.SplitHostPort(hostport)
		if err != nil {
			continue
		}
		if ip4 == "" && (network == "udp4" || network == "tcp4") {
			ip4 = host
		} else if ip6 == "" && (network == "udp6" || network == "tcp6") {
			ip6 = host
		}
	}
	ips := make([]string, 0, 2)
	if ip4 != "" {
		ips = append(ips, ip4)
	}
	if ip6 != "" {
		ips = append(ips, ip6)
	}
	if len(ips) == 0 {
		return nil, errors.New("didn't find any IP addresses")
	}
	return ips, nil
}

func (s *mdnsService) mdnsInstance() string {
	return string(s.host.ID())
}

func (s *mdnsService) startServer() error {
	interfaceAddrs, err := s.host.Network().InterfaceListenAddresses()
	if err != nil {
		return err
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
		ID:    s.host.ID(),
		Addrs: interfaceAddrs,
	})
	if err != nil {
		return err
	}
	var txts []string
	for _, addr := range addrs {
		if manet.IsThinWaist(addr) { // don't announce circuit addresses
			txts = append(txts, dnsaddrPrefix+addr.String())
		}
	}

	ips, err := s.getIPs(addrs)
	if err != nil {
		return err
	}

	server, err := zeroconf.RegisterProxy(
		s.mdnsInstance(),
		mdnsServiceName,
		mdnsDomain,
		4001,
		s.host.ID().Pretty(), // TODO: deals with peer IDs longer than 63 characters
		ips,
		txts,
		nil,
	)
	if err != nil {
		return err
	}
	s.server = server
	return nil
}

func (s *mdnsService) startResolver() error {
	resolver, err := zeroconf.NewResolver()
	if err != nil {
		return err
	}

	entryChan := make(chan *zeroconf.ServiceEntry, 1000)
	go func() {
		for entry := range entryChan {
			// We only care about the TXT records.
			// Ignore A, AAAA and PTR.
			addrs := make([]ma.Multiaddr, 0, len(entry.Text)) // assume that all TXT records are dnsaddrs
			for _, s := range entry.Text {
				if !strings.HasPrefix(s, dnsaddrPrefix) {
					log.Debug("missing dnsaddr prefix")
					continue
				}
				addr, err := ma.NewMultiaddr(s[len(dnsaddrPrefix):])
				if err != nil {
					log.Debugf("failed to parse multiaddr: %s", err)
					continue
				}
				addrs = append(addrs, addr)
			}
			infos, err := peer.AddrInfosFromP2pAddrs(addrs...)
			if err != nil {
				log.Debugf("failed to get peer info: %s", err)
				continue
			}
			s.mutex.Lock()
			for _, info := range infos {
				for _, notif := range s.notifees {
					go notif.HandlePeerFound(info)
				}
			}
			s.mutex.Unlock()
		}
	}()
	return resolver.Browse(s.ctx, mdnsServiceName, mdnsDomain, entryChan)
}

func (s *mdnsService) RegisterNotifee(n Notifee) {
	s.mutex.Lock()
	s.notifees = append(s.notifees, n)
	s.mutex.Unlock()
}

func (s *mdnsService) UnregisterNotifee(n Notifee) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	found := -1
	for i, notif := range s.notifees {
		if notif == n {
			found = i
			break
		}
	}
	if found != -1 {
		s.notifees = append(s.notifees[:found], s.notifees[found+1:]...)
	}
}
