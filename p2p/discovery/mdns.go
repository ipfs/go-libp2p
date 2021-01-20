package discovery

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	logging "github.com/ipfs/go-log"
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

type mdnsService struct {
	server  *mdns.Server
	service *mdns.MDNSService
	host    host.Host
	tag     string

	ifaceName string

	lk       sync.Mutex
	notifees []Notifee
	interval time.Duration
}

type MdnsServiceInit struct {
	Interval  time.Duration // time between mDNS requests. Default: 30 seconds
	IfaceName string        // network interface name, such as "wlan0". Empty string "" is the default to listen to any  TCP interface.
	Tag       string        // peers find themselves using on this common tag. Default is Tag = "_ipfs-discovery._udp"
}

func getDialableListenAddrs(ph host.Host, iface *net.Interface) ([]*net.TCPAddr, error) {
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

		// if `iface` is specified, use address from `iface`
		// otherwise get any TCP-capable address.
		if iface != nil {
			ips, err := iface.Addrs()
			if err != nil {
				return nil, err
			}

			for _, i := range ips {
				if strings.Contains(na.String(), strings.Split(i.String(), "/")[0]) {
					if tcp, ok := na.(*net.TCPAddr); ok {
						out = append(out, tcp)
						break
					}
				}
			}
		} else {
			if tcp, ok := na.(*net.TCPAddr); ok {
				out = append(out, tcp)
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("failed to find good external addr from peerhost")
	}
	return out, nil
}

func NewMdnsService(ctx context.Context, peerhost host.Host, options ...func(*MdnsServiceInit)) (Service, error) {
	var ipaddrs []net.IP
	port := 4001

	// Init options with default and call user-defined functions to override the default values
	opts := MdnsServiceInit{Interval: time.Duration(30) * time.Second, IfaceName: "", Tag: ServiceTag}
	for _, option := range options {
		option(&opts)
	}

	// Get interface from name passed in options if any
	var iface *net.Interface
	if opts.IfaceName != "" {
		var err error
		iface, err = net.InterfaceByName(opts.IfaceName)
		if err != nil {
			return nil, err
		}
	}

	addrs, err := getDialableListenAddrs(peerhost, iface)
	if err != nil {
		log.Warning(err)
	} else {
		port = addrs[0].Port
		for _, a := range addrs {
			ipaddrs = append(ipaddrs, a.IP)
		}
	}

	myid := peerhost.ID().Pretty()

	info := []string{myid}
	service, err := mdns.NewMDNSService(myid, opts.Tag, "", "", port, ipaddrs, info)
	if err != nil {
		return nil, err
	}

	// Create the mDNS server, defer shutdown
	server, err := mdns.NewServer(&mdns.Config{Zone: service, Iface: iface})
	if err != nil {
		return nil, err
	}

	s := &mdnsService{
		server:    server,
		service:   service,
		host:      peerhost,
		interval:  opts.Interval,
		tag:       opts.Tag,
		ifaceName: opts.IfaceName,
	}

	go s.pollForEntries(ctx)

	return s, nil
}

func (m *mdnsService) Close() error {
	return m.server.Shutdown()
}

func (m *mdnsService) pollForEntries(ctx context.Context) {
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

		// get interface from name, if any, and verify the interface is up before sending a new query
		var iface *net.Interface
		var err error
		if m.ifaceName != "" {
			iface, err = net.InterfaceByName(m.ifaceName)
			if err != nil {
				log.Error("Cannot find interface: ", err)
			} else if iface != nil && (iface.Flags&net.FlagUp) != 0 && (iface.Flags&net.FlagMulticast) != 0 && (iface.Flags&net.FlagLoopback) == 0 {
				log.Debug("Starting mdns query on interface ", m.ifaceName)
			} else {
				log.Warn("Interface ", m.ifaceName, " is either down or loopback: ", iface.Flags)
				iface = nil
			}
		}

		qp := &mdns.QueryParam{
			Domain:    "local",
			Entries:   entriesCh,
			Service:   m.tag,
			Timeout:   time.Second * 5,
			Interface: iface,
		}

		err = mdns.Query(qp)
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

func (m *mdnsService) handleEntry(e *mdns.ServiceEntry) {
	log.Debugf("Handling MDNS entry: [IPv4 %s][IPv6 %s]:%d %s", e.AddrV4, e.AddrV6, e.Port, e.Info)
	mpeer, err := peer.IDB58Decode(e.Info)
	if err != nil {
		log.Warning("Error parsing peer ID from mdns entry: ", err)
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
		log.Warning("Error parsing multiaddr from mdns entry: no IP address found")
		return
	}

	maddr, err := manet.FromNetAddr(&net.TCPAddr{
		IP:   addr,
		Port: e.Port,
	})
	if err != nil {
		log.Warning("Error parsing multiaddr from mdns entry: ", err)
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
