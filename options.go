package libp2p

// This file contains all libp2p configuration options (except the defaults,
// those are in defaults.go)

import (
	"fmt"
	"net"

	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/pnet"

	circuit "github.com/libp2p/go-libp2p-circuit"
	config "github.com/libp2p/go-libp2p/config"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	filter "github.com/libp2p/go-maddr-filter"
	ma "github.com/multiformats/go-multiaddr"
)

// ListenAddrStrings configures libp2p to listen on the given (unparsed)
// addresses.
func ListenAddrStrings(s ...string) Option {
	return func(cfg *Config) error {
		for _, addrstr := range s {
			a, err := ma.NewMultiaddr(addrstr)
			if err != nil {
				return err
			}
			cfg.ListenAddrs = append(cfg.ListenAddrs, a)
		}
		return nil
	}
}

// ListenAddrs configures libp2p to listen on the given addresses.
func ListenAddrs(addrs ...ma.Multiaddr) Option {
	return func(cfg *Config) error {
		cfg.ListenAddrs = append(cfg.ListenAddrs, addrs...)
		return nil
	}
}

// Security configures libp2p to use the given security transport (or transport
// constructor).
//
// Name is the protocol name.
//
// The transport can be a constructed security.Transport or a function taking
// any subset of this libp2p node's:
// * Public key
// * Private key
// * Peer ID
// * Host
// * Network
// * Peerstore
func Security(name string, tpt interface{}) Option {
	stpt, err := config.SecurityConstructor(tpt)
	err = traceError(err, 1)
	return func(cfg *Config) error {
		if err != nil {
			return err
		}
		if cfg.Insecure {
			return fmt.Errorf("cannot use security transports with an insecure libp2p configuration")
		}
		cfg.SecurityTransports = append(cfg.SecurityTransports, config.MsSecC{SecC: stpt, ID: name})
		return nil
	}
}

// NoSecurity is an option that completely disables all transport security.
// It's incompatible with all other transport security protocols.
var NoSecurity Option = func(cfg *Config) error {
	if len(cfg.SecurityTransports) > 0 {
		return fmt.Errorf("cannot use security transports with an insecure libp2p configuration")
	}
	cfg.Insecure = true
	return nil
}

// Muxer configures libp2p to use the given stream multiplexer (or stream
// multiplexer constructor).
//
// Name is the protocol name.
//
// The transport can be a constructed mux.Transport or a function taking any
// subset of this libp2p node's:
// * Peer ID
// * Host
// * Network
// * Peerstore
func Muxer(name string, tpt interface{}) Option {
	mtpt, err := config.MuxerConstructor(tpt)
	err = traceError(err, 1)
	return func(cfg *Config) error {
		if err != nil {
			return err
		}
		cfg.Muxers = append(cfg.Muxers, config.MsMuxC{MuxC: mtpt, ID: name})
		return nil
	}
}

// Transport configures libp2p to use the given transport (or transport
// constructor).
//
// The transport can be a constructed transport.Transport or a function taking
// any subset of this libp2p node's:
// * Transport Upgrader (*tptu.Upgrader)
// * Host
// * Stream muxer (muxer.Transport)
// * Security transport (security.Transport)
// * Private network protector (pnet.Protector)
// * Peer ID
// * Private Key
// * Public Key
// * Address filter (filter.Filter)
// * Peerstore
func Transport(tpt interface{}) Option {
	tptc, err := config.TransportConstructor(tpt)
	err = traceError(err, 1)
	return func(cfg *Config) error {
		if err != nil {
			return err
		}
		cfg.Transports = append(cfg.Transports, tptc)
		return nil
	}
}

// Peerstore configures libp2p to use the given peerstore.
func Peerstore(ps peerstore.Peerstore) Option {
	return func(cfg *Config) error {
		if cfg.Peerstore != nil {
			return fmt.Errorf("cannot specify multiple peerstore options")
		}

		cfg.Peerstore = ps
		return nil
	}
}

// PrivateNetwork configures libp2p to use the given private network protector.
func PrivateNetwork(prot pnet.Protector) Option {
	return func(cfg *Config) error {
		if cfg.Protector != nil {
			return fmt.Errorf("cannot specify multiple private network options")
		}

		cfg.Protector = prot
		return nil
	}
}

// BandwidthReporter configures libp2p to use the given bandwidth reporter.
func BandwidthReporter(rep metrics.Reporter) Option {
	return func(cfg *Config) error {
		if cfg.Reporter != nil {
			return fmt.Errorf("cannot specify multiple bandwidth reporter options")
		}

		cfg.Reporter = rep
		return nil
	}
}

// Identity configures libp2p to use the given private key to identify itself.
func Identity(sk crypto.PrivKey) Option {
	return func(cfg *Config) error {
		if cfg.PeerKey != nil {
			return fmt.Errorf("cannot specify multiple identities")
		}

		cfg.PeerKey = sk
		return nil
	}
}

// ConnectionManager configures libp2p to use the given connection manager.
func ConnectionManager(connman connmgr.ConnManager) Option {
	return func(cfg *Config) error {
		if cfg.ConnManager != nil {
			return fmt.Errorf("cannot specify multiple connection managers")
		}
		cfg.ConnManager = connman
		return nil
	}
}

// AddrsFactory configures libp2p to use the given address factory.
func AddrsFactory(factory config.AddrsFactory) Option {
	return func(cfg *Config) error {
		if cfg.AddrsFactory != nil {
			return fmt.Errorf("cannot specify multiple address factories")
		}
		cfg.AddrsFactory = factory
		return nil
	}
}

// EnableRelay configures libp2p to enable the relay transport with
// configuration options. By default, this option only configures libp2p to
// accept inbound connections from relays and make outbound connections
// _through_ relays when requested by the remote peer. (default: enabled)
//
// To _act_ as a relay, pass the circuit.OptHop option.
func EnableRelay(options ...circuit.RelayOpt) Option {
	return func(cfg *Config) error {
		cfg.RelayCustom = true
		cfg.Relay = true
		cfg.RelayOpts = options
		return nil
	}
}

// DisableRelay configures libp2p to disable the relay transport.
func DisableRelay() Option {
	return func(cfg *Config) error {
		cfg.RelayCustom = true
		cfg.Relay = false
		return nil
	}
}

// EnableAutoRelay configures libp2p to enable the AutoRelay subsystem. It is an
// error to enable AutoRelay without enabling relay (enabled by default) and
// routing (not enabled by default).
//
// This subsystem performs two functions:
//
// 1. When this libp2p node is configured to act as a relay "hop"
//    (circuit.OptHop is passed to EnableRelay), this node will advertise itself
//    as a public relay using the provided routing system.
// 2. When this libp2p node is _not_ configured as a relay "hop", it will
//    automatically detect if it is unreachable (e.g., behind a NAT). If so, it will
//    find, configure, and announce a set of public relays.
func EnableAutoRelay() Option {
	return func(cfg *Config) error {
		cfg.EnableAutoRelay = true
		return nil
	}
}

// FilterAddresses configures libp2p to never dial nor accept connections from
// the given addresses.
func FilterAddresses(addrs ...*net.IPNet) Option {
	return func(cfg *Config) error {
		if cfg.Filters == nil {
			cfg.Filters = filter.NewFilters()
		}
		for _, addr := range addrs {
			cfg.Filters.AddDialFilter(addr)
		}
		return nil
	}
}

// NATPortMap configures libp2p to use the default NATManager. The default
// NATManager will attempt to open a port in your network's firewall using UPnP.
func NATPortMap() Option {
	return NATManager(bhost.NewNATManager)
}

// NATManager will configure libp2p to use the requested NATManager. This
// function should be passed a NATManager *constructor* that takes a libp2p Network.
func NATManager(nm config.NATManagerC) Option {
	return func(cfg *Config) error {
		if cfg.NATManager != nil {
			return fmt.Errorf("cannot specify multiple NATManagers")
		}
		cfg.NATManager = nm
		return nil
	}
}

// Ping will configure libp2p to support the ping service; enable by default.
func Ping(enable bool) Option {
	return func(cfg *Config) error {
		cfg.DisablePing = !enable
		return nil
	}
}

// Routing will configure libp2p to use routing.
func Routing(rt config.RoutingC) Option {
	return func(cfg *Config) error {
		if cfg.Routing != nil {
			return fmt.Errorf("cannot specify multiple routing options")
		}
		cfg.Routing = rt
		return nil
	}
}

// EnableSelfDial will configure libp2p to support self dialing; disabled by
// default.
var EnableSelfDial Option = func(cfg *Config) error {
	cfg.SelfDial = true
	return nil
}

// NoListenAddrs will configure libp2p to not listen by default.
//
// This will both clear any configured listen addrs and prevent libp2p from
// applying the default listen address option. It also disables relay, unless the
// user explicitly specifies with an option, as the transport creates an implicit
// listen address that would make the node dialable through any relay it was connected to.
var NoListenAddrs = func(cfg *Config) error {
	cfg.ListenAddrs = []ma.Multiaddr{}
	if !cfg.RelayCustom {
		cfg.RelayCustom = true
		cfg.Relay = false
	}
	return nil
}

// NoTransports will configure libp2p to not enable any transports.
//
// This will both clear any configured transports (specified in prior libp2p
// options) and prevent libp2p from applying the default transports.
var NoTransports = func(cfg *Config) error {
	cfg.Transports = []config.TptC{}
	return nil
}
