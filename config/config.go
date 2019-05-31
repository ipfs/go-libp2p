package config

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/pnet"
	"github.com/libp2p/go-libp2p-core/routing"

	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	relay "github.com/libp2p/go-libp2p/p2p/host/relay"
	routed "github.com/libp2p/go-libp2p/p2p/host/routed"
	pipetransport "github.com/libp2p/go-libp2p/p2p/transport/pipe"

	circuit "github.com/libp2p/go-libp2p-circuit"
	discovery "github.com/libp2p/go-libp2p-discovery"
	swarm "github.com/libp2p/go-libp2p-swarm"
	tptu "github.com/libp2p/go-libp2p-transport-upgrader"

	logging "github.com/ipfs/go-log"
	filter "github.com/libp2p/go-maddr-filter"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("p2p-config")

// AddrsFactory is a function that takes a set of multiaddrs we're listening on and
// returns the set of multiaddrs we should advertise to the network.
type AddrsFactory = bhost.AddrsFactory

// NATManagerC is a NATManager constructor.
type NATManagerC func(network.Network) bhost.NATManager

type RoutingC func(host.Host) (routing.PeerRouting, error)

// Config describes a set of settings for a libp2p node
//
// This is *not* a stable interface. Use the options defined in the root
// package.
type Config struct {
	PeerKey crypto.PrivKey

	Transports         []TptC
	Muxers             []MsMuxC
	SecurityTransports []MsSecC
	Insecure           bool
	Protector          pnet.Protector

	RelayCustom bool
	Relay       bool
	RelayOpts   []circuit.RelayOpt

	ListenAddrs  []ma.Multiaddr
	AddrsFactory bhost.AddrsFactory
	Filters      *filter.Filters

	ConnManager connmgr.ConnManager
	NATManager  NATManagerC
	Peerstore   peerstore.Peerstore
	Reporter    metrics.Reporter

	DisablePing bool
	SelfDial    bool

	Routing RoutingC

	EnableAutoRelay bool
}

// NewNode constructs a new libp2p Host from the Config.
//
// This function consumes the config. Do not reuse it (really!).
func (cfg *Config) NewNode(ctx context.Context) (host.Host, error) {
	// Check this early. Prevents us from even *starting* without verifying this.
	if pnet.ForcePrivateNetwork && cfg.Protector == nil {
		log.Error("tried to create a libp2p node with no Private" +
			" Network Protector but usage of Private Networks" +
			" is forced by the enviroment")
		// Note: This is *also* checked the upgrader itself so it'll be
		// enforced even *if* you don't use the libp2p constructor.
		return nil, pnet.ErrNotInPrivateNetwork
	}

	if cfg.PeerKey == nil {
		return nil, fmt.Errorf("no peer key specified")
	}

	// Obtain Peer ID from public key
	pid, err := peer.IDFromPublicKey(cfg.PeerKey.GetPublic())
	if err != nil {
		return nil, err
	}

	if cfg.Peerstore == nil {
		return nil, fmt.Errorf("no peerstore specified")
	}

	if !cfg.Insecure {
		if err := cfg.Peerstore.AddPrivKey(pid, cfg.PeerKey); err != nil {
			return nil, err
		}
		if err := cfg.Peerstore.AddPubKey(pid, cfg.PeerKey.GetPublic()); err != nil {
			return nil, err
		}
	}

	// TODO: Make the swarm implementation configurable.
	swrm := swarm.NewSwarm(ctx, pid, cfg.Peerstore, cfg.Reporter, cfg.SelfDial)
	if cfg.Filters != nil {
		swrm.Filters = cfg.Filters
	}

	h, err := bhost.NewHost(ctx, swrm, &bhost.HostOpts{
		ConnManager:  cfg.ConnManager,
		AddrsFactory: cfg.AddrsFactory,
		NATManager:   cfg.NATManager,
		EnablePing:   !cfg.DisablePing,
	})

	if err != nil {
		swrm.Close()
		return nil, err
	}

	if cfg.Relay {
		// If we've enabled the relay, we should filter out relay
		// addresses by default.
		//
		// TODO: We shouldn't be doing this here.
		oldFactory := h.AddrsFactory
		h.AddrsFactory = func(addrs []ma.Multiaddr) []ma.Multiaddr {
			return oldFactory(relay.Filter(addrs))
		}
	}

	upgrader := new(tptu.Upgrader)
	upgrader.Protector = cfg.Protector
	upgrader.Filters = swrm.Filters
	if cfg.Insecure {
		upgrader.Secure = makeInsecureTransport(pid)
	} else {
		upgrader.Secure, err = makeSecurityTransport(h, cfg.SecurityTransports)
		if err != nil {
			h.Close()
			return nil, err
		}
	}

	upgrader.Muxer, err = makeMuxer(h, cfg.Muxers)
	if err != nil {
		h.Close()
		return nil, err
	}

	tpts, err := makeTransports(h, upgrader, cfg.Transports)
	if err != nil {
		h.Close()
		return nil, err
	}
	if cfg.SelfDial {
		tpt := pipetransport.New(pid, cfg.PeerKey.GetPublic(), cfg.PeerKey)
		tpts = append(tpts, tpt)
	}
	for _, t := range tpts {
		err = swrm.AddTransport(t)
		if err != nil {
			h.Close()
			return nil, err
		}
	}

	if cfg.Relay {
		err := circuit.AddRelayTransport(swrm.Context(), h, upgrader, cfg.RelayOpts...)
		if err != nil {
			h.Close()
			return nil, err
		}
	}

	// TODO: This method succeeds if listening on one address succeeds. We
	// should probably fail if listening on *any* addr fails.
	if err := h.Network().Listen(cfg.ListenAddrs...); err != nil {
		h.Close()
		return nil, err
	}

	// Configure routing and autorelay
	var router routing.PeerRouting
	if cfg.Routing != nil {
		router, err = cfg.Routing(h)
		if err != nil {
			h.Close()
			return nil, err
		}
	}

	if cfg.EnableAutoRelay {
		if !cfg.Relay {
			h.Close()
			return nil, fmt.Errorf("cannot enable autorelay; relay is not enabled")
		}

		if router == nil {
			h.Close()
			return nil, fmt.Errorf("cannot enable autorelay; no routing for discovery")
		}

		crouter, ok := router.(routing.ContentRouting)
		if !ok {
			h.Close()
			return nil, fmt.Errorf("cannot enable autorelay; no suitable routing for discovery")
		}

		discovery := discovery.NewRoutingDiscovery(crouter)

		hop := false
		for _, opt := range cfg.RelayOpts {
			if opt == circuit.OptHop {
				hop = true
				break
			}
		}

		if hop {
			// advertise ourselves
			relay.Advertise(ctx, discovery)
		} else {
			_ = relay.NewAutoRelay(swrm.Context(), h, discovery, router)
		}
	}

	// start the host background tasks
	h.Start()

	if router != nil {
		return routed.Wrap(h, router), nil
	}
	return h, nil
}

// Option is a libp2p config option that can be given to the libp2p constructor
// (`libp2p.New`).
type Option func(cfg *Config) error

// Apply applies the given options to the config, returning the first error
// encountered (if any).
func (cfg *Config) Apply(opts ...Option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(cfg); err != nil {
			return err
		}
	}
	return nil
}
