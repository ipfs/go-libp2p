package config

import (
	"context"
	"crypto/sha256"
	"io"
	"net"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"golang.org/x/crypto/hkdf"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/quic-go/quic-go"
	"go.uber.org/fx"
)

const (
	statelessResetKeyInfo = "libp2p quic stateless reset key"
	tokenGeneratorKeyInfo = "libp2p quic token generator key"
)

// PrivKeyToStatelessResetKey derives a QUIC stateless reset key from a libp2p private key.
func PrivKeyToStatelessResetKey(key crypto.PrivKey) (quic.StatelessResetKey, error) {
	var statelessResetKey quic.StatelessResetKey
	keyBytes, err := key.Raw()
	if err != nil {
		return statelessResetKey, err
	}
	keyReader := hkdf.New(sha256.New, keyBytes, nil, []byte(statelessResetKeyInfo))
	if _, err := io.ReadFull(keyReader, statelessResetKey[:]); err != nil {
		return statelessResetKey, err
	}
	return statelessResetKey, nil
}

// PrivKeyToTokenGeneratorKey derives a QUIC token generator key from a libp2p private key.
func PrivKeyToTokenGeneratorKey(key crypto.PrivKey) (quic.TokenGeneratorKey, error) {
	var tokenKey quic.TokenGeneratorKey
	keyBytes, err := key.Raw()
	if err != nil {
		return tokenKey, err
	}
	keyReader := hkdf.New(sha256.New, keyBytes, nil, []byte(tokenGeneratorKeyInfo))
	if _, err := io.ReadFull(keyReader, tokenKey[:]); err != nil {
		return tokenKey, err
	}
	return tokenKey, nil
}

// provideQUICReuse provides the QUIC connection manager with all necessary dependencies.
// This is separated to avoid importing quic-go when QUIC transport is not used.
func provideQUICReuse(cfg *Config) fx.Option {
	opts := []fx.Option{
		fx.Provide(PrivKeyToStatelessResetKey),
		fx.Provide(PrivKeyToTokenGeneratorKey),
	}

	if cfg.QUICReuse != nil {
		opts = append(opts, cfg.QUICReuse...)
		return fx.Options(opts...)
	}

	opts = append(opts, fx.Provide(func(key quic.StatelessResetKey, tokenGenerator quic.TokenGeneratorKey, rcmgr network.ResourceManager, lifecycle fx.Lifecycle) (*quicreuse.ConnManager, error) {
		quicOpts := []quicreuse.Option{
			quicreuse.ConnContext(func(ctx context.Context, clientInfo *quic.ClientInfo) (context.Context, error) {
				// even if creating the quic maddr fails, let the rcmgr decide what to do with the connection
				addr, err := quicreuse.ToQuicMultiaddr(clientInfo.RemoteAddr, quic.Version1)
				if err != nil {
					addr = nil
				}
				scope, err := rcmgr.OpenConnection(network.DirInbound, false, addr)
				if err != nil {
					return ctx, err
				}
				ctx = network.WithConnManagementScope(ctx, scope)
				context.AfterFunc(ctx, func() {
					scope.Done()
				})
				return ctx, nil
			}),
			quicreuse.VerifySourceAddress(func(addr net.Addr) bool {
				return rcmgr.VerifySourceAddress(addr)
			}),
		}
		if !cfg.DisableMetrics {
			quicOpts = append(quicOpts, quicreuse.EnableMetrics(cfg.PrometheusRegisterer))
		}
		cm, err := quicreuse.NewConnManager(key, tokenGenerator, quicOpts...)
		if err != nil {
			return nil, err
		}
		lifecycle.Append(fx.StopHook(cm.Close))
		return cm, nil
	}))

	return fx.Options(opts...)
}

// provideWebRTCListenUDP provides the UDP listener function for WebRTC transport.
// This is separated to avoid importing quic-go when WebRTC transport is not used.
func provideWebRTCListenUDP() fx.Option {
	return fx.Provide(func(cm *quicreuse.ConnManager, sw interface{ ListenAddresses() []ma.Multiaddr }) libp2pwebrtc.ListenUDPFn {
		hasQuicAddrPortFor := func(network string, laddr *net.UDPAddr) bool {
			quicAddrPorts := map[string]struct{}{}
			for _, addr := range sw.ListenAddresses() {
				if _, err := addr.ValueForProtocol(ma.P_QUIC_V1); err == nil {
					netw, addr, err := manet.DialArgs(addr)
					if err != nil {
						return false
					}
					quicAddrPorts[netw+"_"+addr] = struct{}{}
				}
			}
			_, ok := quicAddrPorts[network+"_"+laddr.String()]
			return ok
		}

		return func(network string, laddr *net.UDPAddr) (net.PacketConn, error) {
			if hasQuicAddrPortFor(network, laddr) {
				return cm.SharedNonQUICPacketConn(network, laddr)
			}
			return net.ListenUDP(network, laddr)
		}
	})
}
