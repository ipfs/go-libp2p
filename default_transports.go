package libp2p

// This file contains default transport configurations.
// By separating these from defaults.go, we avoid forcing imports of all
// transport packages when users only need a subset of transports.

import (
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	ws "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
)

// DefaultTransports are the default libp2p transports.
//
// Use this option when you want to *extend* the set of transports used by
// libp2p instead of replacing them.
var DefaultTransports = ChainOptions(
	Transport(tcp.NewTCPTransport),
	Transport(quic.NewTransport),
	Transport(ws.New),
	Transport(webtransport.New),
	Transport(libp2pwebrtc.New),
)

// DefaultPrivateTransports are the default libp2p transports when a PSK is supplied.
//
// Use this option when you want to *extend* the set of transports used by
// libp2p instead of replacing them.
var DefaultPrivateTransports = ChainOptions(
	Transport(tcp.NewTCPTransport),
	Transport(ws.New),
)
