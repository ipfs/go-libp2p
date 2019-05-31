module github.com/libp2p/go-libp2p

require (
	github.com/gogo/protobuf v1.2.1
	github.com/ipfs/go-cid v0.0.2
	github.com/ipfs/go-detect-race v0.0.1
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-log v0.0.1
	github.com/jbenet/go-cienv v0.1.0
	github.com/jbenet/goprocess v0.1.3
	github.com/libp2p/go-conn-security-multistream v0.1.0
	github.com/libp2p/go-libp2p-autonat v0.1.0
	github.com/libp2p/go-libp2p-blankhost v0.1.1
	github.com/libp2p/go-libp2p-circuit v0.1.0
	github.com/libp2p/go-libp2p-core v0.0.1
	github.com/libp2p/go-libp2p-crypto v0.1.0
	github.com/libp2p/go-libp2p-discovery v0.1.0
	github.com/libp2p/go-libp2p-loggables v0.1.0
	github.com/libp2p/go-libp2p-mplex v0.2.1
	github.com/libp2p/go-libp2p-nat v0.0.4
	github.com/libp2p/go-libp2p-netutil v0.1.0
	github.com/libp2p/go-libp2p-peer v0.2.0
	github.com/libp2p/go-libp2p-peerstore v0.1.0
	github.com/libp2p/go-libp2p-secio v0.1.0
	github.com/libp2p/go-libp2p-swarm v0.1.0
	github.com/libp2p/go-libp2p-testing v0.0.3
	github.com/libp2p/go-libp2p-transport v0.1.0
	github.com/libp2p/go-libp2p-transport-upgrader v0.1.1
	github.com/libp2p/go-libp2p-yamux v0.2.0
	github.com/libp2p/go-maddr-filter v0.0.4
	github.com/libp2p/go-stream-muxer v0.1.0
	github.com/libp2p/go-stream-muxer-multistream v0.2.0
	github.com/libp2p/go-tcp-transport v0.1.0
	github.com/libp2p/go-ws-transport v0.1.0
	github.com/miekg/dns v1.1.12 // indirect
	github.com/multiformats/go-multiaddr v0.0.4
	github.com/multiformats/go-multiaddr-dns v0.0.2
	github.com/multiformats/go-multiaddr-net v0.0.1
	github.com/multiformats/go-multistream v0.1.0
	github.com/whyrusleeping/mdns v0.0.0-20180901202407-ef14215e6b30
)

replace github.com/libp2p/go-libp2p-swarm => ../go-libp2p-swarm

replace github.com/multiformats/go-multiaddr => ../../workspace-multiformats/go-multiaddr
