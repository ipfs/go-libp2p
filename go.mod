module github.com/libp2p/go-libp2p

go 1.26.0

retract v0.26.1 // Tag was applied incorrectly due to a bug in the release workflow.

retract v0.36.0 // Accidentally modified the tag.

require (
	filippo.io/keygen v1.0.0
	github.com/benbjohnson/clock v1.3.5
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1
	github.com/filecoin-project/go-clock v0.1.0
	github.com/flynn/noise v1.1.0
	github.com/gorilla/websocket v1.5.3
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/huin/goupnp v1.3.0
	github.com/ipfs/go-cid v0.6.2
	github.com/ipfs/go-datastore v0.9.2
	github.com/jackpal/go-nat-pmp v1.0.2
	github.com/jbenet/go-temp-err-catcher v0.1.0
	github.com/koron/go-ssdp v0.9.1
	github.com/libp2p/go-buffer-pool v0.1.0
	github.com/libp2p/go-flow-metrics v0.3.0
	github.com/libp2p/go-libp2p-asn-util v0.4.1
	github.com/libp2p/go-libp2p-testing v0.12.0
	github.com/libp2p/go-msgio v0.3.0
	github.com/libp2p/go-netroute v0.4.0
	github.com/libp2p/go-reuseport v0.4.0
	github.com/libp2p/go-yamux/v5 v5.1.0
	github.com/libp2p/zeroconf/v2 v2.2.0
	github.com/marcopolo/simnet v0.0.7
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b
	github.com/mr-tron/base58 v1.3.0
	github.com/multiformats/go-base32 v0.1.0
	github.com/multiformats/go-multiaddr v0.16.1
	github.com/multiformats/go-multiaddr-dns v0.6.0
	github.com/multiformats/go-multiaddr-fmt v0.1.0
	github.com/multiformats/go-multibase v0.3.0
	github.com/multiformats/go-multicodec v0.10.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/multiformats/go-multistream v0.6.1
	github.com/multiformats/go-varint v0.1.0
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/pion/datachannel v1.5.10
	github.com/pion/ice/v4 v4.0.10
	github.com/pion/logging v0.2.4
	github.com/pion/sctp v1.8.39
	github.com/pion/stun/v3 v3.1.1
	github.com/pion/webrtc/v4 v4.1.2
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.2
	github.com/quic-go/quic-go v0.64.1
	github.com/quic-go/webtransport-go v0.13.0
	github.com/stretchr/testify v1.12.1
	go.uber.org/fx v1.24.0
	go.uber.org/goleak v1.3.0
	go.uber.org/mock v0.6.0
	golang.org/x/crypto v0.54.0
	golang.org/x/net v0.57.0
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0
	golang.org/x/time v0.15.0
	golang.org/x/tools v0.48.0
	google.golang.org/protobuf v1.36.11
)

require (
	filippo.io/bigmod v0.1.1-0.20260103110540-f8a47775ebe5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dunglas/httpsfv v1.1.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pion/dtls/v3 v3.1.2 // indirect
	github.com/pion/interceptor v0.1.40 // indirect
	github.com/pion/mdns/v2 v2.0.7 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.16 // indirect
	github.com/pion/rtp v1.8.19 // indirect
	github.com/pion/sdp/v3 v3.0.18 // indirect
	github.com/pion/srtp/v3 v3.0.6 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/pion/transport/v4 v4.0.1 // indirect
	github.com/pion/turn/v4 v4.0.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	go.uber.org/dig v1.19.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260718201538-764159d718ef // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/telemetry v0.0.0-20260717140457-bdb89881bb75 // indirect
	golang.org/x/text v0.40.0 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
)
