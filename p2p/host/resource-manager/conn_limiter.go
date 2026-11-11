package rcmgr

import (
	"math"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/x/rate"
)

type ConnLimitPerSubnet struct {
	// This defines how big the subnet is. For example, a /24 subnet has a
	// PrefixLength of 24. All IPs that share the same 24 bit prefix are in the same
	// subnet. Are in the same subnet, and bound to the same limit.
	PrefixLength int
	// The maximum number of connections allowed for each subnet.
	ConnCount int
}

type NetworkPrefixLimit struct {
	// The Network prefix for which this limit applies.
	Network netip.Prefix

	// The maximum number of connections allowed for this subnet.
	ConnCount int
}

// 8 for now so that it matches the number of concurrent dials we may do
// in swarm_dial.go. With future smart dialing work we should bring this
// down
var defaultMaxConcurrentConns = 8

var defaultIP4Limit = ConnLimitPerSubnet{
	ConnCount:    defaultMaxConcurrentConns,
	PrefixLength: 32,
}
var defaultIP6Limits = []ConnLimitPerSubnet{
	{
		ConnCount:    defaultMaxConcurrentConns,
		PrefixLength: 56,
	},
	{
		ConnCount:    8 * defaultMaxConcurrentConns,
		PrefixLength: 48,
	},
}

var DefaultNetworkPrefixLimitV4 = sortNetworkPrefixes([]NetworkPrefixLimit{
	{
		// Loopback address for v4 https://datatracker.ietf.org/doc/html/rfc6890#section-2.2.2
		Network:   netip.MustParsePrefix("127.0.0.0/8"),
		ConnCount: math.MaxInt, // Unlimited
	},
})
var DefaultNetworkPrefixLimitV6 = sortNetworkPrefixes([]NetworkPrefixLimit{
	{
		// Loopback address for v6 https://datatracker.ietf.org/doc/html/rfc6890#section-2.2.3
		Network:   netip.MustParsePrefix("::1/128"),
		ConnCount: math.MaxInt, // Unlimited
	},
})

// Network prefixes limits must be sorted by most specific to least specific.  This lets us
// actually use the more specific limits, otherwise only the less specific ones
// would be matched. e.g. 1.2.3.0/24 must come before 1.2.0.0/16.
func sortNetworkPrefixes(limits []NetworkPrefixLimit) []NetworkPrefixLimit {
	slices.SortStableFunc(limits, func(a, b NetworkPrefixLimit) int {
		return b.Network.Bits() - a.Network.Bits()
	})
	return limits
}

// WithNetworkPrefixLimit sets the limits for the number of connections allowed
// for a specific Network Prefix. Use this when you want to set higher limits
// for a specific subnet than the default limit per subnet.
func WithNetworkPrefixLimit(ipv4 []NetworkPrefixLimit, ipv6 []NetworkPrefixLimit) Option {
	return func(rm *resourceManager) error {
		if ipv4 != nil {
			rm.connLimiter.networkPrefixLimitV4 = sortNetworkPrefixes(ipv4)
		}
		if ipv6 != nil {
			rm.connLimiter.networkPrefixLimitV6 = sortNetworkPrefixes(ipv6)
		}
		return nil
	}
}

// WithLimitPerSubnet sets the limits for the number of connections allowed per
// subnet. This will limit the number of connections per subnet if that subnet
// is not defined in the NetworkPrefixLimit option. Think of this as a default
// limit for any given subnet.
func WithLimitPerSubnet(ipv4 []ConnLimitPerSubnet, ipv6 []ConnLimitPerSubnet) Option {
	return func(rm *resourceManager) error {
		if ipv4 != nil {
			rm.connLimiter.connLimitPerSubnetV4 = ipv4
		}
		if ipv6 != nil {
			rm.connLimiter.connLimitPerSubnetV6 = ipv6
		}
		return nil
	}
}

// WithDisableConnLimits disables connection limiting per IP/subnet.
// Useful with InfiniteLimits for testing/debugging.
// Warning: removes DoS protection, only use in trusted environments.
func WithDisableConnLimits() Option {
	return WithLimitPerSubnet(
		[]ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: math.MaxInt}},
		[]ConnLimitPerSubnet{{PrefixLength: 128, ConnCount: math.MaxInt}},
	)
}

type connLimiter struct {
	mu sync.Mutex

	// Specific Network Prefix limits. If these are set, they take precedence over the
	// subnet limits.
	// These must be sorted by most specific to least specific.
	networkPrefixLimitV4    []NetworkPrefixLimit
	networkPrefixLimitV6    []NetworkPrefixLimit
	connsPerNetworkPrefixV4 []int
	connsPerNetworkPrefixV6 []int

	// Subnet limits.
	connLimitPerSubnetV4 []ConnLimitPerSubnet
	connLimitPerSubnetV6 []ConnLimitPerSubnet
	ip4connsPerLimit     []map[netip.Prefix]int
	ip6connsPerLimit     []map[netip.Prefix]int
}

func newConnLimiter() *connLimiter {
	return &connLimiter{
		networkPrefixLimitV4: DefaultNetworkPrefixLimitV4,
		networkPrefixLimitV6: DefaultNetworkPrefixLimitV6,

		connLimitPerSubnetV4: []ConnLimitPerSubnet{defaultIP4Limit},
		connLimitPerSubnetV6: defaultIP6Limits,
	}
}

func (cl *connLimiter) addNetworkPrefixLimit(isIP6 bool, npLimit NetworkPrefixLimit) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if isIP6 {
		cl.networkPrefixLimitV6 = append(cl.networkPrefixLimitV6, npLimit)
		cl.networkPrefixLimitV6 = sortNetworkPrefixes(cl.networkPrefixLimitV6)
	} else {
		cl.networkPrefixLimitV4 = append(cl.networkPrefixLimitV4, npLimit)
		cl.networkPrefixLimitV4 = sortNetworkPrefixes(cl.networkPrefixLimitV4)
	}
}

// addConn adds a connection for the given IP address. It returns true if the connection is allowed.
func (cl *connLimiter) addConn(ip netip.Addr) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	networkPrefixLimits := cl.networkPrefixLimitV4
	connsPerNetworkPrefix := cl.connsPerNetworkPrefixV4
	limits := cl.connLimitPerSubnetV4
	connsPerLimit := cl.ip4connsPerLimit
	isIP6 := ip.Is6()
	if isIP6 {
		networkPrefixLimits = cl.networkPrefixLimitV6
		connsPerNetworkPrefix = cl.connsPerNetworkPrefixV6
		limits = cl.connLimitPerSubnetV6
		connsPerLimit = cl.ip6connsPerLimit
	}

	// Check Network Prefix limits first
	if len(connsPerNetworkPrefix) == 0 && len(networkPrefixLimits) > 0 {
		// Initialize the counts
		connsPerNetworkPrefix = make([]int, len(networkPrefixLimits))
		if isIP6 {
			cl.connsPerNetworkPrefixV6 = connsPerNetworkPrefix
		} else {
			cl.connsPerNetworkPrefixV4 = connsPerNetworkPrefix
		}
	}

	for i, limit := range networkPrefixLimits {
		if limit.Network.Contains(ip) {
			if connsPerNetworkPrefix[i]+1 > limit.ConnCount {
				return false
			}
			connsPerNetworkPrefix[i]++
			// Done. If we find a match in the network prefix limits, we use
			// that and don't use the general subnet limits.
			return true
		}
	}

	if len(connsPerLimit) == 0 && len(limits) > 0 {
		connsPerLimit = make([]map[netip.Prefix]int, len(limits))
		if isIP6 {
			cl.ip6connsPerLimit = connsPerLimit
		} else {
			cl.ip4connsPerLimit = connsPerLimit
		}
	}

	for i, limit := range limits {
		prefix, err := ip.Prefix(limit.PrefixLength)
		if err != nil {
			return false
		}
		counts, ok := connsPerLimit[i][prefix]
		if !ok {
			if connsPerLimit[i] == nil {
				connsPerLimit[i] = make(map[netip.Prefix]int)
			}
			connsPerLimit[i][prefix] = 0
		}
		if counts+1 > limit.ConnCount {
			return false
		}
	}

	// All limit checks passed, now we update the counts
	for i, limit := range limits {
		prefix, _ := ip.Prefix(limit.PrefixLength)
		connsPerLimit[i][prefix]++
	}

	return true
}

func (cl *connLimiter) rmConn(ip netip.Addr) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	networkPrefixLimits := cl.networkPrefixLimitV4
	connsPerNetworkPrefix := cl.connsPerNetworkPrefixV4
	limits := cl.connLimitPerSubnetV4
	connsPerLimit := cl.ip4connsPerLimit
	isIP6 := ip.Is6()
	if isIP6 {
		networkPrefixLimits = cl.networkPrefixLimitV6
		connsPerNetworkPrefix = cl.connsPerNetworkPrefixV6
		limits = cl.connLimitPerSubnetV6
		connsPerLimit = cl.ip6connsPerLimit
	}

	// Check NetworkPrefix limits first
	if len(connsPerNetworkPrefix) == 0 && len(networkPrefixLimits) > 0 {
		// Initialize just in case. We should have already initialized in
		// addConn, but if the callers calls rmConn first we don't want to panic
		connsPerNetworkPrefix = make([]int, len(networkPrefixLimits))
		if isIP6 {
			cl.connsPerNetworkPrefixV6 = connsPerNetworkPrefix
		} else {
			cl.connsPerNetworkPrefixV4 = connsPerNetworkPrefix
		}
	}
	for i, limit := range networkPrefixLimits {
		if limit.Network.Contains(ip) {
			count := connsPerNetworkPrefix[i]
			if count <= 0 {
				log.Error("unexpected conn count for ip. Was this not added with addConn first?", "ip", ip)
				return
			}
			connsPerNetworkPrefix[i]--
			// Done. We updated the count in the defined network prefix limit.
			return
		}
	}

	if len(connsPerLimit) == 0 && len(limits) > 0 {
		// Initialize just in case. We should have already initialized in
		// addConn, but if the callers calls rmConn first we don't want to panic
		connsPerLimit = make([]map[netip.Prefix]int, len(limits))
		if isIP6 {
			cl.ip6connsPerLimit = connsPerLimit
		} else {
			cl.ip4connsPerLimit = connsPerLimit
		}
	}

	for i, limit := range limits {
		prefix, err := ip.Prefix(limit.PrefixLength)
		if err != nil {
			// Unexpected since we should have seen this IP before in addConn
			log.Error("unexpected error getting prefix", "err", err)
			continue
		}
		counts, ok := connsPerLimit[i][prefix]
		if !ok || counts == 0 {
			// Unexpected, but don't panic
			log.Error("unexpected conn count", "prefix", prefix, "ok", ok, "count", counts)
			continue
		}
		connsPerLimit[i][prefix]--
		if connsPerLimit[i][prefix] <= 0 {
			delete(connsPerLimit[i], prefix)
		}
	}
}

// handshakeDuration is a higher end estimate of QUIC handshake time
const handshakeDuration = 5 * time.Second

// sourceAddressRPS is the refill rate for the source address verification rate limiter.
// A spoofed address if not verified will take a connLimiter token for handshakeDuration.
// Slow refill rate here favours increasing latency(because of address verification) in
// exchange for reducing the chances of spoofing successfully causing a DoS.
const sourceAddressRPS = float64(1.0*time.Second) / (2 * float64(handshakeDuration))

// newVerifySourceAddressRateLimiter returns a rate limiter for verifying source addresses.
// The returned limiter allows maxAllowedConns / 2 unverified addresses to begin handshake.
// This ensures that in the event someone is spoofing IPs, 1/2 the maximum allowed connections
// will be able to connect, although they will have increased latency because of address
// verification.
func newVerifySourceAddressRateLimiter(cl *connLimiter) *rate.Limiter {
	networkPrefixLimits := make([]rate.PrefixLimit, 0, len(cl.networkPrefixLimitV4)+len(cl.networkPrefixLimitV6))
	for _, l := range cl.networkPrefixLimitV4 {
		networkPrefixLimits = append(networkPrefixLimits, rate.PrefixLimit{
			Prefix: l.Network,
			Limit:  rate.Limit{RPS: sourceAddressRPS, Burst: l.ConnCount / 2},
		})
	}
	for _, l := range cl.networkPrefixLimitV6 {
		networkPrefixLimits = append(networkPrefixLimits, rate.PrefixLimit{
			Prefix: l.Network,
			Limit:  rate.Limit{RPS: sourceAddressRPS, Burst: l.ConnCount / 2},
		})
	}

	ipv4SubnetLimits := make([]rate.SubnetLimit, 0, len(cl.connLimitPerSubnetV4))
	for _, l := range cl.connLimitPerSubnetV4 {
		ipv4SubnetLimits = append(ipv4SubnetLimits, rate.SubnetLimit{
			PrefixLength: l.PrefixLength,
			Limit:        rate.Limit{RPS: sourceAddressRPS, Burst: l.ConnCount / 2},
		})
	}

	ipv6SubnetLimits := make([]rate.SubnetLimit, 0, len(cl.connLimitPerSubnetV6))
	for _, l := range cl.connLimitPerSubnetV6 {
		ipv6SubnetLimits = append(ipv6SubnetLimits, rate.SubnetLimit{
			PrefixLength: l.PrefixLength,
			Limit:        rate.Limit{RPS: sourceAddressRPS, Burst: l.ConnCount / 2},
		})
	}

	return &rate.Limiter{
		NetworkPrefixLimits: networkPrefixLimits,
		SubnetRateLimiter: rate.SubnetLimiter{
			IPv4SubnetLimits: ipv4SubnetLimits,
			IPv6SubnetLimits: ipv6SubnetLimits,
			GracePeriod:      1 * time.Minute,
		},
	}
}
