// Package rate provides rate limiting functionality at a global, network, and subnet level.
package rate

import (
	"container/heap"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/time/rate"
)

// Limit is the configuration for a token bucket rate limiter.
// The bucket has a capacity of Burst, and is refilled at a rate of RPS tokens per second.
// Initially, buckets are completely full, i.e. the number of tokens in the bucket is equal to `Burst`.
// In any given time interval T seconds, maximum events allowed will be `T*RPS + Burst`.
type Limit struct {
	// RPS is the rate of requests per second in steady state.
	RPS float64
	// Burst is the number of requests allowed over the RPS.
	Burst int
}

// PrefixLimit is a rate limit configuration that applies to a specific network prefix.
type PrefixLimit struct {
	Prefix netip.Prefix
	Limit
}

// SubnetLimit is a rate limit configuration that applies to a specific subnet.
type SubnetLimit struct {
	PrefixLength int
	Limit
}

// Limiter rate limits new streams for a service. It allows setting NetworkPrefix specific,
// global, and subnet specific limits. Use 0 for no rate limiting.
// The limiter maintains state that must be periodically cleaned up using Cleanup
type Limiter struct {
	// NetworkPrefixLimits are limits for streams with peer IPs belonging to specific subnets.
	// It can be used to increase the limit for trusted networks and decrease the limit for specific networks.
	NetworkPrefixLimits []PrefixLimit
	// GlobalLimit is the limit for all streams where the peer IP doesn't fall within any
	// of the `NetworkPrefixLimits`
	GlobalLimit Limit
	// SubnetRateLimiter is a rate limiter for subnets.
	SubnetRateLimiter SubnetLimiter

	initOnce             sync.Once
	globalBucket         *rate.Limiter
	networkPrefixBuckets []*rate.Limiter // ith element ratelimits ith NetworkPrefixLimits
}

func (r *Limiter) init() {
	r.initOnce.Do(func() {
		if r.GlobalLimit.RPS == 0 {
			r.globalBucket = rate.NewLimiter(rate.Inf, 0)
		} else {
			r.globalBucket = rate.NewLimiter(rate.Limit(r.GlobalLimit.RPS), r.GlobalLimit.Burst)
		}
		// clone the slice in case it's shared with other limiters
		r.NetworkPrefixLimits = slices.Clone(r.NetworkPrefixLimits)
		// sort such that the widest prefix (smallest bit count) is last.
		slices.SortFunc(r.NetworkPrefixLimits, func(a, b PrefixLimit) int { return b.Prefix.Bits() - a.Prefix.Bits() })
		r.networkPrefixBuckets = make([]*rate.Limiter, 0, len(r.NetworkPrefixLimits))
		for _, limit := range r.NetworkPrefixLimits {
			if limit.RPS == 0 {
				r.networkPrefixBuckets = append(r.networkPrefixBuckets, rate.NewLimiter(rate.Inf, 0))
			} else {
				r.networkPrefixBuckets = append(r.networkPrefixBuckets, rate.NewLimiter(rate.Limit(limit.RPS), limit.Burst))
			}
		}
	})
}

// Limit rate limits a StreamHandler function.
func (r *Limiter) Limit(f func(s network.Stream)) func(s network.Stream) {
	r.init()
	return func(s network.Stream) {
		addr := s.Conn().RemoteMultiaddr()
		ip, err := manet.ToIP(addr)
		if err != nil {
			ip = nil
		}
		ipAddr, ok := netip.AddrFromSlice(ip)
		if !ok {
			ipAddr = netip.Addr{}
		}
		if !r.Allow(ipAddr) {
			_ = s.ResetWithError(network.StreamRateLimited)
			return
		}
		f(s)
	}
}

// Allow returns true if requests for `ipAddr` are within specified rate limits
func (r *Limiter) Allow(ipAddr netip.Addr) bool {
	r.init()
	// Check buckets from the most specific to the least.
	//
	// This ensures that a single peer cannot take up all the tokens in the global
	// rate limiting bucket. We *MUST* follow this order because the rate limiter
	// implementation doesn't have a `ReturnToken` method. If we checked the global
	// bucket before the specific bucket, and the specific bucket rejected the
	// request, there's no way to return the token to the global bucket. So all
	// rejected requests from the specific bucket would take up tokens from the global bucket.

	// prefixs have been sorted from most to least specific so rejected requests for more
	// specific prefixes don't take up tokens from the less specific prefixes.
	isWithinNetworkPrefix := false
	for i, limit := range r.NetworkPrefixLimits {
		if limit.Prefix.Contains(ipAddr) {
			if !r.networkPrefixBuckets[i].Allow() {
				return false
			}
			isWithinNetworkPrefix = true
		}
	}
	if isWithinNetworkPrefix {
		return true
	}

	if !r.SubnetRateLimiter.Allow(ipAddr, time.Now()) {
		return false
	}
	return r.globalBucket.Allow()
}

// SubnetLimiter rate limits requests per ip subnet.
type SubnetLimiter struct {
	// IPv4SubnetLimits are the per subnet limits for streams with IPv4 Peers.
	IPv4SubnetLimits []SubnetLimit
	// IPv6SubnetLimits are the per subnet limits for streams with IPv6 Peers.
	IPv6SubnetLimits []SubnetLimit
	// GracePeriod is the time to wait to remove a full capacity bucket.
	// Keeping a bucket around helps prevent allocations
	GracePeriod time.Duration

	initOnce  sync.Once
	mx        sync.Mutex
	ipv4Heaps []*bucketHeap
	ipv6Heaps []*bucketHeap
}

func (s *SubnetLimiter) init() {
	s.initOnce.Do(func() {
		// smaller prefix length, i.e. largest subnet, last
		slices.SortFunc(s.IPv4SubnetLimits, func(a, b SubnetLimit) int { return b.PrefixLength - a.PrefixLength })
		slices.SortFunc(s.IPv6SubnetLimits, func(a, b SubnetLimit) int { return b.PrefixLength - a.PrefixLength })

		s.ipv4Heaps = make([]*bucketHeap, len(s.IPv4SubnetLimits))
		for i := range s.IPv4SubnetLimits {
			s.ipv4Heaps[i] = &bucketHeap{
				prefixBucket:  make([]prefixBucketWithExpiry, 0),
				prefixToIndex: make(map[netip.Prefix]int),
			}
			heap.Init(s.ipv4Heaps[i])
		}

		s.ipv6Heaps = make([]*bucketHeap, len(s.IPv6SubnetLimits))
		for i := range s.IPv6SubnetLimits {
			s.ipv6Heaps[i] = &bucketHeap{
				prefixBucket:  make([]prefixBucketWithExpiry, 0),
				prefixToIndex: make(map[netip.Prefix]int),
			}
			heap.Init(s.ipv6Heaps[i])
		}
	})
}

// Allow returns true if requests for `ipAddr` are within specified rate limits
func (s *SubnetLimiter) Allow(ipAddr netip.Addr, now time.Time) bool {
	s.init()
	s.mx.Lock()
	defer s.mx.Unlock()

	s.cleanUp(now)

	var subNetLimits []SubnetLimit
	var heaps []*bucketHeap
	if ipAddr.Is4() {
		subNetLimits = s.IPv4SubnetLimits
		heaps = s.ipv4Heaps
	} else {
		subNetLimits = s.IPv6SubnetLimits
		heaps = s.ipv6Heaps
	}

	for i, limit := range subNetLimits {
		prefix, err := ipAddr.Prefix(limit.PrefixLength)
		if err != nil {
			return false // we have a ipaddr this shouldn't happen
		}

		bucket := heaps[i].Get(prefix)
		if bucket == (prefixBucketWithExpiry{}) {
			bucket = prefixBucketWithExpiry{
				Prefix:      prefix,
				tokenBucket: tokenBucket{rate.NewLimiter(rate.Limit(limit.RPS), limit.Burst)},
				Expiry:      now,
			}
		}

		if !bucket.Allow() {
			// bucket is empty, its expiry would have been set correctly the last time
			// it allowed a request.
			return false
		}
		bucket.Expiry = bucket.FullAt(now).Add(s.GracePeriod)
		heaps[i].Upsert(bucket)
	}
	return true
}

// cleanUp removes limiters that have expired by now.
func (s *SubnetLimiter) cleanUp(now time.Time) {
	for _, h := range s.ipv4Heaps {
		h.Expire(now)
	}
	for _, h := range s.ipv6Heaps {
		h.Expire(now)
	}
}

// tokenBucket is a *rate.Limiter with a `FullAt` method.
type tokenBucket struct {
	*rate.Limiter
}

// FullAt returns the instant at which the bucket will be full.
func (b *tokenBucket) FullAt(now time.Time) time.Time {
	tokensNeeded := float64(b.Burst()) - b.TokensAt(now)
	refillRate := float64(b.Limit())
	eta := time.Duration((tokensNeeded / refillRate) * float64(time.Second))
	return now.Add(eta)
}

// prefixBucketWithExpiry is a token bucket with a prefix and Expiry. The expiry is when the bucket
// will be full with tokens.
type prefixBucketWithExpiry struct {
	tokenBucket
	Prefix netip.Prefix
	Expiry time.Time
}

// bucketHeap is a heap of buckets ordered by their Expiry. At expiry, the bucket
// is removed from the heap as a full bucket is indistinguishable from a new bucket.
type bucketHeap struct {
	prefixBucket  []prefixBucketWithExpiry
	prefixToIndex map[netip.Prefix]int
}

var _ heap.Interface = (*bucketHeap)(nil)

// Upsert replaces the bucket with prefix `b.Prefix` with the provided bucket, `b`, or
// inserts `b` if no bucket with prefix `b.Prefix` exists.
func (h *bucketHeap) Upsert(b prefixBucketWithExpiry) {
	if i, ok := h.prefixToIndex[b.Prefix]; ok {
		h.prefixBucket[i] = b
		heap.Fix(h, i)
		return
	}
	heap.Push(h, b)
}

// Get returns the limiter for a prefix
func (h *bucketHeap) Get(prefix netip.Prefix) prefixBucketWithExpiry {
	if i, ok := h.prefixToIndex[prefix]; ok {
		return h.prefixBucket[i]
	}
	return prefixBucketWithExpiry{}
}

// Expire removes elements with expiry before `expiry`
func (h *bucketHeap) Expire(expiry time.Time) {
	for h.Len() > 0 {
		oldest := h.prefixBucket[0]
		if oldest.Expiry.After(expiry) {
			break
		}
		heap.Pop(h)
	}
}

// Methods for the heap interface

// Len returns the length of the heap
func (h *bucketHeap) Len() int {
	return len(h.prefixBucket)
}

// Less compares two elements in the heap
func (h *bucketHeap) Less(i, j int) bool {
	return h.prefixBucket[i].Expiry.Before(h.prefixBucket[j].Expiry)
}

// Swap swaps two elements in the heap
func (h *bucketHeap) Swap(i, j int) {
	h.prefixBucket[i], h.prefixBucket[j] = h.prefixBucket[j], h.prefixBucket[i]
	h.prefixToIndex[h.prefixBucket[i].Prefix] = i
	h.prefixToIndex[h.prefixBucket[j].Prefix] = j
}

// Push adds a new element to the heap
func (h *bucketHeap) Push(x any) {
	item := x.(prefixBucketWithExpiry)
	h.prefixBucket = append(h.prefixBucket, item)
	h.prefixToIndex[item.Prefix] = len(h.prefixBucket) - 1
}

// Pop removes and returns the top element from the heap
func (h *bucketHeap) Pop() any {
	n := len(h.prefixBucket)
	item := h.prefixBucket[n-1]
	h.prefixBucket = h.prefixBucket[0 : n-1]
	delete(h.prefixToIndex, item.Prefix)
	return item
}
