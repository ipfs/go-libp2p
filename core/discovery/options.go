package discovery

import (
	"errors"
	"time"
)

// ErrNegativeTTL is returned by [TTL] when a negative lifetime is provided.
var ErrNegativeTTL = errors.New("discovery: negative ttl")

// DiscoveryOpt is a single discovery option.
type Option func(opts *Options) error

// DiscoveryOpts is a set of discovery options.
type Options struct {
	Ttl   time.Duration
	Limit int

	// Other (implementation-specific) options
	Other map[any]any
}

// Apply applies the given options to this DiscoveryOpts
func (opts *Options) Apply(options ...Option) error {
	for _, o := range options {
		if err := o(opts); err != nil {
			return err
		}
	}
	return nil
}

// TTL is an option that provides a hint for the duration of an advertisement.
//
// TTL(0) leaves the choice of lifetime to the implementation. Negative TTLs are
// meaningless -- advertisements are valid for at least as long as the round-trip
// to publish them -- and would make consumers such as util.Advertise republish
// in a tight loop, so they are rejected with [ErrNegativeTTL].
func TTL(ttl time.Duration) Option {
	return func(opts *Options) error {
		if ttl < 0 {
			return ErrNegativeTTL
		}
		opts.Ttl = ttl
		return nil
	}
}

// Limit is an option that provides an upper bound on the peer count for discovery
func Limit(limit int) Option {
	return func(opts *Options) error {
		opts.Limit = limit
		return nil
	}
}
