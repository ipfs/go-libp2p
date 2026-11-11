package autonatv2

import (
	"time"

	"github.com/libp2p/go-libp2p/core/network"
)

// autoNATSettings is used to configure AutoNAT
type autoNATSettings struct {
	allowPrivateAddrs                    bool
	serverRPM                            int
	serverPerPeerRPM                     int
	serverDialDataRPM                    int
	maxConcurrentRequestsPerPeer         int
	dataRequestPolicy                    dataRequestPolicyFunc
	now                                  func() time.Time
	amplificatonAttackPreventionDialWait time.Duration
	metricsTracer                        MetricsTracer
	throttlePeerDuration                 time.Duration
	forceReachability                    *network.Reachability
}

func defaultSettings() *autoNATSettings {
	return &autoNATSettings{
		allowPrivateAddrs:                    false,
		serverRPM:                            60, // 1 every second
		serverPerPeerRPM:                     12, // 1 every 5 seconds
		serverDialDataRPM:                    12, // 1 every 5 seconds
		maxConcurrentRequestsPerPeer:         2,
		dataRequestPolicy:                    amplificationAttackPrevention,
		amplificatonAttackPreventionDialWait: 3 * time.Second,
		now:                                  time.Now,
		throttlePeerDuration:                 defaultThrottlePeerDuration,
	}
}

type AutoNATOption func(s *autoNATSettings) error

func WithServerRateLimit(rpm, perPeerRPM, dialDataRPM int, maxConcurrentRequestsPerPeer int) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.serverRPM = rpm
		s.serverPerPeerRPM = perPeerRPM
		s.serverDialDataRPM = dialDataRPM
		s.maxConcurrentRequestsPerPeer = maxConcurrentRequestsPerPeer
		return nil
	}
}

func WithMetricsTracer(m MetricsTracer) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.metricsTracer = m
		return nil
	}
}

// WithReachability overrides automatic reachability detection with a fixed status.
// The AutoNAT service continues to handle requests from other peers.
func WithReachability(reachability network.Reachability) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.forceReachability = &reachability
		return nil
	}
}

func withDataRequestPolicy(drp dataRequestPolicyFunc) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.dataRequestPolicy = drp
		return nil
	}
}

func AllowPrivateAddrs(s *autoNATSettings) error {
	s.allowPrivateAddrs = true
	return nil
}

func withAmplificationAttackPreventionDialWait(d time.Duration) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.amplificatonAttackPreventionDialWait = d
		return nil
	}
}

func withThrottlePeerDuration(d time.Duration) AutoNATOption {
	return func(s *autoNATSettings) error {
		s.throttlePeerDuration = d
		return nil
	}
}
