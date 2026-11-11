package holepunch

import (
	"fmt"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func mustAddrs(t *testing.T, ss ...string) []ma.Multiaddr {
	t.Helper()
	out := make([]ma.Multiaddr, len(ss))
	for i, s := range ss {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			t.Fatalf("bad test multiaddr %q: %v", s, err)
		}
		out[i] = a
	}
	return out
}

// Test A: non-public addresses are dropped and never survive as dial candidates.
func TestFilterHolePunchAddrs_RejectsNonPublic(t *testing.T) {
	nonPublic := mustAddrs(t,
		"/ip4/127.0.0.1/tcp/8080",     // loopback
		"/ip4/0.0.0.0/tcp/8080",       // unspecified
		"/ip4/10.1.2.3/tcp/22",        // RFC1918
		"/ip4/172.16.9.9/tcp/6379",    // RFC1918
		"/ip4/192.168.1.1/tcp/443",    // RFC1918
		"/ip4/169.254.169.254/tcp/80", // link-local
		"/ip4/100.64.0.1/tcp/80",      // CGNAT (RFC6598)
		"/ip6/::1/tcp/8080",           // loopback
		"/ip6/::/tcp/8080",            // unspecified
		"/ip6/fe80::1/tcp/8080",       // link-local
		"/ip6/fc00::1/tcp/8080",       // unique-local (ULA)
	)
	got := filterHolePunchAddrs(nonPublic)
	if len(got) != 0 {
		t.Fatalf("expected all non-public addresses rejected, got %v", got)
	}
	// And each individually, so a failure names the offender.
	for _, a := range nonPublic {
		if len(filterHolePunchAddrs([]ma.Multiaddr{a})) != 0 {
			t.Errorf("non-public address survived filtering: %s", a)
		}
	}
}

// Test B: public addresses survive.
func TestFilterHolePunchAddrs_KeepsPublic(t *testing.T) {
	public := mustAddrs(t,
		"/ip4/8.8.8.8/tcp/443",
		"/ip6/2606:4700:4700::1111/tcp/443",
	)
	got := filterHolePunchAddrs(public)
	if len(got) != len(public) {
		t.Fatalf("expected %d public addresses kept, got %d: %v", len(public), len(got), got)
	}
	for _, a := range got {
		if !manet.IsPublicAddr(a) {
			t.Errorf("kept a non-public address: %s", a)
		}
	}
}

// Test C: /p2p-circuit (relay) addresses are removed by the receiver pipeline
// (removeRelayAddrs runs before the public-address gate on both paths).
func TestFilterHolePunchAddrs_RemovesRelay(t *testing.T) {
	in := mustAddrs(t,
		"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X/p2p-circuit",
		"/ip4/8.8.8.8/tcp/443",
	)
	got := filterHolePunchAddrs(removeRelayAddrs(in))
	if len(got) != 1 || !got[0].Equal(ma.StringCast("/ip4/8.8.8.8/tcp/443")) {
		t.Fatalf("expected only the public non-relay address, got %v", got)
	}
}

// Test D: the candidate list is capped at maxHolePunchAddrs.
func TestFilterHolePunchAddrs_Caps(t *testing.T) {
	in := make([]ma.Multiaddr, 0, maxHolePunchAddrs*4)
	for i := 0; i < maxHolePunchAddrs*4; i++ {
		// distinct public addresses
		in = append(in, ma.StringCast(fmt.Sprintf("/ip4/8.8.%d.%d/tcp/443", i/256, i%256)))
	}
	got := filterHolePunchAddrs(in)
	if len(got) != maxHolePunchAddrs {
		t.Fatalf("expected result capped at %d, got %d", maxHolePunchAddrs, len(got))
	}
	// The kept addresses are the first maxHolePunchAddrs public candidates.
	for i, a := range got {
		if !a.Equal(in[i]) {
			t.Fatalf("cap changed ordering at %d: got %s want %s", i, a, in[i])
		}
	}
}

// Test E: the gate is final. A custom AddrFilter runs first (so it can still
// reject public addresses), and any non-public address it adds is still removed.
func TestFilterHolePunchAddrs_FinalGateAfterAddrFilter(t *testing.T) {
	remote := mustAddrs(t, "/ip4/8.8.8.8/tcp/443", "/ip4/1.1.1.1/tcp/443")

	// (1) A subsetting filter that drops a public address: it stays dropped.
	subset := func(_ []ma.Multiaddr) []ma.Multiaddr {
		return mustAddrs(t, "/ip4/8.8.8.8/tcp/443") // reject 1.1.1.1
	}
	got := filterHolePunchAddrs(subset(remote))
	if len(got) != 1 || !got[0].Equal(ma.StringCast("/ip4/8.8.8.8/tcp/443")) {
		t.Fatalf("custom filter's subsetting not preserved: %v", got)
	}

	// (2) A filter that (mis)uses the "can add addresses" latitude to inject a
	// private address: the final gate removes it regardless.
	injecting := func(in []ma.Multiaddr) []ma.Multiaddr {
		return append(append([]ma.Multiaddr{}, in...), ma.StringCast("/ip4/127.0.0.1/tcp/1"))
	}
	got = filterHolePunchAddrs(injecting(remote))
	for _, a := range got {
		if !manet.IsPublicAddr(a) {
			t.Fatalf("final gate let a non-public address through after AddrFilter: %s", a)
		}
	}
	if len(got) != 2 {
		t.Fatalf("expected the two public addresses, got %v", got)
	}
}

// Test G: DNS-form addresses are rejected (they resolve to a peer-controlled IP,
// which IsPublicAddr can't see), while IP-based public addresses are kept.
func TestFilterHolePunchAddrs_RejectsDNS(t *testing.T) {
	in := mustAddrs(t,
		"/dns4/example.com/tcp/443",
		"/dns6/example.com/tcp/443",
		"/dns/example.com/tcp/443",
		"/dnsaddr/example.com/tcp/443",
		"/ip4/8.8.8.8/tcp/443",
	)
	got := filterHolePunchAddrs(in)
	if len(got) != 1 || !got[0].Equal(ma.StringCast("/ip4/8.8.8.8/tcp/443")) {
		t.Fatalf("expected only the IP-based public address to survive, got %v", got)
	}
	for _, a := range in[:4] {
		if len(filterHolePunchAddrs([]ma.Multiaddr{a})) != 0 {
			t.Errorf("DNS address survived filtering: %s", a)
		}
	}
}

// Test H: filtering happens before capping, so public addresses following a full
// batch of non-public ones still survive (cap-then-filter would drop them).
func TestFilterHolePunchAddrs_FilterBeforeCap(t *testing.T) {
	in := make([]ma.Multiaddr, 0, maxHolePunchAddrs+2)
	for i := 0; i < maxHolePunchAddrs; i++ {
		in = append(in, ma.StringCast(fmt.Sprintf("/ip4/10.0.%d.%d/tcp/80", i/256, i%256))) // private
	}
	pub := mustAddrs(t, "/ip4/8.8.8.8/tcp/443", "/ip4/1.1.1.1/tcp/443")
	in = append(in, pub...)

	got := filterHolePunchAddrs(in)
	if len(got) != len(pub) {
		t.Fatalf("filter-before-cap broken: expected %d public survivors, got %d: %v", len(pub), len(got), got)
	}
	for i, a := range got {
		if !a.Equal(pub[i]) {
			t.Errorf("survivor %d = %s, want %s", i, a, pub[i])
		}
	}
}
