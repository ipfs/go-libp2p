package quicreuse

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// netroute.New() fails wherever the process cannot read the kernel route table
// — routinely on Android, where an unprivileged app has no netlink access
// ("route ip+net: netlinkrib: permission denied").
//
// defaultSourceIPSelectorFn used to hand that failure back as
// (&netrouteSourceIPSelector{routes: nil}, err): a non-nil selector wrapping a
// nil router. Both call sites discard the error, then guard with
// `if router != nil` — which passes, because what is nil is the field rather
// than the wrapper. The next dial dereferenced it:
//
//	panic: runtime error: invalid memory address or nil pointer dereference
//	[signal SIGSEGV code=0x1 addr=0x18]
//	quicreuse.(*netrouteSourceIPSelector).PreferredSourceIPForDestination
//	quicreuse.(*reuse).transportWithAssociationForDial
//	quicreuse.(*ConnManager).DialQUIC
//	quic.(*transport).Dial
//	swarm.(*Swarm).dialAddr
//
// See #3537.

func TestSourceIPSelectorIsNilWhenTheRouteTableIsUnavailable(t *testing.T) {
	sel, err := newSourceIPSelector(nil, errors.New("netlinkrib: permission denied"))
	require.Error(t, err)
	// A nil interface, not a non-nil interface holding a nil pointer: the
	// `router != nil` guard in transportWithAssociationForDial has to see it.
	require.True(t, sel == nil)
}

// go-netroute can also return (nil, nil).
func TestSourceIPSelectorIsNilWhenTheRouterIsNilWithoutAnError(t *testing.T) {
	sel, err := newSourceIPSelector(nil, nil)
	require.NoError(t, err)
	require.True(t, sel == nil)
}

func TestPreferredSourceIPWithNoRouterReturnsAnError(t *testing.T) {
	s := &netrouteSourceIPSelector{routes: nil}
	ip, err := s.PreferredSourceIPForDestination(&net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443})
	require.Error(t, err)
	require.Nil(t, ip)
}

// The dial path itself: a reuse whose selector could not be built dials without
// source-IP affinity rather than crashing.
func TestDialWithoutASourceIPSelector(t *testing.T) {
	reuse := newReuse(nil, nil, defaultListenUDP, func() (SourceIPSelector, error) {
		return newSourceIPSelector(nil, errors.New("netlinkrib: permission denied"))
	}, nil, nil)
	defer reuse.Close()

	tr, err := reuse.TransportWithAssociationForDial(nil, "udp4", &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443})
	require.NoError(t, err)
	require.NotNil(t, tr)
	tr.DecreaseCount()
}
