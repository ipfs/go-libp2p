package introspect

import (
	"testing"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/introspection/pb"
	"github.com/libp2p/go-libp2p-core/peer"
)

type omnievent struct {
	String     string
	Strings    []string
	Int        int
	Ints       []int
	RawJSON    event.RawJSON
	RawJSONs   []event.RawJSON
	PeerID     peer.ID
	PeerIDs    []peer.ID
	Time       time.Time
	Times      []time.Time
	Multiaddr  multiaddr.Multiaddr
	Multiaddrs []multiaddr.Multiaddr
	Nested     struct {
		String     string
		Strings    []string
		Int        int
		Ints       []int
		RawJSON    event.RawJSON
		RawJSONs   []event.RawJSON
		PeerID     peer.ID
		PeerIDs    []peer.ID
		Time       time.Time
		Times      []time.Time
		Multiaddr  multiaddr.Multiaddr
		Multiaddrs []multiaddr.Multiaddr
	}
}

type EventA omnievent
type EventB omnievent

func TestEventManager(t *testing.T) {
	inCh := make(chan interface{}, 10)
	em := newEventManager(inCh)
	defer em.Close()

	require.Empty(t, em.EventMetadata())
	inCh <- EventA{}

	evt := <-em.EventChan()
	require.NotNil(t, evt)

	compare := []struct {
		Name     string
		Type     pb.EventType_EventProperty_PropertyType
		Multiple bool
	}{
		{"String", pb.EventType_EventProperty_STRING, false},
		{"Strings", pb.EventType_EventProperty_STRING, true},
		{"Int", pb.EventType_EventProperty_NUMBER, false},
		{"Ints", pb.EventType_EventProperty_NUMBER, true},
		{"RawJSON", pb.EventType_EventProperty_JSON, false},
		{"RawJSONs", pb.EventType_EventProperty_JSON, true},
		{"PeerID", pb.EventType_EventProperty_PEERID, false},
		{"PeerIDs", pb.EventType_EventProperty_PEERID, true},
		{"Time", pb.EventType_EventProperty_TIME, false},
		{"Times", pb.EventType_EventProperty_TIME, true},
		{"Multiaddr", pb.EventType_EventProperty_MULTIADDR, false},
		{"Multiaddrs", pb.EventType_EventProperty_MULTIADDR, true},
		{"Nested", pb.EventType_EventProperty_JSON, false},
	}

	require.Equal(t, "EventA", evt.Type.Name)

	for i, pt := range evt.Type.PropertyTypes {
		require.Equal(t, compare[i].Name, pt.Name)
		require.Equal(t, compare[i].Type, pt.Type)
		require.Equal(t, compare[i].Multiple, pt.HasMultiple)
	}

	require.Len(t, em.EventMetadata(), 1)
	require.Equal(t, evt.Type, em.EventMetadata()[0])

	// send another event of type EventA; it should not inline the type definition.
	inCh <- EventA{}

	evt = <-em.EventChan()
	require.NotNil(t, evt)
	require.Equal(t, "EventA", evt.Type.Name)
	require.Nil(t, evt.Type.PropertyTypes)

	// send a new event; the type definition must be inlined.
	inCh <- EventB{}

	evt = <-em.EventChan()
	require.NotNil(t, evt)
	require.Equal(t, "EventB", evt.Type.Name)

	for i, pt := range evt.Type.PropertyTypes {
		require.Equal(t, compare[i].Name, pt.Name)
		require.Equal(t, compare[i].Type, pt.Type)
		require.Equal(t, compare[i].Multiple, pt.HasMultiple)
	}

	require.Len(t, em.EventMetadata(), 2)
}

func TestSubscriptionClosedClosesOut(t *testing.T) {
	inCh := make(chan interface{}, 10)
	em := newEventManager(inCh)
	defer em.Close()

	require.Empty(t, em.EventMetadata())
	inCh <- EventA{}

	evt := <-em.EventChan()
	require.NotNil(t, evt)
	close(inCh)

	evt, more := <-em.EventChan()
	require.Nil(t, evt)
	require.False(t, more)
}

func TestCloseStopsProcessing(t *testing.T) {
	inCh := make(chan interface{}, 10)
	em := newEventManager(inCh)

	require.Empty(t, em.EventMetadata())
	inCh <- EventA{}
	evt := <-em.EventChan()
	require.NotNil(t, evt)

	err := em.Close()
	require.NoError(t, err)

	inCh <- EventA{}
	inCh <- EventA{}
	require.Len(t, inCh, 2)

	evt, more := <-em.EventChan()
	require.Nil(t, evt)
	require.False(t, more)
}
