package introspect

import (
	"github.com/stretchr/testify/mock"

	"github.com/libp2p/go-libp2p-core/introspection"
	"github.com/libp2p/go-libp2p-core/introspection/pb"
)

type MockIntrospector struct {
	*eventManager
	mock.Mock

	EventCh chan interface{}
}

var _ introspection.Introspector = (*MockIntrospector)(nil)

func NewMockIntrospector() *MockIntrospector {
	mi := &MockIntrospector{
		EventCh: make(chan interface{}, 128),
	}
	mi.eventManager = newEventManager(mi.EventCh)
	return mi
}

func (m *MockIntrospector) Close() error {
	return m.eventManager.Close()
}

func (m *MockIntrospector) FetchRuntime() (*pb.Runtime, error) {
	args := m.MethodCalled("FetchRuntime")
	return args.Get(0).(*pb.Runtime), args.Error(1)
}

func (m *MockIntrospector) FetchFullState() (*pb.State, error) {
	args := m.MethodCalled("FetchFullState")
	return args.Get(0).(*pb.State), args.Error(1)
}
