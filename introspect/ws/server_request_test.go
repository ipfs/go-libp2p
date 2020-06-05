package ws

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/stretchr/testify/require"
)

func TestRequestState(t *testing.T) {
	server, mocki, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	mocki.On("FetchFullState").Return(&pb.State{}, nil)
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_STATE})

	msg := conn.readNext()
	require.NotNil(t, msg.Payload.(*pb.ServerMessage_State).State)
	mocki.AssertNumberOfCalls(t, "FetchFullState", 1)
}

func TestRequestRuntime(t *testing.T) {
	server, mocki, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	mocki.On("FetchRuntime").Return(&pb.Runtime{}, nil)
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_RUNTIME})

	msg := conn.readNext()
	require.NotNil(t, msg.Payload.(*pb.ServerMessage_Runtime).Runtime)
	mocki.AssertNumberOfCalls(t, "FetchRuntime", 1)
}

func TestRequestEventsFails(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_EVENTS})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_ERR, resp.Result)
}
