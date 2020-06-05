package ws

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/stretchr/testify/require"
)

func TestStartSession(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()
}

func TestDoubleHelloFails(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_HELLO})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_ERR, resp.Result)
}

func TestNoHelloFails(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.sendCommand(&pb.ClientCommand{Id: 200, Command: pb.ClientCommand_REQUEST, Source: pb.ClientCommand_RUNTIME})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 200, resp.Id)
	require.EqualValues(t, pb.CommandResponse_ERR, resp.Result)
}
