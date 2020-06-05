package ws

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/stretchr/testify/require"
)

func TestValidConfiguration(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	config := &pb.Configuration{
		RetentionPeriodMs:       uint64(MaxRetentionPeriod.Milliseconds() - 1),
		StateSnapshotIntervalMs: uint64(MinStateSnapshotInterval.Milliseconds() + 1),
	}

	// on HELLO
	conn.sendCommand(&pb.ClientCommand{Id: 200, Command: pb.ClientCommand_HELLO, Config: config})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 200, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)
	require.EqualValues(t, config, resp.EffectiveConfig)
	require.Empty(t, resp.Error)

	// on UPDATE_VALUES, adjust the values to verify new values have been set.
	config.RetentionPeriodMs -= 1
	config.StateSnapshotIntervalMs += 1
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_UPDATE_CONFIG, Config: config})

	msg = conn.readNext()
	resp = msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)
	require.EqualValues(t, config, resp.EffectiveConfig)
	require.Empty(t, resp.Error)
}

func TestHelloWithInvalidConfig(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	expected := &pb.Configuration{
		RetentionPeriodMs:       uint64(MaxRetentionPeriod.Milliseconds()),
		StateSnapshotIntervalMs: uint64(MinStateSnapshotInterval.Milliseconds()),
	}

	config := &pb.Configuration{
		RetentionPeriodMs:       uint64(MaxRetentionPeriod.Milliseconds() + 1),
		StateSnapshotIntervalMs: uint64(MinStateSnapshotInterval.Milliseconds() - 1),
	}

	// on HELLO
	conn.sendCommand(&pb.ClientCommand{Id: 200, Command: pb.ClientCommand_HELLO, Config: config})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 200, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)
	require.EqualValues(t, expected, resp.EffectiveConfig)
	require.Empty(t, resp.Error)

	// on UPDATE_VALUES, adjust the values to verify new values have been set.
	config.RetentionPeriodMs += 1
	config.StateSnapshotIntervalMs -= 1
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_UPDATE_CONFIG, Config: config})

	msg = conn.readNext()
	resp = msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)
	require.EqualValues(t, expected, resp.EffectiveConfig)
	require.Empty(t, resp.Error)
}
