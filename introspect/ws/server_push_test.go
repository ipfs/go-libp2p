package ws

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/stretchr/testify/require"
)

type simpleEvent struct {
	String string
	Number int
}

type EventA simpleEvent
type EventB simpleEvent
type EventC simpleEvent

func TestPushEvents(t *testing.T) {
	server, mocki, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)

	assertEvent := func(name string) {
		msg := conn.readNext()
		evt := msg.Payload.(*pb.ServerMessage_Event).Event

		require.EqualValues(t, name, evt.Type.Name)
		require.NotEmpty(t, evt.Content)
		require.NotZero(t, evt.Ts)
	}

	mocki.EventCh <- EventA{String: "hello", Number: 100}
	mocki.EventCh <- EventA{String: "hello", Number: 100}
	mocki.EventCh <- EventB{String: "hello", Number: 100}

	assertEvent("EventA")
	assertEvent("EventA")
	assertEvent("EventB")
}

func TestPushStopPushEvents(t *testing.T) {
	server, mocki, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)

	assertEvent := func(name string) {
		msg := conn.readNext()
		evt := msg.Payload.(*pb.ServerMessage_Event).Event

		require.EqualValues(t, name, evt.Type.Name)
		require.NotEmpty(t, evt.Content)
		require.NotZero(t, evt.Ts)
	}

	mocki.EventCh <- EventA{String: "hello", Number: 100}
	assertEvent("EventA")

	// now disable the pusher and verify that we actually missed those events.
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_DISABLE, Source: pb.ClientCommand_EVENTS})
	msg = conn.readNext()
	resp = msg.Payload.(*pb.ServerMessage_Response).Response
	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)

	time.Sleep(1 * time.Second)

	// these events will be missed.
	mocki.EventCh <- EventA{String: "hello", Number: 100}
	mocki.EventCh <- EventB{String: "hello", Number: 100}
	time.Sleep(500 * time.Millisecond)

	// enable the pusher again
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})
	msg = conn.readNext()
	resp = msg.Payload.(*pb.ServerMessage_Response).Response
	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)

	// these events will be received.
	mocki.EventCh <- EventC{String: "hello", Number: 100}
	mocki.EventCh <- EventC{String: "hello", Number: 100}

	assertEvent("EventC")
	assertEvent("EventC")
}

func TestPushEventsIdempotent(t *testing.T) {
	server, _, _ := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()
	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})

	msg := conn.readNext()
	resp := msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 201, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)

	conn.sendCommand(&pb.ClientCommand{Id: 202, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})

	msg = conn.readNext()
	resp = msg.Payload.(*pb.ServerMessage_Response).Response

	require.EqualValues(t, 202, resp.Id)
	require.EqualValues(t, pb.CommandResponse_OK, resp.Result)
}

func TestPauseResume(t *testing.T) {
	server, mocki, clk := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	mocki.On("FetchFullState").Return(&pb.State{}, nil)

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})
	conn.sendCommand(&pb.ClientCommand{Id: 202, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_STATE})
	conn.sendCommand(&pb.ClientCommand{Id: 203, Command: pb.ClientCommand_PUSH_PAUSE})

	conn.consumeCommandResponse(201, pb.CommandResponse_OK)
	conn.consumeCommandResponse(202, pb.CommandResponse_OK)
	conn.consumeCommandResponse(203, pb.CommandResponse_OK)

	type tsmsg struct {
		ts  time.Time
		msg *pb.ServerMessage
	}

	var rcvd []tsmsg
	triggerDrain, rcvdAll := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(rcvdAll)
		<-triggerDrain
		for i := 0; i < 9; i++ {
			m := tsmsg{clk.Now(), conn.readNext()}
			rcvd = append(rcvd, m)
		}
	}()

	// tick 5 seconds; this will generate 5 state messages.
	clk.Add(5 * time.Second)

	// send one event, tick once (state), send one event, tick once (state)
	mocki.EventCh <- EventA{String: "hello", Number: 100}
	clk.Add(time.Second)
	mocki.EventCh <- EventB{String: "hello", Number: 100}
	clk.Add(time.Second)

	// now resume, we should receive 9 messages all after now.
	now := clk.Now()
	conn.sendCommand(&pb.ClientCommand{Id: 204, Command: pb.ClientCommand_PUSH_RESUME})
	conn.consumeCommandResponse(204, pb.CommandResponse_OK)
	close(triggerDrain)

	// we should be having 9 messages.
	<-rcvdAll

	// all messages were received _after_ we resumed.
	for _, m := range rcvd {
		require.True(t, m.ts.Equal(now) || m.ts.After(now))
	}
}

func TestPauseResumeMessagesDropped(t *testing.T) {
	server, mocki, clk := createTestServer(t)
	require.NoError(t, server.Start())
	defer server.Close()

	mocki.On("FetchFullState").Return(&pb.State{}, nil)

	conn := createConn(t, server)
	defer conn.Close()

	conn.greet()

	config := DefaultSessionConfig
	config.RetentionPeriodMs = 2000 // 2 seconds retention period.

	conn.sendCommand(&pb.ClientCommand{Id: 201, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_EVENTS})
	conn.sendCommand(&pb.ClientCommand{Id: 202, Command: pb.ClientCommand_PUSH_ENABLE, Source: pb.ClientCommand_STATE})
	conn.sendCommand(&pb.ClientCommand{Id: 203, Command: pb.ClientCommand_UPDATE_CONFIG, Config: &config})
	conn.sendCommand(&pb.ClientCommand{Id: 204, Command: pb.ClientCommand_PUSH_PAUSE})

	conn.consumeCommandResponse(201, pb.CommandResponse_OK)
	conn.consumeCommandResponse(202, pb.CommandResponse_OK)
	conn.consumeCommandResponse(203, pb.CommandResponse_OK)
	conn.consumeCommandResponse(204, pb.CommandResponse_OK)

	type tsmsg struct {
		ts  time.Time
		msg *pb.ServerMessage
	}

	var rcvd []tsmsg
	triggerDrain, rcvdAll := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(rcvdAll)
		<-triggerDrain
		for i := 0; i < 5; i++ {
			m := tsmsg{clk.Now(), conn.readNext()}
			rcvd = append(rcvd, m)
		}
	}()

	// the event and the first few state messages will be discarded because they slid out of the retention window.
	clk.Add(5 * time.Second)
	mocki.EventCh <- EventA{String: "hello", Number: 100}
	clk.Add(5 * time.Second)

	// send one event, tick once (state), send one event, tick once (state)
	mocki.EventCh <- EventB{String: "hello", Number: 100}
	mocki.EventCh <- EventB{String: "hello", Number: 100}

	// now resume, we should receive 9 messages all after now.
	now := clk.Now()
	conn.sendCommand(&pb.ClientCommand{Id: 205, Command: pb.ClientCommand_PUSH_RESUME})
	conn.consumeCommandResponse(205, pb.CommandResponse_OK)
	close(triggerDrain)

	// we should be having 5 messages.
	<-rcvdAll

	// all messages were received _after_ we resumed.
	for _, m := range rcvd {
		require.True(t, m.ts.Equal(now) || m.ts.After(now))
	}

	// three state messages + two events of type EventB.
	require.NotNil(t, rcvd[0].msg.Payload.(*pb.ServerMessage_State).State)
	require.NotNil(t, rcvd[1].msg.Payload.(*pb.ServerMessage_State).State)
	require.NotNil(t, rcvd[2].msg.Payload.(*pb.ServerMessage_State).State)
	require.NotNil(t, rcvd[3].msg.Payload.(*pb.ServerMessage_Event).Event)
	require.NotNil(t, rcvd[4].msg.Payload.(*pb.ServerMessage_Event).Event)
	require.EqualValues(t, "EventB", rcvd[3].msg.Payload.(*pb.ServerMessage_Event).Event.Type.Name)
	require.EqualValues(t, "EventB", rcvd[4].msg.Payload.(*pb.ServerMessage_Event).Event.Type.Name)
}
