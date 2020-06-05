package ws

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/benbjohnson/clock"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var handlers = map[pb.ClientCommand_Command]func(*session, *pb.ClientCommand) *pb.ServerMessage{
	pb.ClientCommand_HELLO:         (*session).handleHelloCmd,
	pb.ClientCommand_REQUEST:       (*session).handleRequestCmd,
	pb.ClientCommand_PUSH_ENABLE:   (*session).handlePushEnableCmd,
	pb.ClientCommand_PUSH_DISABLE:  (*session).handlePushDisableCmd,
	pb.ClientCommand_PUSH_PAUSE:    (*session).handlePushPauseCmd,
	pb.ClientCommand_PUSH_RESUME:   (*session).handlePushResumeCmd,
	pb.ClientCommand_UPDATE_CONFIG: (*session).handleUpdateConfigCmd,
}

var (
	MaxRetentionPeriod       = 120 * time.Second
	MinStateSnapshotInterval = 500 * time.Millisecond
	PruneRetentionInterval   = 2 * time.Second

	WriteTimeout   = 5 * time.Second
	ConnBufferSize = 1 << 8

	DefaultSessionConfig = pb.Configuration{
		RetentionPeriodMs:       uint64(MaxRetentionPeriod / time.Millisecond),
		StateSnapshotIntervalMs: uint64(time.Second / time.Millisecond),
	}
)

type qitem struct {
	ts      time.Time
	payload []byte
}

type session struct {
	server  *Endpoint
	wsconn  *websocket.Conn
	logger  *zap.SugaredLogger
	writeCh chan []byte

	pushingEvents bool
	pushingState  bool
	paused        bool

	eventCh   chan []byte
	commandCh chan *pb.ClientCommand

	stateTicker *clock.Ticker

	greeted bool
	config  *pb.Configuration
	q       []*qitem

	wg      sync.WaitGroup
	closed  int32
	closeCh chan struct{}
}

func newSession(sv *Endpoint, wsconn *websocket.Conn) *session {
	cfgcpy := DefaultSessionConfig
	ch := &session{
		server:      sv,
		wsconn:      wsconn,
		config:      &cfgcpy,
		stateTicker: new(clock.Ticker),

		writeCh:   make(chan []byte, ConnBufferSize),
		eventCh:   make(chan []byte, ConnBufferSize),
		commandCh: make(chan *pb.ClientCommand),
		closeCh:   make(chan struct{}),
	}

	ch.logger = logger.Named(wsconn.RemoteAddr().String())
	return ch
}

func (s *session) run() {
	s.wg.Add(3)

	go s.writeLoop()
	go s.readLoop()
	go s.control()

	s.wg.Wait()
}

func (s *session) trySendEvent(msg []byte) {
	select {
	case s.eventCh <- msg:
	case <-s.closeCh:
	default:
		s.logger.Warnf("unable to queue event; dropping")
	}
}

func (s *session) queueWrite(msg []byte) {
	select {
	case s.writeCh <- msg:
	case <-s.closeCh:
		s.logger.Warnf("dropping queued message upon close")
	}
}

func (s *session) control() {
	defer s.wg.Done()

	// dummy ticker that won't tick unless enabled.
	pruneQTicker := s.server.clock.Ticker(PruneRetentionInterval)
	defer pruneQTicker.Stop()
	defer func() {
		if s.pushingState {
			s.stateTicker.Stop()
		}
	}()

	for {
		select {
		case <-s.stateTicker.C:
			msg, err := s.server.createStateMsg()
			if err != nil {
				s.logger.Warnf("failed to generate state message on tick; err: %s", err)
				continue
			}
			if s.paused {
				s.q = append(s.q, &qitem{s.server.clock.Now(), msg})
				continue
			}
			s.queueWrite(msg)

		case now := <-pruneQTicker.C:
			if !s.paused || len(s.q) == 0 {
				continue
			}

			i := 0
			thres := now.Add(-time.Duration(s.config.RetentionPeriodMs) * time.Millisecond)
			for ; i < len(s.q) && s.q[i].ts.Before(thres); i++ {
			}
			s.q = s.q[i:]

		case evt := <-s.eventCh:
			if !s.pushingEvents {
				continue
			}
			if s.paused {
				s.q = append(s.q, &qitem{s.server.clock.Now(), evt})
				continue
			}
			s.queueWrite(evt)

		case cmd := <-s.commandCh:
			var resp *pb.ServerMessage
			handler, ok := handlers[cmd.Command]
			if !ok {
				err := fmt.Errorf("unknown command type: %v", cmd.Command)
				resp = createCmdErrorResp(cmd, err)
				s.logger.Warnf("%s", err)
			} else {
				resp = handler(s, cmd)
			}

			if resp != nil {
				msg, err := envelopePacket(resp)
				if err != nil {
					s.logger.Warnf("failed to marshal client message; err: %s", err)
					s.kill()
					return
				}
				s.queueWrite(msg)
			}

		case <-s.closeCh:
			s.q = nil
			return
		}
	}
}

func (s *session) writeLoop() {
	defer s.wg.Done()

	for {
		select {
		case msg := <-s.writeCh:
			_ = s.wsconn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if err := s.wsconn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				s.logger.Warnf("failed to send binary message to client with addr %s, err=%s", err)
				s.kill()
				return
			}

		case <-s.closeCh:
			return
		}
	}
}

func (s *session) kill() {
	if atomic.SwapInt32(&s.closed, 1) == 0 {
		close(s.closeCh)
		_ = s.wsconn.Close()
	}
}

func (s *session) readLoop() {
	defer s.wg.Done()

	for {
		mt, message, err := s.wsconn.ReadMessage()
		switch err.(type) {
		case nil:
		case *websocket.CloseError:
			s.logger.Warnf("connection closed; err: %s", err)
			s.kill()
			return
		default:
			s.logger.Warnf("failed to read message from ws connection; err: %s", err)
			s.kill()
			return
		}

		s.logger.Debugf("received message from ws connection; type: %d; recv: %x", mt, message)

		cmd := new(pb.ClientCommand)
		if err := cmd.Unmarshal(message); err != nil {
			s.logger.Warnf("failed to read client message; err: %s", err)
			s.kill()
			return
		}

		select {
		case s.commandCh <- cmd:
		case <-s.closeCh:
			return
		}
	}
}

func (s *session) handleHelloCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client had already greeted server"))
	}
	s.greeted = true
	if cmd.Config != nil {
		s.config = s.validateConfig(*cmd.Config)
	}
	resp := createCmdOKResp(cmd)
	resp.Payload.(*pb.ServerMessage_Response).Response.EffectiveConfig = s.config
	return resp
}

func (s *session) handleRequestCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	var (
		bytes []byte
		err   error
	)
	switch cmd.Source {
	case pb.ClientCommand_EVENTS:
		err = fmt.Errorf("illegal request for events messages")
	case pb.ClientCommand_RUNTIME:
		bytes, err = s.server.createRuntimeMsg()
	case pb.ClientCommand_STATE:
		bytes, err = s.server.createStateMsg()
	}
	if err != nil {
		return createCmdErrorResp(cmd, err)
	}
	s.writeCh <- bytes
	return nil // response is the actual requested payload
}

func (s *session) handlePushEnableCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	switch cmd.Source {
	case pb.ClientCommand_STATE:
		if s.pushingState {
			break // do nothing
		}
		s.pushingState = true
		s.stateTicker = s.server.clock.Ticker(time.Duration(s.config.StateSnapshotIntervalMs) * time.Millisecond)
	case pb.ClientCommand_EVENTS:
		s.pushingEvents = true
	default:
		return createCmdErrorResp(cmd, fmt.Errorf("specified source does not support pushing"))
	}
	return createCmdOKResp(cmd)
}

func (s *session) handlePushDisableCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	switch cmd.Source {
	case pb.ClientCommand_STATE:
		if !s.pushingState {
			break // do nothing
		}
		s.pushingState = false
		s.stateTicker.Stop()
	case pb.ClientCommand_EVENTS:
		s.pushingEvents = false
	default:
		return createCmdErrorResp(cmd, fmt.Errorf("specified source does not support pushing"))
	}

	// if all pushers are disabled, clear the queue.
	if !s.pushingState && !s.pushingEvents {
		s.q = nil
	}

	return createCmdOKResp(cmd)
}

func (s *session) handlePushPauseCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	s.paused = true
	return createCmdOKResp(cmd)
}

func (s *session) handlePushResumeCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	if !s.paused {
		// if we are not paused, there's nothing to do.
		return createCmdOKResp(cmd)
	}

	msg := createCmdOKResp(cmd)
	bytes, err := envelopePacket(msg)
	if err != nil {
		s.logger.Warnf("failed to marshal client message; err: %s", err)
		s.kill()
		return nil
	}

	s.queueWrite(bytes)
	for _, msg := range s.q {
		s.queueWrite(msg.payload)
	}

	s.q = nil
	s.paused = false
	return nil
}

func (s *session) handleUpdateConfigCmd(cmd *pb.ClientCommand) *pb.ServerMessage {
	if !s.greeted {
		return createCmdErrorResp(cmd, fmt.Errorf("client has not greeted server yet"))
	}

	if cmd.Config == nil {
		return createCmdErrorResp(cmd, fmt.Errorf("client passed nil configuration"))
	}

	old, neu := s.config, cmd.Config
	neu = s.validateConfig(*neu)

	if s.pushingState && old.StateSnapshotIntervalMs != neu.StateSnapshotIntervalMs {
		// reset the state ticker to the new interval, if we're pushing state.
		s.stateTicker.Stop()
		s.stateTicker = s.server.clock.Ticker(time.Duration(neu.StateSnapshotIntervalMs) * time.Millisecond)
	}

	s.config = neu
	resp := createCmdOKResp(cmd)
	resp.Payload.(*pb.ServerMessage_Response).Response.EffectiveConfig = s.config
	return resp
}

func (s *session) validateConfig(config pb.Configuration) *pb.Configuration {
	if min := uint64(MinStateSnapshotInterval.Milliseconds()); config.StateSnapshotIntervalMs < min {
		config.StateSnapshotIntervalMs = min
	}
	if max := uint64(MaxRetentionPeriod.Milliseconds()); config.RetentionPeriodMs > max {
		config.RetentionPeriodMs = max
	}
	return &config
}

func createCmdErrorResp(cmd *pb.ClientCommand, err error) *pb.ServerMessage {
	return &pb.ServerMessage{
		Version: ProtoVersionPb,
		Payload: &pb.ServerMessage_Response{
			Response: &pb.CommandResponse{
				Id:     cmd.Id,
				Result: pb.CommandResponse_ERR,
				Error:  err.Error(),
			},
		},
	}
}

func createCmdOKResp(cmd *pb.ClientCommand) *pb.ServerMessage {
	return &pb.ServerMessage{
		Version: ProtoVersionPb,
		Payload: &pb.ServerMessage_Response{
			Response: &pb.CommandResponse{
				Id:     cmd.Id,
				Result: pb.CommandResponse_OK,
			},
		},
	}
}
