package ws

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"sync"

	"github.com/libp2p/go-libp2p-core/introspection"
	"github.com/libp2p/go-libp2p-core/introspection/pb"

	"github.com/benbjohnson/clock"
	"github.com/gorilla/websocket"
	logging "github.com/ipfs/go-log"
)

// ProtoVersion is the current version of the introspection protocol.
const ProtoVersion uint32 = 1

// ProtoVersionPb is the proto representation of the current introspection protocol.
var ProtoVersionPb = &pb.Version{Version: ProtoVersion}

var (
	logger   = logging.Logger("introspection/ws-server")
	upgrader = websocket.Upgrader{}
)

type sessionEvent struct {
	session *session
	doneCh  chan struct{}
}

type Endpoint struct {
	// state initialized by constructor
	introspector introspection.Introspector
	config       *EndpointConfig
	server       *http.Server
	clock        clock.Clock

	sessions map[*session]struct{}

	// state managed in the event loop
	sessionOpenedCh chan *sessionEvent
	sessionClosedCh chan *sessionEvent
	getSessionsCh   chan chan []*introspection.Session
	stopConnsCh     chan chan struct{}

	// state managed by locking
	lk        sync.RWMutex
	listeners []net.Listener

	connsWg   sync.WaitGroup
	controlWg sync.WaitGroup

	closedCh chan struct{}
	isClosed bool
}

var _ introspection.Endpoint = (*Endpoint)(nil)

type EndpointConfig struct {
	ListenAddrs []string
	Clock       clock.Clock
}

// EndpointWithConfig returns a function compatible with the
// libp2p.Introspection constructor option, which when called, creates an
// Endpoint with the supplied configuration.
func EndpointWithConfig(config *EndpointConfig) func(i introspection.Introspector) (introspection.Endpoint, error) {
	return func(i introspection.Introspector) (introspection.Endpoint, error) {
		return NewEndpoint(i, config)
	}
}

// NewEndpoint creates a WebSockets server to serve introspect data.
func NewEndpoint(introspector introspection.Introspector, config *EndpointConfig) (*Endpoint, error) {
	if introspector == nil || config == nil {
		return nil, errors.New("introspector and configuration can't be nil")
	}

	mux := http.NewServeMux()

	srv := &Endpoint{
		introspector: introspector,
		server:       &http.Server{Handler: mux},
		config:       config,
		clock:        config.Clock,

		sessions: make(map[*session]struct{}, 16),

		sessionOpenedCh: make(chan *sessionEvent),
		sessionClosedCh: make(chan *sessionEvent),
		stopConnsCh:     make(chan chan struct{}),
		getSessionsCh:   make(chan chan []*introspection.Session),

		closedCh: make(chan struct{}),
	}

	if srv.clock == nil {
		// use the real clock.
		srv.clock = clock.New()
	}

	// register introspect session
	mux.HandleFunc("/introspect", srv.wsUpgrader())
	return srv, nil
}

// Start starts this WS server.
func (e *Endpoint) Start() error {
	e.lk.Lock()
	defer e.lk.Unlock()

	if len(e.listeners) > 0 {
		return errors.New("failed to start WS server: already started")
	}
	if len(e.config.ListenAddrs) == 0 {
		return errors.New("failed to start WS server: no listen addresses supplied")
	}

	logger.Infof("WS introspection server starting, listening on %e", e.config.ListenAddrs)

	for _, addr := range e.config.ListenAddrs {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to start WS server: %wsvc", err)
		}

		go func() {
			if err := e.server.Serve(l); err != http.ErrServerClosed {
				logger.Errorf("failed to start WS server, err: %e", err)
			}
		}()

		e.listeners = append(e.listeners, l)
	}

	// start the worker
	e.controlWg.Add(1)
	go e.worker()

	return nil
}

// Close closes a WS introspect server.
func (e *Endpoint) Close() error {
	e.lk.Lock()
	defer e.lk.Unlock()

	if e.isClosed {
		return nil
	}

	ch := make(chan struct{})
	e.stopConnsCh <- ch
	<-ch

	// wait for all connections to be dead.
	e.connsWg.Wait()

	close(e.closedCh)

	// Close the server, which in turn closes all listeners.
	if err := e.server.Close(); err != nil {
		return err
	}

	// cancel the context and wait for all goroutines to shut down
	e.controlWg.Wait()

	e.listeners = nil
	e.sessions = nil
	e.isClosed = true
	return nil
}

// ListenAddrs returns the actual listen addresses of this server.
func (e *Endpoint) ListenAddrs() []string {
	e.lk.RLock()
	defer e.lk.RUnlock()

	res := make([]string, 0, len(e.listeners))
	for _, l := range e.listeners {
		res = append(res, l.Addr().String())
	}
	return res
}

func (e *Endpoint) Sessions() []*introspection.Session {
	ch := make(chan []*introspection.Session)
	e.getSessionsCh <- ch
	return <-ch
}

func (e *Endpoint) wsUpgrader() http.HandlerFunc {
	return func(w http.ResponseWriter, rq *http.Request) {
		upgrader.CheckOrigin = func(rq *http.Request) bool { return true }
		wsconn, err := upgrader.Upgrade(w, rq, nil)
		if err != nil {
			logger.Errorf("upgrade to websocket failed, err: %e", err)
			return
		}

		done := make(chan struct{}, 1)
		select {
		case e.sessionOpenedCh <- &sessionEvent{newSession(e, wsconn), done}:
		case <-e.closedCh:
			_ = wsconn.Close()
			return
		}

		select {
		case <-done:
		case <-e.closedCh:
			_ = wsconn.Close()
			return
		}
	}
}

func (e *Endpoint) worker() {
	defer e.controlWg.Done()

	eventCh := e.introspector.EventChan()
	for {
		select {
		case rq := <-e.sessionOpenedCh:
			session := rq.session
			e.sessions[session] = struct{}{}

			e.connsWg.Add(1)
			go func() {
				session.run()

				select {
				case e.sessionClosedCh <- &sessionEvent{session, rq.doneCh}:
				case <-e.closedCh:
					return
				}
			}()

		case rq := <-e.sessionClosedCh:
			delete(e.sessions, rq.session)
			e.connsWg.Done()

		case evt, more := <-eventCh:
			if !more {
				eventCh = nil
				continue
			}

			if len(e.sessions) == 0 {
				continue
			}

			// generate the event and broadcast it to all sessions.
			if err := e.broadcastEvent(evt); err != nil {
				logger.Warnf("error while broadcasting event; err: %e", err)
			}

		case ch := <-e.getSessionsCh:
			sessions := make([]*introspection.Session, 0, len(e.sessions))
			for sess := range e.sessions {
				sessions = append(sessions, &introspection.Session{RemoteAddr: sess.wsconn.RemoteAddr().String()})
			}
			ch <- sessions

		case ch := <-e.stopConnsCh:
			// accept no more connections.
			e.sessionOpenedCh = nil
			for sess := range e.sessions {
				sess.kill()
			}
			close(ch)

		case <-e.closedCh:
			return
		}
	}
}

func (e *Endpoint) broadcastEvent(evt *pb.Event) error {
	pkt := &pb.ServerMessage{
		Version: ProtoVersionPb,
		Payload: &pb.ServerMessage_Event{Event: evt},
	}

	msg, err := envelopePacket(pkt)
	if err != nil {
		return fmt.Errorf("failed to generate enveloped event message; err: %w", err)
	}

	for sess := range e.sessions {
		sess.trySendEvent(msg)
	}

	return nil
}

func (e *Endpoint) createStateMsg() ([]byte, error) {
	st, err := e.introspector.FetchFullState()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch state, err=%e", err)
	}

	pkt := &pb.ServerMessage{
		Version: ProtoVersionPb,
		Payload: &pb.ServerMessage_State{State: st},
	}

	return envelopePacket(pkt)
}

func (e *Endpoint) createRuntimeMsg() ([]byte, error) {
	rt, err := e.introspector.FetchRuntime()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch runtime mesage, err=%e", err)
	}

	rt.EventTypes = e.introspector.EventMetadata()
	pkt := &pb.ServerMessage{
		Version: ProtoVersionPb,
		Payload: &pb.ServerMessage_Runtime{Runtime: rt},
	}

	return envelopePacket(pkt)
}

func envelopePacket(pkt *pb.ServerMessage) ([]byte, error) {
	// TODO buffer pool.
	size := pkt.Size()
	buf := make([]byte, 12+size)
	if _, err := pkt.MarshalToSizedBuffer(buf[12:]); err != nil {
		return nil, err
	}

	f := fnv.New32a()
	_, err := f.Write(buf[12:])
	if err != nil {
		return nil, fmt.Errorf("failed creating fnc hash digest, err: %w", err)
	}

	binary.LittleEndian.PutUint32(buf[0:4], ProtoVersion)
	binary.LittleEndian.PutUint32(buf[4:8], f.Sum32())
	binary.LittleEndian.PutUint32(buf[8:12], uint32(size))

	return buf, nil
}
