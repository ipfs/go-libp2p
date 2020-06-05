package mocknet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/mux"
	"github.com/libp2p/go-libp2p-core/network"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

var streamCounter int64

// stream implements network.Stream
type stream struct {
	notifLk sync.Mutex

	rstream *stream
	conn    *conn
	id      int64

	write     *io.PipeWriter
	read      *io.PipeReader
	toDeliver chan *transportObject

	reset  chan struct{}
	close  chan struct{}
	closed chan struct{}

	writeErr error

	protocol atomic.Value
	stat     network.Stat
}

var ErrClosed error = errors.New("stream closed")

type transportObject struct {
	msg         []byte
	arrivalTime time.Time
}

func newStreamPair() (*stream, *stream) {
	ra, wb := io.Pipe()
	rb, wa := io.Pipe()

	sa := newStream(wa, ra, network.DirOutbound)
	sb := newStream(wb, rb, network.DirInbound)
	sa.rstream = sb
	sb.rstream = sa
	return sa, sb
}

func newStream(w *io.PipeWriter, r *io.PipeReader, dir network.Direction) *stream {
	s := &stream{
		read:      r,
		write:     w,
		id:        atomic.AddInt64(&streamCounter, 1),
		reset:     make(chan struct{}, 1),
		close:     make(chan struct{}, 1),
		closed:    make(chan struct{}),
		toDeliver: make(chan *transportObject),
		stat:      network.Stat{Direction: dir},
	}

	go s.transport()
	return s
}

//  How to handle errors with writes?
func (s *stream) Write(p []byte) (n int, err error) {
	l := s.conn.link
	delay := l.GetLatency() + l.RateLimit(len(p))
	t := time.Now().Add(delay)

	// Copy it.
	cpy := make([]byte, len(p))
	copy(cpy, p)

	select {
	case <-s.closed: // bail out if we're closing.
		return 0, s.writeErr
	case s.toDeliver <- &transportObject{msg: cpy, arrivalTime: t}:
	}
	return len(p), nil
}

func (s *stream) ID() string {
	return strconv.FormatInt(s.id, 10)
}

func (s *stream) Protocol() protocol.ID {
	// Ignore type error. It means that the protocol is unset.
	p, _ := s.protocol.Load().(protocol.ID)
	return p
}

func (s *stream) Stat() network.Stat {
	return s.stat
}

func (s *stream) SetProtocol(proto protocol.ID) {
	s.protocol.Store(proto)
}

func (s *stream) Close() error {
	select {
	case s.close <- struct{}{}:
	default:
	}
	<-s.closed
	if s.writeErr != ErrClosed {
		return s.writeErr
	}
	return nil
}

func (s *stream) Reset() error {
	// Cancel any pending reads/writes with an error.
	s.write.CloseWithError(mux.ErrReset)
	s.read.CloseWithError(mux.ErrReset)

	select {
	case s.reset <- struct{}{}:
	default:
	}
	<-s.closed

	// No meaningful error case here.
	return nil
}

func (s *stream) teardown() {
	// at this point, no streams are writing.
	s.conn.removeStream(s)

	// Mark as closed.
	close(s.closed)
}

func (s *stream) Conn() network.Conn {
	return s.conn
}

func (s *stream) SetDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) SetReadDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) SetWriteDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) Read(b []byte) (int, error) {
	return s.read.Read(b)
}

// transport will grab message arrival times, wait until that time, and
// then write the message out when it is scheduled to arrive
func (s *stream) transport() {
	defer s.teardown()

	bufsize := 256
	buf := new(bytes.Buffer)
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	// cleanup
	defer timer.Stop()

	// writeBuf writes the contents of buf through to the s.Writer.
	// done only when arrival time makes sense.
	drainBuf := func() error {
		if buf.Len() > 0 {
			_, err := s.write.Write(buf.Bytes())
			if err != nil {
				return err
			}
			buf.Reset()
		}
		return nil
	}

	// deliverOrWait is a helper func that processes
	// an incoming packet. it waits until the arrival time,
	// and then writes things out.
	deliverOrWait := func(o *transportObject) error {
		buffered := len(o.msg) + buf.Len()

		// Yes, we can end up extending a timer multiple times if we
		// keep on making small writes but that shouldn't be too much of an
		// issue. Fixing that would be painful.
		if !timer.Stop() {
			// FIXME: So, we *shouldn't* need to do this but we hang
			// here if we don't... Go bug?
			select {
			case <-timer.C:
			default:
			}
		}
		delay := time.Until(o.arrivalTime)
		if delay >= 0 {
			timer.Reset(delay)
		} else {
			timer.Reset(0)
		}

		if buffered >= bufsize {
			select {
			case <-timer.C:
			case <-s.reset:
				select {
				case s.reset <- struct{}{}:
				default:
				}
				return mux.ErrReset
			}
			if err := drainBuf(); err != nil {
				return err
			}
			// write this message.
			_, err := s.write.Write(o.msg)
			if err != nil {
				return err
			}
		} else {
			buf.Write(o.msg)
		}
		return nil
	}

	for {
		// Reset takes precedent.
		select {
		case <-s.reset:
			s.writeErr = mux.ErrReset
			return
		default:
		}

		select {
		case <-s.reset:
			s.writeErr = mux.ErrReset
			return
		case <-s.close:
			if err := drainBuf(); err != nil {
				s.resetWith(err)
				return
			}
			s.writeErr = s.write.Close()
			if s.writeErr == nil {
				s.writeErr = ErrClosed
			}
			return
		case o := <-s.toDeliver:
			if err := deliverOrWait(o); err != nil {
				s.resetWith(err)
				return
			}
		case <-timer.C: // ok, due to write it out.
			if err := drainBuf(); err != nil {
				s.resetWith(err)
				return
			}
		}
	}
}

func (s *stream) resetWith(err error) {
	s.write.CloseWithError(err)
	s.read.CloseWithError(err)
	s.writeErr = err
}
