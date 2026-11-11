package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// ErrReset is returned when reading or writing on a reset stream.
var ErrReset = errors.New("stream reset")

type StreamErrorCode uint32

type StreamError struct {
	ErrorCode      StreamErrorCode
	Remote         bool
	TransportError error
}

func (s *StreamError) Error() string {
	side := "local"
	if s.Remote {
		side = "remote"
	}
	if s.TransportError != nil {
		return fmt.Sprintf("stream reset (%s): code: 0x%x: transport error: %s", side, s.ErrorCode, s.TransportError)
	}
	return fmt.Sprintf("stream reset (%s): code: 0x%x", side, s.ErrorCode)
}

func (s *StreamError) Is(target error) bool {
	if tse, ok := target.(*StreamError); ok {
		return tse.ErrorCode == s.ErrorCode && tse.Remote == s.Remote
	}
	return false
}

func (s *StreamError) Unwrap() []error {
	return []error{ErrReset, s.TransportError}
}

const (
	StreamNoError                   StreamErrorCode = 0
	StreamProtocolNegotiationFailed StreamErrorCode = 0x1001
	StreamResourceLimitExceeded     StreamErrorCode = 0x1002
	StreamRateLimited               StreamErrorCode = 0x1003
	StreamProtocolViolation         StreamErrorCode = 0x1004
	StreamSupplanted                StreamErrorCode = 0x1005
	StreamGarbageCollected          StreamErrorCode = 0x1006
	StreamShutdown                  StreamErrorCode = 0x1007
	StreamGated                     StreamErrorCode = 0x1008
	StreamCodeOutOfRange            StreamErrorCode = 0x1009
)

// MuxedStream is a bidirectional io pipe within a connection.
type MuxedStream interface {
	io.Reader
	io.Writer

	// Close closes the stream.
	//
	// * Any buffered data for writing will be flushed.
	// * Future reads will fail.
	// * Any in-progress reads/writes will be interrupted.
	//
	// Close may be asynchronous and _does not_ guarantee receipt of the
	// data.
	//
	// Close closes the stream for both reading and writing.
	// Close is equivalent to calling `CloseRead` and `CloseWrite`. Importantly, Close will not wait for any form of acknowledgment.
	// If acknowledgment is required, the caller must call `CloseWrite`, then wait on the stream for a response (or an EOF),
	// then call Close() to free the stream object.
	//
	// When done with a stream, the user must call either Close() or `Reset()` to discard the stream, even after calling `CloseRead` and/or `CloseWrite`.
	io.Closer

	// CloseWrite closes the stream for writing but leaves it open for
	// reading.
	//
	// CloseWrite does not free the stream, users must still call Close or
	// Reset.
	CloseWrite() error

	// CloseRead closes the stream for reading but leaves it open for
	// writing.
	//
	// When CloseRead is called, all in-progress Read calls are interrupted with a non-EOF error and
	// no further calls to Read will succeed.
	//
	// The handling of new incoming data on the stream after calling this function is implementation defined.
	//
	// CloseRead does not free the stream, users must still call Close or
	// Reset.
	CloseRead() error

	// Reset closes both ends of the stream. Use this to tell the remote
	// side to hang up and go away.
	Reset() error

	// ResetWithError aborts both ends of the stream with `errCode`. `errCode` is sent
	// to the peer on a best effort basis. For transports that do not support sending
	// error codes to remote peer, the behavior is identical to calling Reset
	ResetWithError(errCode StreamErrorCode) error

	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// MuxedConn represents a connection to a remote peer that has been
// extended to support stream multiplexing.
//
// A MuxedConn allows a single net.Conn connection to carry many logically
// independent bidirectional streams of binary data.
//
// Together with network.ConnSecurity, MuxedConn is a component of the
// transport.CapableConn interface, which represents a "raw" network
// connection that has been "upgraded" to support the libp2p capabilities
// of secure communication and stream multiplexing.
type MuxedConn interface {
	// Close closes the stream muxer and the underlying net.Conn.
	io.Closer

	// CloseWithError closes the connection with errCode. The errCode is sent
	// to the peer.
	CloseWithError(errCode ConnErrorCode) error

	// IsClosed returns whether a connection is fully closed, so it can
	// be garbage collected.
	IsClosed() bool

	// OpenStream creates a new stream.
	OpenStream(context.Context) (MuxedStream, error)

	// AcceptStream accepts a stream opened by the other side.
	AcceptStream() (MuxedStream, error)

	// As finds the first conn in MuxedConn's wrapped types that matches target,
	// and if one is found, sets target to that conn value and returns true.
	// Otherwise, it returns false. Similar to errors.As.
	//
	// target must be a pointer to the type you are matching against.
	//
	// This is an EXPERIMENTAL API. Getting access to the underlying type can
	// lead to hard to debug issues. For example, if you mutate connection state
	// on the underlying type, hooks that relied on only mutating that state
	// from the wrapped connection would never be called.
	//
	// You very likely do not need to use this method.
	As(target any) bool
}

// Multiplexer wraps a net.Conn with a stream multiplexing
// implementation and returns a MuxedConn that supports opening
// multiple streams over the underlying net.Conn
type Multiplexer interface {
	// NewConn constructs a new connection
	NewConn(c net.Conn, isServer bool, scope PeerScope) (MuxedConn, error)
}
