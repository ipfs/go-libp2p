package pipetransport

import (
	"fmt"
	"net"
	"time"

	streammux "github.com/libp2p/go-stream-muxer"
)

var ErrHalfClosed = fmt.Errorf("tried to write to stream that was half closed")
var ErrAlreadyHalfClosed = fmt.Errorf("tried to half close stream that was already half closed")

type PipeStream struct {
	inbound  net.Conn
	outbound net.Conn
}

func NewPipeStream(inbound net.Conn, outbound net.Conn) *PipeStream {
	return &PipeStream{
		inbound:  inbound,
		outbound: outbound,
	}
}

func (s *PipeStream) Close() error {
	return s.outbound.Close()
}

func (s *PipeStream) Reset() error {
	err1 := s.inbound.Close()
	err2 := s.outbound.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (s *PipeStream) Read(b []byte) (int, error) {
	return s.inbound.Read(b)
}

func (s *PipeStream) Write(b []byte) (int, error) {
	return s.outbound.Write(b)
}

func (s *PipeStream) SetDeadline(t time.Time) error {
	err := s.inbound.SetDeadline(t)
	if err != nil {
		return err
	}
	err = s.outbound.SetDeadline(t)
	return err
}

func (s *PipeStream) SetReadDeadline(t time.Time) error {
	err := s.inbound.SetReadDeadline(t)
	if err != nil {
		return err
	}
	err = s.outbound.SetReadDeadline(t)
	return err
}

func (s *PipeStream) SetWriteDeadline(t time.Time) error {
	err := s.inbound.SetWriteDeadline(t)
	if err != nil {
		return err
	}
	err = s.outbound.SetWriteDeadline(t)
	return err
}

var _ streammux.Stream = (*PipeStream)(nil)
