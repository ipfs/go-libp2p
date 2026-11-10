package pstoremem

import (
	"errors"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type protoSegment struct {
	sync.RWMutex
	protocols map[peer.ID]map[protocol.ID]struct{}
}

type protoSegments [256]*protoSegment

func (s *protoSegments) get(p peer.ID) *protoSegment {
	return s[p[len(p)-1]]
}

var errTooManyProtocols = errors.New("too many protocols")

type memoryProtoBook struct {
	segments protoSegments

	maxProtos int
}

var _ pstore.ProtoBook = (*memoryProtoBook)(nil)

type ProtoBookOption func(book *memoryProtoBook) error

func WithMaxProtocols(num int) ProtoBookOption {
	return func(pb *memoryProtoBook) error {
		pb.maxProtos = num
		return nil
	}
}

func NewProtoBook(opts ...ProtoBookOption) (*memoryProtoBook, error) {
	pb := &memoryProtoBook{
		segments: func() (ret protoSegments) {
			for i := range ret {
				ret[i] = &protoSegment{
					protocols: make(map[peer.ID]map[protocol.ID]struct{}),
				}
			}
			return ret
		}(),
		maxProtos: 128,
	}

	for _, opt := range opts {
		if err := opt(pb); err != nil {
			return nil, err
		}
	}
	return pb, nil
}

func (pb *memoryProtoBook) SetProtocols(p peer.ID, protos ...protocol.ID) error {
	if pb.maxProtos < 0 {
		return errTooManyProtocols
	}

	newprotos := make(map[protocol.ID]struct{})
	for _, proto := range protos {
		newprotos[proto] = struct{}{}
		if len(newprotos) > pb.maxProtos {
			return errTooManyProtocols
		}
	}

	s := pb.segments.get(p)
	s.Lock()
	s.protocols[p] = newprotos
	s.Unlock()

	return nil
}

func (pb *memoryProtoBook) AddProtocols(p peer.ID, protos ...protocol.ID) error {
	s := pb.segments.get(p)
	s.Lock()
	defer s.Unlock()

	protomap := s.protocols[p]
	if len(protomap) > pb.maxProtos {
		return errTooManyProtocols
	}
	newprotos := make(map[protocol.ID]struct{})
	for _, proto := range protos {
		if _, exists := protomap[proto]; !exists {
			newprotos[proto] = struct{}{}
		}
		if len(protomap)+len(newprotos) > pb.maxProtos {
			return errTooManyProtocols
		}
	}
	if protomap == nil {
		protomap = make(map[protocol.ID]struct{}, len(newprotos))
		s.protocols[p] = protomap
	}
	for proto := range newprotos {
		protomap[proto] = struct{}{}
	}
	return nil
}

func (pb *memoryProtoBook) GetProtocols(p peer.ID) ([]protocol.ID, error) {
	s := pb.segments.get(p)
	s.RLock()
	defer s.RUnlock()

	out := make([]protocol.ID, 0, len(s.protocols[p]))
	for k := range s.protocols[p] {
		out = append(out, k)
	}

	return out, nil
}

func (pb *memoryProtoBook) RemoveProtocols(p peer.ID, protos ...protocol.ID) error {
	s := pb.segments.get(p)
	s.Lock()
	defer s.Unlock()

	protomap, ok := s.protocols[p]
	if !ok {
		// nothing to remove.
		return nil
	}

	for _, proto := range protos {
		delete(protomap, proto)
	}
	if len(protomap) == 0 {
		delete(s.protocols, p)
	}
	return nil
}

func (pb *memoryProtoBook) SupportsProtocols(p peer.ID, protos ...protocol.ID) ([]protocol.ID, error) {
	s := pb.segments.get(p)
	s.RLock()
	defer s.RUnlock()

	out := make([]protocol.ID, 0, len(protos))
	for _, proto := range protos {
		if _, ok := s.protocols[p][proto]; ok {
			out = append(out, proto)
		}
	}

	return out, nil
}

func (pb *memoryProtoBook) FirstSupportedProtocol(p peer.ID, protos ...protocol.ID) (protocol.ID, error) {
	s := pb.segments.get(p)
	s.RLock()
	defer s.RUnlock()

	for _, proto := range protos {
		if _, ok := s.protocols[p][proto]; ok {
			return proto, nil
		}
	}
	return "", nil
}

func (pb *memoryProtoBook) RemovePeer(p peer.ID) {
	s := pb.segments.get(p)
	s.Lock()
	delete(s.protocols, p)
	s.Unlock()
}
