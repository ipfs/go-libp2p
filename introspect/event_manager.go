package introspect

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ipfs/go-log/v2"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/introspection/pb"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/multiformats/go-multiaddr"
)

var (
	jsType     = reflect.TypeOf(new(event.RawJSON)).Elem()
	peerIdType = reflect.TypeOf(new(peer.ID)).Elem()
	timeType   = reflect.TypeOf(new(time.Time)).Elem()
	maddrType  = reflect.TypeOf(new(multiaddr.Multiaddr)).Elem()
)

type eventManager struct {
	sync.RWMutex
	logger *log.ZapEventLogger

	inCh     <-chan interface{}
	outCh    chan *pb.Event
	metadata map[reflect.Type]*pb.EventType

	closed  bool
	closeCh chan struct{}
	closeWg sync.WaitGroup
}

func newEventManager(inCh <-chan interface{}) *eventManager {
	em := &eventManager{
		inCh:     inCh,
		logger:   log.Logger("introspection/event-manager"),
		outCh:    make(chan *pb.Event, cap(inCh)),
		closeCh:  make(chan struct{}),
		metadata: make(map[reflect.Type]*pb.EventType),
	}

	em.closeWg.Add(1)
	go em.processEvents()
	return em
}

func (em *eventManager) Close() error {
	em.Lock()
	defer em.Unlock()

	if em.closed {
		em.closeWg.Wait()
		return nil
	}

	close(em.closeCh)
	em.closeWg.Wait()
	return nil
}

func (em *eventManager) EventChan() <-chan *pb.Event {
	return em.outCh
}

func (em *eventManager) EventMetadata() []*pb.EventType {
	em.RLock()
	defer em.RUnlock()

	res := make([]*pb.EventType, 0, len(em.metadata))
	for k := range em.metadata {
		v := em.metadata[k]
		res = append(res, v)
	}
	return res
}

func (em *eventManager) processEvents() {
	defer em.closeWg.Done()
	defer close(em.outCh)

	for {
		select {
		case <-em.closeCh:
			return

		case evt, more := <-em.inCh:
			if !more {
				return
			}

			e, err := em.createEvent(evt)
			if err != nil {
				em.logger.Warnf("failed to process event; err: %s", err)
				continue
			}

			select {
			case em.outCh <- e:
			case <-em.closeCh:
				return
			default:
				em.logger.Warnf("failed to queue event")
			}
		}
	}
}

func (em *eventManager) createEvent(evt interface{}) (*pb.Event, error) {
	js, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event to json; err: %w", err)
	}

	ret := &pb.Event{
		Type:    &pb.EventType{},
		Ts:      uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		Content: string(js),
	}

	key := reflect.TypeOf(evt)

	em.RLock()
	et, ok := em.metadata[key]
	em.RUnlock()

	if ok {
		// just send the name if we've already seen the event before
		ret.Type.Name = et.Name
		return ret, nil
	}

	if key.Kind() != reflect.Struct {
		return nil, errors.New("event type must be a struct")
	}

	ret.Type.Name = key.Name()
	ret.Type.PropertyTypes = make([]*pb.EventType_EventProperty, 0, key.NumField())

	for i := 0; i < key.NumField(); i++ {
		fld := key.Field(i)
		fldType := fld.Type

		prop := &pb.EventType_EventProperty{}
		prop.Name = fld.Name

		if fldType.Kind() == reflect.Array || fldType.Kind() == reflect.Slice {
			prop.HasMultiple = true
			fldType = fld.Type.Elem()
		}

		switch fldType {
		case jsType:
			prop.Type = pb.EventType_EventProperty_JSON
		case peerIdType:
			prop.Type = pb.EventType_EventProperty_PEERID
		case maddrType:
			prop.Type = pb.EventType_EventProperty_MULTIADDR
		case timeType:
			prop.Type = pb.EventType_EventProperty_TIME
		default:
			switch fldType.Kind() {
			case reflect.String:
				prop.Type = pb.EventType_EventProperty_STRING
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
				reflect.Uint64, reflect.Float32, reflect.Float64:
				prop.Type = pb.EventType_EventProperty_NUMBER
			default:
				prop.Type = pb.EventType_EventProperty_JSON
			}
		}

		ret.Type.PropertyTypes = append(ret.Type.PropertyTypes, prop)
	}

	em.Lock()
	et, ok = em.metadata[key]
	if ok {
		// another write added the entry in the interim; discard ours.
		em.Unlock()
		ret.Type = et
		return ret, nil
	}
	em.metadata[key] = ret.Type
	em.Unlock()
	return ret, nil
}
