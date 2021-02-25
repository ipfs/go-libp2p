package holepunch

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	ma "github.com/multiformats/go-multiaddr"
)

// WithTracer is a HolePunchService option that enables hole punching tracing
func WithTracer(tr EventTracer) Option {
	return func(hps *HolePunchService) error {
		hps.tracer = &Tracer{tr: tr, self: hps.host.ID()}
		return nil
	}
}

type Tracer struct {
	tr   EventTracer
	self peer.ID
}

type EventTracer interface {
	Trace(evt *Event)
}

type Event struct {
	Timestamp int64       // UNIX nanos
	Peer      peer.ID     // local peer ID
	Remote    peer.ID     // remote peer ID
	Type      string      // event type
	Evt       interface{} // the actual event
}

// Event Types
const (
	DirectDialEvtT       = "DirectDial"
	ProtocolErrorEvtT    = "ProtocolError"
	StartHolePunchEvtT   = "StartHolePunch"
	EndHolePunchEvtT     = "EndHolePunch"
	HolePunchAttemptEvtT = "HolePunchAttempt"
)

// Event Objects
type DirectDialEvt struct {
	Success      bool
	EllapsedTime time.Duration
	Error        string `json:",omitempty"`
}

type ProtocolErrorEvt struct {
	Error string
}

type StartHolePunchEvt struct {
	RemoteAddrs []string
	RTT         time.Duration
}

type EndHolePunchEvt struct {
	Success      bool
	EllapsedTime time.Duration
	Error        string `json:",omitempty"`
}

type HolePunchAttemptEvt struct {
	Attempt int
}

// Tracer interface
func (t *Tracer) DirectDialSuccessful(p peer.ID, dt time.Duration) {
	if t == nil {
		return
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      DirectDialEvtT,
		Evt: &DirectDialEvt{
			Success:      true,
			EllapsedTime: dt,
		},
	})
}

func (t *Tracer) DirectDialFailed(p peer.ID, dt time.Duration, err error) {
	if t == nil {
		return
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      DirectDialEvtT,
		Evt: &DirectDialEvt{
			Success:      false,
			EllapsedTime: dt,
			Error:        err.Error(),
		},
	})
}

func (t *Tracer) ProtocolError(p peer.ID, err error) {
	if t == nil {
		return
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      ProtocolErrorEvtT,
		Evt: &ProtocolErrorEvt{
			Error: err.Error(),
		},
	})
}

func (t *Tracer) StartHolePunch(p peer.ID, obsAddrs []ma.Multiaddr, rtt time.Duration) {
	if t == nil {
		return
	}

	addrs := make([]string, 0, len(obsAddrs))
	for _, a := range obsAddrs {
		addrs = append(addrs, a.String())
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      StartHolePunchEvtT,
		Evt: &StartHolePunchEvt{
			RemoteAddrs: addrs,
			RTT:         rtt,
		},
	})
}

func (t *Tracer) EndHolePunch(p peer.ID, dt time.Duration, err error) {
	if t == nil {
		return
	}

	evt := &EndHolePunchEvt{
		Success:      err == nil,
		EllapsedTime: dt,
	}
	if err != nil {
		evt.Error = err.Error()
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      EndHolePunchEvtT,
		Evt:       evt,
	})
}

func (t *Tracer) HolePunchAttempt(p peer.ID, attempt int) {
	if t == nil {
		return
	}

	t.tr.Trace(&Event{
		Timestamp: time.Now().UnixNano(),
		Peer:      t.self,
		Remote:    p,
		Type:      HolePunchAttemptEvtT,
		Evt: &HolePunchAttemptEvt{
			Attempt: attempt,
		},
	})
}
