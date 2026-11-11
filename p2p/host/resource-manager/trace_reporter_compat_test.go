// Package rcmgr_test is deliberately an EXTERNAL test package. TraceReporter is
// exported, so third-party implementations may read TraceEvt.Time and
// TraceEvt.Scope. Testing from outside is what makes these tests evidence about
// that contract rather than about internal state.
package rcmgr_test

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

const tracePeer = peer.ID("\x00\x24\x08\x01\x12\x20tracetestpeerdeadbeefcafe000001")

// recordingReporter is a third-party reporter. Note what it can do from outside
// the package: implement the exported interface, keep exported TraceEvt values,
// read evt.Time, and compare evt.Scope against nil even though *scopeClass is
// unexported.
type recordingReporter struct{ evts []rcmgr.TraceEvt }

func (r *recordingReporter) ConsumeEvent(e rcmgr.TraceEvt) { r.evts = append(r.evts, e) }

func newManager(tb testing.TB, opts ...rcmgr.Option) network.ResourceManager {
	tb.Helper()
	m, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits), opts...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { m.Close() })
	return m
}

func openStreamScope(tb testing.TB, m network.ResourceManager) {
	tb.Helper()
	s, err := m.OpenStream(tracePeer, network.DirInbound)
	if err != nil {
		tb.Fatal(err)
	}
	if err := s.ReserveMemory(4096, network.ReservationPriorityAlways); err != nil {
		tb.Fatal(err)
	}
	s.ReleaseMemory(4096)
	s.Done()
}

func assertMetadata(tb testing.TB, label string, evts []rcmgr.TraceEvt) {
	tb.Helper()
	if len(evts) == 0 {
		tb.Fatalf("%s: reporter received no events", label)
	}
	var sawNamed bool
	for _, e := range evts {
		if e.Time == "" {
			tb.Errorf("%s: event %q has empty Time", label, e.Type)
		}
		if e.Name != "" {
			sawNamed = true
			if e.Scope == nil {
				tb.Errorf("%s: event %q has Name=%q but nil Scope", label, e.Type, e.Name)
			}
		}
	}
	if !sawNamed {
		tb.Errorf("%s: never saw a named event", label)
	}
}

// A third-party reporter must receive Time and Scope whether or not a trace file
// is configured. With a file configured the metadata must be populated before
// the reporter loop, not merely before the pending-write append.
func TestThirdPartyReporterReceivesMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(dir string, rep rcmgr.TraceReporter) []rcmgr.Option
	}{
		{"no trace file", func(_ string, rep rcmgr.TraceReporter) []rcmgr.Option {
			return []rcmgr.Option{rcmgr.WithTraceReporter(rep)}
		}},
		{"with trace file", func(dir string, rep rcmgr.TraceReporter) []rcmgr.Option {
			return []rcmgr.Option{rcmgr.WithTraceReporter(rep), rcmgr.WithTrace(dir + "/t.json")}
		}},
		{"trace file declared first", func(dir string, rep rcmgr.TraceReporter) []rcmgr.Option {
			return []rcmgr.Option{rcmgr.WithTrace(dir + "/t.json"), rcmgr.WithTraceReporter(rep)}
		}},
		{"alongside another reporter", func(_ string, rep rcmgr.TraceReporter) []rcmgr.Option {
			return []rcmgr.Option{rcmgr.WithTraceReporter(&recordingReporter{}), rcmgr.WithTraceReporter(rep)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := &recordingReporter{}
			m := newManager(t, tc.opts(t.TempDir(), rep)...)
			// Events seen before any user-driven operation are the ones emitted
			// from inside NewResourceManager. They must already carry metadata,
			// which pins that the decision is settled before the first push.
			if rep.evts[0].Type != rcmgr.TraceStartEvt {
				t.Errorf("first event is %q, want the start event", rep.evts[0].Type)
			}
			assertMetadata(t, tc.name+" (construction)", rep.evts)
			openStreamScope(t, m)
			assertMetadata(t, tc.name, rep.evts)
		})
	}
}

// scopeClass is unexported, so an external package cannot read .name -- but the
// Scope field is exported and scopeClass.MarshalJSON has a value receiver, so
// marshalling the event yields structured scope data. A third-party trace sink
// can legitimately depend on that.
func TestThirdPartyReporterCanMarshalScope(t *testing.T) {
	rep := &recordingReporter{}
	openStreamScope(t, newManager(t, rcmgr.WithTraceReporter(rep)))

	var n int
	for _, e := range rep.evts {
		if e.Scope == nil {
			continue
		}
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("json.Marshal(TraceEvt): %v", err)
		}
		var raw struct{ Scope map[string]any }
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatalf("round-tripping marshalled event: %v", err)
		}
		if _, ok := raw.Scope["Class"]; !ok {
			t.Errorf("marshalled Scope has no Class key: %s", b)
		}
		n++
	}
	if n == 0 {
		t.Fatal("no event carried a Scope to marshal")
	}
}

// The JSON trace file must carry non-empty Time and a Scope object. Only a clean
// io.EOF ends the decode loop; any other decoder error -- including a truncated
// or malformed record -- fails rather than silently shortening the count.
func TestTraceFileCarriesMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.json")
	m, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits), rcmgr.WithTrace(path))
	if err != nil {
		t.Fatal(err)
	}
	openStreamScope(t, m)
	m.Close() // flushes and closes the writer

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f) // the writer always gzips, whatever the extension
	if err != nil {
		t.Fatalf("trace file is not valid gzip: %v", err)
	}
	defer gz.Close()

	dec := json.NewDecoder(gz)
	var n, withTime, withScope int
	for {
		var raw map[string]any
		err := dec.Decode(&raw)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decoding trace record %d: %v", n+1, err)
		}
		n++
		if s, _ := raw["Time"].(string); s != "" {
			withTime++
		}
		if _, ok := raw["Scope"]; ok {
			withScope++
		}
	}
	if n == 0 {
		t.Fatal("no events decoded from trace file")
	}
	if withTime != n {
		t.Errorf("only %d/%d trace records carry a non-empty Time", withTime, n)
	}
	if withScope == 0 {
		t.Errorf("no trace record carries a Scope (of %d)", n)
	}
}
