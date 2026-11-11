package rcmgr

import (
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const benchPeer = peer.ID("\x00\x24\x08\x01\x12\x20tracebenchpeerdeadbeefcafe00001")

type noopReporter struct{}

func (noopReporter) ConsumeEvent(TraceEvt) {}

// embeddingReporter is a distinct concrete type that embeds the in-tree
// reporter; it must not be mistaken for it.
type embeddingReporter struct{ StatsTraceReporter }

// needEventMetadata decides whether push builds TraceEvt.Time and
// TraceEvt.Scope. It identifies "reads neither" by concrete type, so it must
// match exactly the value NewResourceManager installs and err safe -- metadata
// on -- for every other shape.
func TestNeedEventMetadata(t *testing.T) {
	sr, err := NewStatsTraceReporter()
	if err != nil {
		t.Fatal(err)
	}
	// If the constructor stops returning the concrete value type the check
	// silently stops matching, and the optimization silently stops applying.
	if _, ok := any(sr).(StatsTraceReporter); !ok {
		t.Fatal("NewStatsTraceReporter no longer returns StatsTraceReporter by value")
	}

	for _, tc := range []struct {
		name string
		opts []Option
		want bool
	}{
		{"default: StatsTraceReporter only", nil, false},
		{"explicit value StatsTraceReporter", []Option{WithTraceReporter(sr)}, false},
		{"pointer to StatsTraceReporter", []Option{WithTraceReporter(&sr)}, true},
		{"type embedding StatsTraceReporter", []Option{WithTraceReporter(embeddingReporter{sr})}, true},
		{"third-party reporter", []Option{WithTraceReporter(noopReporter{})}, true},
		{"trace file", []Option{WithTrace(filepath.Join(t.TempDir(), "t.json"))}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewResourceManager(NewFixedLimiter(InfiniteLimits), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if got := m.(*resourceManager).trace.needEventMetadata; got != tc.want {
				t.Errorf("needEventMetadata = %v, want %v", got, tc.want)
			}
		})
	}
}

// BenchmarkOpenStreamScope reports the cost of one stream scope lifecycle with
// and without an observer of the event metadata, so the effect of skipping it
// is visible in a single run.
func BenchmarkOpenStreamScope(b *testing.B) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"metadata skipped", nil},
		{"metadata observed", []Option{WithTraceReporter(noopReporter{})}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			m, err := NewResourceManager(NewFixedLimiter(InfiniteLimits), tc.opts...)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := m.OpenStream(benchPeer, network.DirInbound)
				if err != nil {
					b.Fatal(err)
				}
				s.Done()
			}
		})
	}
}
