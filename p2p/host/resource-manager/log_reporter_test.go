package rcmgr

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestLogReporter(t *testing.T) {
	tests := []struct {
		name string
		evt  TraceEvt
		want map[string]any
	}{
		{
			name: "memory",
			evt: TraceEvt{
				Type: TraceBlockReserveMemoryEvt, Name: "test-scope",
				Priority: 128, Delta: 1024, Memory: 2048,
			},
			want: map[string]any{
				"msg": "blocked memory reservation", "scope": "test-scope",
				"priority": float64(128), "current": float64(2048), "attempted": float64(1024),
			},
		},
		{
			name: "inbound stream",
			evt: TraceEvt{
				Type: TraceBlockAddStreamEvt, Name: "test-scope",
				DeltaIn: 1, StreamsIn: 5, StreamsOut: 3,
			},
			want: map[string]any{
				"msg": "blocked stream", "scope": "test-scope", "direction": float64(network.DirInbound),
				"inbound": float64(5), "outbound": float64(3),
			},
		},
		{
			name: "outbound stream",
			evt: TraceEvt{
				Type: TraceBlockAddStreamEvt, Name: "test-scope",
				DeltaOut: 1, StreamsIn: 5, StreamsOut: 3,
			},
			want: map[string]any{
				"msg": "blocked stream", "scope": "test-scope", "direction": float64(network.DirOutbound),
				"inbound": float64(5), "outbound": float64(3),
			},
		},
		{
			name: "inbound connection without fd",
			evt: TraceEvt{
				Type: TraceBlockAddConnEvt, Name: "test-scope",
				DeltaIn: 1, ConnsIn: 2, ConnsOut: 4, FD: 10,
			},
			want: map[string]any{
				"msg": "blocked connection", "scope": "test-scope", "direction": float64(network.DirInbound),
				"usefd": false, "inbound": float64(2), "outbound": float64(4), "fd": float64(10),
			},
		},
		{
			name: "outbound connection with fd",
			evt: TraceEvt{
				Type: TraceBlockAddConnEvt, Name: "test-scope",
				DeltaOut: 1, Delta: 1, ConnsIn: 2, ConnsOut: 4, FD: 10,
			},
			want: map[string]any{
				"msg": "blocked connection", "scope": "test-scope", "direction": float64(network.DirOutbound),
				"usefd": true, "inbound": float64(2), "outbound": float64(4), "fd": float64(10),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			reporter := NewLogReporter(logger.With("reporter", "custom"))
			reporter.ConsumeEvent(tt.evt)

			got := readLogReporterRecord(t, &buf)
			tt.want["level"] = "DEBUG"
			tt.want["reporter"] = "custom"
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLogReporterIgnoresOtherEvents(t *testing.T) {
	for _, typ := range []TraceEvtTyp{
		TraceStartEvt, TraceCreateScopeEvt, TraceDestroyScopeEvt,
		TraceReserveMemoryEvt, TraceReleaseMemoryEvt,
		TraceAddStreamEvt, TraceRemoveStreamEvt,
		TraceAddConnEvt, TraceRemoveConnEvt, "unknown",
	} {
		t.Run(string(typ), func(t *testing.T) {
			var buf bytes.Buffer
			reporter := NewLogReporter(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			reporter.ConsumeEvent(TraceEvt{Type: typ, Name: "test-scope"})
			require.Empty(t, buf.String())
		})
	}
}

func TestLogReporterRespectsLogLevel(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewLogReporter(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	reporter.ConsumeEvent(TraceEvt{Type: TraceBlockReserveMemoryEvt, Name: "test-scope", Delta: 1024})
	require.Empty(t, buf.String())
}

func TestLogReporterDefault(t *testing.T) {
	require.Same(t, log, NewLogReporter().logger)
	require.Same(t, log, NewLogReporter(nil).logger)

	for _, disableMetrics := range []bool{false, true} {
		name := "with metrics"
		if disableMetrics {
			name = "without metrics"
		}
		t.Run(name, func(t *testing.T) {
			var opts []Option
			if disableMetrics {
				opts = append(opts, WithMetricsDisabled())
			}
			mgr, err := NewResourceManager(NewFixedLimiter(InfiniteLimits), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, mgr.Close()) })

			var count int
			for _, reporter := range mgr.(*resourceManager).trace.reporters {
				if reporter, ok := reporter.(LogReporter); ok {
					count++
					require.Same(t, log, reporter.logger)
				}
			}
			require.Equal(t, 1, count)
		})
	}
}

func TestLogReporterZeroValue(t *testing.T) {
	var reporter LogReporter
	for _, typ := range []TraceEvtTyp{
		TraceBlockReserveMemoryEvt, TraceBlockAddStreamEvt, TraceBlockAddConnEvt,
	} {
		t.Run(string(typ), func(t *testing.T) {
			require.NotPanics(t, func() {
				reporter.ConsumeEvent(TraceEvt{Type: typ, Name: "test-scope", DeltaIn: 1, Delta: 1024})
			})
		})
	}
}

func TestLogReporterIntegration(t *testing.T) {
	for _, asPointer := range []bool{false, true} {
		name := "value reporter"
		if asPointer {
			name = "pointer reporter"
		}
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			reporter := NewLogReporter(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			var traceReporter TraceReporter = reporter
			if asPointer {
				traceReporter = &reporter
			}
			limits := PartialLimitConfig{System: ResourceLimits{
				Memory: 1024, ConnsInbound: BlockAllLimit, StreamsOutbound: BlockAllLimit,
			}}.Build(InfiniteLimits)
			mgr, err := NewResourceManager(NewFixedLimiter(limits), WithMetricsDisabled(), WithTraceReporter(traceReporter))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, mgr.Close()) })
			// A custom log reporter replaces the default, avoiding duplicate logs.
			require.Len(t, mgr.(*resourceManager).trace.reporters, 1)

			err = mgr.ViewSystem(func(scope network.ResourceScope) error {
				return scope.ReserveMemory(2048, network.ReservationPriorityAlways)
			})
			require.ErrorIs(t, err, network.ErrResourceLimitExceeded)
			require.Equal(t, map[string]any{
				"level": "DEBUG", "msg": "blocked memory reservation", "scope": "system",
				"priority": float64(network.ReservationPriorityAlways),
				"current":  float64(0), "attempted": float64(2048),
			}, readLogReporterRecord(t, &buf))

			conn, err := mgr.OpenConnection(network.DirInbound, true, dummyMA)
			if conn != nil {
				conn.Done()
			}
			require.ErrorIs(t, err, network.ErrResourceLimitExceeded)
			require.Equal(t, map[string]any{
				"level": "DEBUG", "msg": "blocked connection", "scope": "system",
				"direction": float64(network.DirInbound), "usefd": true,
				"inbound": float64(0), "outbound": float64(0), "fd": float64(0),
			}, readLogReporterRecord(t, &buf))

			stream, err := mgr.OpenStream(peer.ID("A"), network.DirOutbound)
			if stream != nil {
				stream.Done()
			}
			require.ErrorIs(t, err, network.ErrResourceLimitExceeded)
			require.Equal(t, map[string]any{
				"level": "DEBUG", "msg": "blocked stream", "scope": "system",
				"direction": float64(network.DirOutbound),
				"inbound":   float64(0), "outbound": float64(0),
			}, readLogReporterRecord(t, &buf))
		})
	}
}

func readLogReporterRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(buf)
	var record map[string]any
	require.NoError(t, decoder.Decode(&record))
	require.Contains(t, record, "time")
	delete(record, "time")
	var extra map[string]any
	require.ErrorIs(t, decoder.Decode(&extra), io.EOF, "expected exactly one log record")
	return record
}
