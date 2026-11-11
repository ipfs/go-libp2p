package rcmgr

import (
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"
)

// LogReporter is a trace reporter that logs blocked resource events.
// It consumes trace events and produces debug logs, ensuring consistency
// between logs and metrics by using the same trace event source.
// Its zero value uses the package's default logger.
type LogReporter struct {
	logger *slog.Logger
}

var _ TraceReporter = LogReporter{}

// NewLogReporter creates a reporter that logs blocked resources at debug level.
// If no logger is provided or it is nil, it uses the package's default logger.
// Pass the reporter to WithTraceReporter to use it instead of the default log reporter.
func NewLogReporter(loggers ...*slog.Logger) LogReporter {
	logger := log
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return LogReporter{logger: logger}
}

// ConsumeEvent implements TraceReporter by logging blocked resource events.
func (r LogReporter) ConsumeEvent(evt TraceEvt) {
	if r.logger == nil {
		r.logger = log
	}
	switch evt.Type {
	case TraceBlockReserveMemoryEvt:
		r.logBlockedMemory(evt)
	case TraceBlockAddStreamEvt:
		r.logBlockedStream(evt)
	case TraceBlockAddConnEvt:
		r.logBlockedConn(evt)
	}
}

func (r LogReporter) logBlockedMemory(evt TraceEvt) {
	r.logger.Debug("blocked memory reservation",
		"scope", evt.Name,
		"priority", evt.Priority,
		"current", evt.Memory,
		"attempted", evt.Delta,
	)
}

func (r LogReporter) logBlockedStream(evt TraceEvt) {
	var dir network.Direction
	if evt.DeltaIn != 0 {
		dir = network.DirInbound
	} else {
		dir = network.DirOutbound
	}

	r.logger.Debug("blocked stream",
		"scope", evt.Name,
		"direction", dir,
		"inbound", evt.StreamsIn,
		"outbound", evt.StreamsOut,
	)
}

func (r LogReporter) logBlockedConn(evt TraceEvt) {
	var dir network.Direction
	if evt.DeltaIn != 0 {
		dir = network.DirInbound
	} else {
		dir = network.DirOutbound
	}

	r.logger.Debug("blocked connection",
		"scope", evt.Name,
		"direction", dir,
		"usefd", evt.Delta != 0,
		"inbound", evt.ConnsIn,
		"outbound", evt.ConnsOut,
		"fd", evt.FD,
	)
}
