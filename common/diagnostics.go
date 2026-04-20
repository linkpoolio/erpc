package common

import (
	"os"
	"strings"
	"sync/atomic"
)

// DiagnosticsConfig exposes operator-visible diagnostic toggles that
// genuinely need their own gate — i.e. knobs where turning the signal on
// has a meaningful runtime cost OR requires selecting a subset of
// subjects. Other diagnostic logs that were added during targeted
// investigations now emit at Trace level unconditionally and are
// filtered via the standard `logLevel` knob instead of living here.
//
// This is distinct from TracingConfig (OTEL spans) and LogLevel (global
// log level).
type DiagnosticsConfig struct {
	// WSFrameTraceNetworkIDs enables per-frame INFO logging for client
	// WebSocket connections whose networkId matches one of the entries.
	// Empty = off. Turns the WS write path off the zero-copy NextWriter
	// fast path onto buffered WriteMessage, so log volume AND bytes-
	// through-memory cost are non-trivial — leave empty in production
	// except for targeted investigations.
	//
	// The ERPC_WS_TRACE_NETWORK env var seeds this list as a single-entry
	// backdoor when config isn't live-editable; config takes precedence
	// when non-empty.
	WSFrameTraceNetworkIDs []string `yaml:"wsFrameTraceNetworkIds,omitempty" json:"wsFrameTraceNetworkIds"`
}

// SetDefaults folds the ERPC_WS_TRACE_NETWORK env var into the WS trace
// list when config doesn't already specify one. Safe to call on a nil
// receiver (no-op).
func (d *DiagnosticsConfig) SetDefaults() {
	if d == nil {
		return
	}
	if len(d.WSFrameTraceNetworkIDs) == 0 {
		if env := strings.TrimSpace(os.Getenv("ERPC_WS_TRACE_NETWORK")); env != "" {
			d.WSFrameTraceNetworkIDs = []string{env}
		}
	}
}

// IsNetworkWSFrameTraced is the hot-path check used by the WS write/read
// loops. Small list so linear scan; expected length 0 in prod.
func (d *DiagnosticsConfig) IsNetworkWSFrameTraced(networkId string) bool {
	if d == nil {
		return false
	}
	for _, id := range d.WSFrameTraceNetworkIDs {
		if id == networkId {
			return true
		}
	}
	return false
}

// activeDiagnostics holds the process-wide diagnostics config. Atomic
// pointer so hot-path call sites can read without locking and config
// reloads would be safe if/when supported.
var activeDiagnostics atomic.Pointer[DiagnosticsConfig]

// Diagnostics returns the active diagnostics config. Never nil — if no
// config has been registered yet (early boot, tests) it returns a
// defaulted zero value so call sites can dereference without a guard.
func Diagnostics() *DiagnosticsConfig {
	if d := activeDiagnostics.Load(); d != nil {
		return d
	}
	d := &DiagnosticsConfig{}
	d.SetDefaults()
	return d
}

// SetDiagnostics installs cfg as the process-wide diagnostics config.
// Typically called once by Config.SetDefaults. Nil is normalized to a
// defaulted zero value.
func SetDiagnostics(cfg *DiagnosticsConfig) {
	if cfg == nil {
		cfg = &DiagnosticsConfig{}
	}
	cfg.SetDefaults()
	activeDiagnostics.Store(cfg)
}
