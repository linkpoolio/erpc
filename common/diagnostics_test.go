package common

import (
	"testing"
)

func TestDiagnosticsConfig_SetDefaults_NilReceiverIsSafe(t *testing.T) {
	var d *DiagnosticsConfig
	d.SetDefaults() // must not panic
}

func TestDiagnosticsConfig_SetDefaults_EnvVarBackdoorSeedsWsTraceList(t *testing.T) {
	t.Setenv("ERPC_WS_TRACE_NETWORK", "evm:1101")

	d := &DiagnosticsConfig{}
	d.SetDefaults()

	if !d.IsNetworkWSFrameTraced("evm:1101") {
		t.Errorf("env var ERPC_WS_TRACE_NETWORK=evm:1101 must seed WSFrameTraceNetworkIDs when config doesn't specify one")
	}
	if d.IsNetworkWSFrameTraced("evm:1") {
		t.Errorf("only the env-var-named network should be traced; got trace for evm:1")
	}
}

func TestDiagnosticsConfig_SetDefaults_ExplicitListWinsOverEnvVar(t *testing.T) {
	t.Setenv("ERPC_WS_TRACE_NETWORK", "evm:1101")

	d := &DiagnosticsConfig{
		WSFrameTraceNetworkIDs: []string{"evm:10"},
	}
	d.SetDefaults()

	if d.IsNetworkWSFrameTraced("evm:1101") {
		t.Errorf("config-specified list must take precedence over env-var backdoor")
	}
	if !d.IsNetworkWSFrameTraced("evm:10") {
		t.Errorf("config-specified list must be honored")
	}
}

func TestDiagnostics_GlobalAccessor_NeverNil(t *testing.T) {
	// Reset to ensure we exercise the unset-pointer branch even if prior
	// tests have stored a value on this shared atomic.
	activeDiagnostics.Store(nil)

	d := Diagnostics()
	if d == nil {
		t.Fatalf("Diagnostics() must never return nil")
	}
}

func TestDiagnostics_SetDiagnostics_NilNormalizedToDefaults(t *testing.T) {
	SetDiagnostics(nil)
	d := Diagnostics()
	if d == nil {
		t.Fatalf("SetDiagnostics(nil) must leave Diagnostics() returning non-nil")
	}
}
