package common

import "context"

// selectionProbeKey marks a context as belonging to a selection-policy recovery
// probe (the shadow request the prober mirrors against a currently-excluded
// upstream).
//
// A recovery probe MUST reach the upstream even when that upstream's circuit
// breaker is open: its whole job is to gather a fresh, real health signal for
// the selection policy. If the breaker denied the probe, the prober would
// record the breaker-open error as a probe FAILURE, keeping the upstream's
// error rate high and excluding it forever — the "probe/selection wedge". So a
// probe is breaker-INELIGIBLE: it neither acquires a permit nor records an
// outcome into the breaker. The breaker still recovers on its own via its
// HalfOpen trials on real traffic; the probe's signal flows through the
// selection tracker instead.
const selectionProbeKey ContextKey = "selection_probe"

// WithSelectionProbe marks ctx as a selection-policy recovery probe so the
// upstream executor treats it as breaker-ineligible. It does not mutate the
// request object, which the prober shares with live client traffic.
func WithSelectionProbe(ctx context.Context) context.Context {
	return context.WithValue(ctx, selectionProbeKey, true)
}

// IsSelectionProbe reports whether ctx was marked by WithSelectionProbe.
func IsSelectionProbe(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(selectionProbeKey).(bool)
	return v
}
