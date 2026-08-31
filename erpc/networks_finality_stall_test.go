package erpc

import (
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/require"
)

// TestCorroboratedFallbackFinalized covers the per-source rogue-high guard: a
// fallback's finalized is adopted only when its OWN latest supports it. The
// corroboration is per source on purpose — comparing against the max latest
// across all fallbacks would let a single rogue-high fallback ride another
// fallback's high tip, which is exactly the gap this guards.
func TestCorroboratedFallbackFinalized(t *testing.T) {
	// reorg window 0 (default): cap at the fallback's own latest.
	n := &Network{}

	t.Run("finalized below its own latest is adopted", func(t *testing.T) {
		require.Equal(t, int64(1000), n.corroboratedFallbackFinalized(1000, 1050))
	})
	t.Run("finalized equal to its own latest is adopted (reorg 0)", func(t *testing.T) {
		require.Equal(t, int64(1050), n.corroboratedFallbackFinalized(1050, 1050))
	})
	t.Run("rogue-high finalized above its own latest is rejected", func(t *testing.T) {
		require.Equal(t, int64(0), n.corroboratedFallbackFinalized(1100, 1050),
			"a fallback claiming finalized past its own tip must not be adopted")
	})
	t.Run("zero finalized or latest is rejected", func(t *testing.T) {
		require.Equal(t, int64(0), n.corroboratedFallbackFinalized(0, 1050))
		require.Equal(t, int64(0), n.corroboratedFallbackFinalized(1000, 0))
	})

	t.Run("reorg window tightens the bound to latest - window", func(t *testing.T) {
		nr := &Network{cfg: &common.NetworkConfig{
			Evm: &common.EvmNetworkConfig{FinalizedCorroborationReorgWindow: 64},
		}}
		// bound = 1050 - 64 = 986.
		require.Equal(t, int64(980), nr.corroboratedFallbackFinalized(980, 1050),
			"finalized within latest-window is adopted")
		require.Equal(t, int64(0), nr.corroboratedFallbackFinalized(1000, 1050),
			"finalized inside the reorg margin (986..1050) is rejected")
	})
}

// TestEvaluateFinalityStall exhaustively covers the pure finality-stall
// classifier — the timer, the margin, advance-resets-the-timer, the disable
// switches, and the edge transitions — deterministically, with no upstreams,
// gock, or wall-clock sleeps. nowMs is injected so "time" is just an argument.
func TestEvaluateFinalityStall(t *testing.T) {
	const window = int64(1000) // ms
	const margin = int64(100)  // blocks

	t.Run("advancing finalized is never stalled, even with a huge gap", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		// Finalized advances on every observation while latest sits far ahead.
		// Each advance resets the freeze timer, so the window can never elapse.
		for i := int64(0); i < 100; i++ {
			stalled, became, _ := evaluateFinalityStall(e, 1000+i, 1_000_000, i*10_000, window, margin)
			require.False(t, stalled, "advancing finalized must never be flagged (iter %d)", i)
			require.False(t, became, "no stall transition while advancing (iter %d)", i)
		}
	})

	t.Run("frozen within the window is not stalled", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		// t=0 seeds the value+timer; t=500ms (< 1000ms window) must not trip.
		_, _, _ = evaluateFinalityStall(e, 1000, 1_000_000, 10_000, window, margin)
		stalled, became, _ := evaluateFinalityStall(e, 1000, 1_000_000, 10_500, window, margin)
		require.False(t, stalled, "frozen but within the window must not be stalled")
		require.False(t, became)
	})

	t.Run("frozen past the window with a large gap is stalled (edge-triggered)", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		_, _, _ = evaluateFinalityStall(e, 1000, 1_000_000, 10_000, window, margin)
		stalled, became, _ := evaluateFinalityStall(e, 1000, 1_000_000, 11_500, window, margin)
		require.True(t, stalled, "frozen past window with gap > margin must be stalled")
		require.True(t, became, "first stalled observation must report the transition")
		// A second stalled observation must NOT re-report the transition.
		stalled2, became2, _ := evaluateFinalityStall(e, 1000, 1_000_000, 11_600, window, margin)
		require.True(t, stalled2)
		require.False(t, became2, "stall transition must fire only once")
	})

	t.Run("frozen past the window but small gap is NOT stalled (margin guard)", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		// latest only 50 ahead of finalized (< margin 100): a tight-finality chain.
		_, _, _ = evaluateFinalityStall(e, 1000, 1050, 10_000, window, margin)
		stalled, became, _ := evaluateFinalityStall(e, 1000, 1050, 15_000, window, margin)
		require.False(t, stalled, "small latest-finalized gap must never be classified as stalled")
		require.False(t, became)
	})

	t.Run("resuming after a stall clears it and reports the transition", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		_, _, _ = evaluateFinalityStall(e, 1000, 1_000_000, 10_000, window, margin)
		stalled, became, _ := evaluateFinalityStall(e, 1000, 1_000_000, 12_000, window, margin)
		require.True(t, stalled)
		require.True(t, became)
		// Finalized advances again → no longer stalled, resume transition reported.
		stalled2, _, resumed := evaluateFinalityStall(e, 1001, 1_000_000, 12_100, window, margin)
		require.False(t, stalled2, "an advance must clear the stall")
		require.True(t, resumed, "resume must be reported once")
		// And the timer is reset, so it isn't immediately stalled again.
		stalled3, _, _ := evaluateFinalityStall(e, 1001, 1_000_000, 12_200, window, margin)
		require.False(t, stalled3, "freeze timer must restart after an advance")
	})

	t.Run("disabled when window <= 0", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		_, _, _ = evaluateFinalityStall(e, 1000, 1_000_000, 10_000, 0, margin)
		stalled, _, _ := evaluateFinalityStall(e, 1000, 1_000_000, 1_000_000, 0, margin)
		require.False(t, stalled, "window <= 0 disables detection")
	})

	t.Run("disabled when margin <= 0", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		_, _, _ = evaluateFinalityStall(e, 1000, 1_000_000, 10_000, window, 0)
		stalled, _, _ := evaluateFinalityStall(e, 1000, 1_000_000, 1_000_000, window, 0)
		require.False(t, stalled, "margin <= 0 disables detection")
	})

	t.Run("incomplete inputs (zero finalized/latest) are not stalled", func(t *testing.T) {
		e := &finalizedProgressEntry{}
		stalled, _, _ := evaluateFinalityStall(e, 0, 1_000_000, 5000, window, margin)
		require.False(t, stalled, "finalized=0 must not be classified")
		e2 := &finalizedProgressEntry{}
		_, _, _ = evaluateFinalityStall(e2, 1000, 0, 10_000, window, margin)
		stalled2, _, _ := evaluateFinalityStall(e2, 1000, 0, 15_000, window, margin)
		require.False(t, stalled2, "latest=0 must not be classified")
	})
}
