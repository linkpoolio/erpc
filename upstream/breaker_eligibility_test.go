package upstream

import (
	"context"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/require"
)

// TestUpstreamBreakerEligible_SelectionProbe is the guard for the probe/selection
// wedge follow-up: a selection-recovery probe must be breaker-INELIGIBLE so it
// reaches an excluded upstream even while the breaker is open (gathering a real
// health signal) instead of being denied a permit — which the prober would
// record as a probe failure, excluding the upstream forever.
func TestUpstreamBreakerEligible_SelectionProbe(t *testing.T) {
	t.Run("a selection probe is breaker-ineligible", func(t *testing.T) {
		ctx := common.WithSelectionProbe(context.Background())
		require.False(t, upstreamBreakerEligible(ctx, nil, false),
			"a selection-recovery probe must not acquire/record a breaker permit")
	})

	t.Run("ordinary traffic stays breaker-eligible", func(t *testing.T) {
		require.True(t, upstreamBreakerEligible(context.Background(), nil, false),
			"non-probe, non-hedge requests must remain breaker-eligible")
	})

	t.Run("hedge attempts remain ineligible regardless of probe flag", func(t *testing.T) {
		require.False(t, upstreamBreakerEligible(context.Background(), nil, true))
	})
}

// TestSelectionProbeContextRoundtrip verifies the marker helpers.
func TestSelectionProbeContextRoundtrip(t *testing.T) {
	require.False(t, common.IsSelectionProbe(context.Background()))
	require.True(t, common.IsSelectionProbe(common.WithSelectionProbe(context.Background())))
	require.False(t, common.IsSelectionProbe(nil))
}
