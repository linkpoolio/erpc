package erpc

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
)

// finalizedFallbackConfigs builds two primaries (with a hair-trigger circuit
// breaker so a single failure opens them) and two fallback-tier upstreams whose
// state poller is DISABLED (StatePollerInterval: 0) — the quota-saving "probe
// off" shape. Because their poller never runs, the fallbacks' finalized height
// is only learnable via the on-demand refresh under test.
func finalizedFallbackConfigs() []*common.UpstreamConfig {
	primaryFailsafe := []*common.FailsafeConfig{{
		CircuitBreaker: &common.CircuitBreakerPolicyConfig{
			FailureThresholdCount:    1,
			FailureThresholdCapacity: 1,
			HalfOpenAfter:            common.Duration(5 * time.Minute),
		},
	}}
	return []*common.UpstreamConfig{
		{
			Type: common.UpstreamTypeEvm, Id: "primary-1",
			Endpoint: "http://rpc1.localhost",
			Failsafe: primaryFailsafe,
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "primary-2",
			Endpoint: "http://rpc2.localhost",
			Failsafe: primaryFailsafe,
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "fallback-1",
			Endpoint: "http://rpc3.localhost",
			Tags:     []string{common.TagTierFallback},
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(0), // poller OFF (quota-saving)
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "fallback-2",
			Endpoint: "http://rpc4.localhost",
			Tags:     []string{common.TagTierFallback},
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(0), // poller OFF (quota-saving)
			},
		},
	}
}

// mock503EthCall makes a host return a 503 for eth_call, used to trip a
// primary's circuit breaker deterministically.
func mock503EthCall(host string) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Reply(503).
		JSON(map[string]interface{}{
			"error": map[string]interface{}{"code": -32000, "message": "upstream down"},
		})
}

// TestFinalizedResolution_RefreshesPollerOffFallbacksWhenPrimariesDown is the
// regression for the CRE Base 8453 incident (2026-06-23): all primaries were
// circuit-broken while holding a stale finalized, and the healthy fallbacks —
// which run with their state poller disabled to save quota — never had their
// finalized learned, so eRPC kept serving the stale primary value.
//
// With the fix, once the primaries are down the network fires a throttled,
// finalized-only on-demand poll of the fallbacks, learns their higher finalized,
// and serves it — forward-only.
func TestFinalizedResolution_RefreshesPollerOffFallbacksWhenPrimariesDown(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const chainIdHex = "0x3e7"             // 999
	const primaryFinalized = int64(0x3e0)  // 992  — stale
	const fallbackFinalized = int64(0x3f0) // 1008 — true, higher

	// Primaries: reachable for state polling (stale finalized) but eth_call 503s
	// so we can trip their breakers. Fallbacks: poller is off, but their block
	// endpoint serves the true higher finalized when polled on demand.
	mockJsonRpcUpstream("rpc1.localhost", chainIdHex, "0x3e8", "0x3e0")
	mockJsonRpcUpstream("rpc2.localhost", chainIdHex, "0x3e8", "0x3e0")
	mockJsonRpcUpstream("rpc3.localhost", chainIdHex, "0x3f8", "0x3f0")
	mockJsonRpcUpstream("rpc4.localhost", chainIdHex, "0x3f8", "0x3f0")
	mock503EthCall("rpc1.localhost")
	mock503EthCall("rpc2.localhost")

	network, upr, _ := buildFailoverNetwork(t, ctx, finalizedFallbackConfigs(), true)
	upsList := upr.GetNetworkUpstreams(ctx, util.EvmNetworkId(999))
	require.Len(t, upsList, 4)

	// Baseline: primaries healthy, fallbacks' poller off → network serves the
	// primary finalized, fallbacks invisible.
	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == primaryFinalized
	}, 3*time.Second, 50*time.Millisecond,
		"baseline: finalized must come from the primaries (%d) while they are healthy", primaryFinalized)

	// Trip both primaries' circuit breakers (FailureThresholdCount=1).
	for _, ups := range upsList {
		if ups.Config().HasTag(common.TagTierFallback) {
			continue
		}
		req := common.NewNormalizedRequest([]byte(
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0xdead","data":"0x"},"latest"]}`))
		_, _ = ups.Forward(ctx, req, true, false)
	}
	require.Eventually(t, func() bool {
		for _, ups := range upsList {
			if ups.Config().HasTag(common.TagTierFallback) {
				continue
			}
			if !ups.IsDown() {
				return false
			}
		}
		return true
	}, 3*time.Second, 50*time.Millisecond, "both primaries' circuit breakers must open")

	// With no primary up, the on-demand refresh learns the poller-off fallbacks'
	// finalized and the network fails over to it.
	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == fallbackFinalized
	}, 5*time.Second, 100*time.Millisecond,
		"finalized must fail over to the poller-off fallback's higher value (%d) once primaries are down",
		fallbackFinalized)

	// Forward-only: never regress below the value already served.
	for i := 0; i < 30; i++ {
		require.GreaterOrEqual(t, network.EvmHighestFinalizedBlockNumber(ctx), fallbackFinalized,
			"finalized must never regress below the served fallback value (iter %d)", i)
	}
}

// finalityStallConfigs builds two circuit-CLOSED primaries (state poller on, so
// they report latest/finalized) and two fallback-tier upstreams whose poller is
// disabled (quota-saving "probe off"). The primaries are healthy and serve
// requests normally; the test mocks freeze their finalized while latest stays at
// tip to simulate a node-software finality bug.
func finalityStallConfigs() []*common.UpstreamConfig {
	return []*common.UpstreamConfig{
		{
			Type: common.UpstreamTypeEvm, Id: "primary-1",
			Endpoint: "http://rpc1.localhost",
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "primary-2",
			Endpoint: "http://rpc2.localhost",
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "fallback-1",
			Endpoint: "http://rpc3.localhost",
			Tags:     []string{common.TagTierFallback},
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(0), // poller OFF (quota-saving)
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "fallback-2",
			Endpoint: "http://rpc4.localhost",
			Tags:     []string{common.TagTierFallback},
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(0), // poller OFF (quota-saving)
			},
		},
	}
}

// setFinalityStallThresholds tightens the per-network finality-stall knobs so the
// behaviour is testable in milliseconds instead of the 90s/8192 production
// defaults.
func setFinalityStallThresholds(n *Network, window time.Duration, margin int64) {
	w := common.Duration(window)
	m := margin
	n.cfg.Evm.FinalityStallWindow = &w
	n.cfg.Evm.FinalityStallMargin = &m
}

// TestFinalizedResolution_DemotesCircuitClosedFinalityStalledPrimary reproduces
// the Base mainnet incident (2026-06-24): the primary is UP and circuit-CLOSED,
// `latest` tracks tip, but its `finalized` is FROZEN far behind tip (a node
// finality bug). PR #2's all-primaries-down failover is a no-op here because the
// primary is up. eRPC must detect the stalled primary, demote it as a finalized
// source, and fail over to the healthy fallback's higher finalized — forward-only.
func TestFinalizedResolution_DemotesCircuitClosedFinalityStalledPrimary(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const chainIdHex = "0x3e7"
	const primaryFinalized = int64(0xF0000)  // frozen, far behind tip
	const fallbackFinalized = int64(0xFFF00) // true, higher finalized
	const fallbackLatest = "0x100010"

	// Primaries: latest at tip (0x100000) but finalized FROZEN at 0xF0000
	// (~65k blocks behind — clearly a finality bug, not normal lag). Circuit
	// stays closed (no failures). Fallbacks: poller off, holding the correct
	// higher finalized.
	mockJsonRpcUpstream("rpc1.localhost", chainIdHex, "0x100000", "0xF0000")
	mockJsonRpcUpstream("rpc2.localhost", chainIdHex, "0x100000", "0xF0000")
	mockJsonRpcUpstream("rpc3.localhost", chainIdHex, fallbackLatest, "0xFFF00")
	mockJsonRpcUpstream("rpc4.localhost", chainIdHex, fallbackLatest, "0xFFF00")

	network, upr, _ := buildFailoverNetwork(t, ctx, finalityStallConfigs(), true)
	require.Len(t, upr.GetNetworkUpstreams(ctx, util.EvmNetworkId(999)), 4)
	setFinalityStallThresholds(network, 200*time.Millisecond, 100)

	// Seed the finalized-advance record while the primary is still trusted.
	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == primaryFinalized
	}, 3*time.Second, 50*time.Millisecond,
		"baseline: finalized comes from the primary (%d) before the stall window elapses", primaryFinalized)

	// Let the freeze window elapse; the primary's finalized never advances.
	time.Sleep(300 * time.Millisecond)

	// The stalled-but-circuit-closed primary must be demoted and finalized must
	// fail over to the fallback's higher value.
	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == fallbackFinalized
	}, 5*time.Second, 100*time.Millisecond,
		"finalized must fail over to the fallback's higher value (%d) once the primary is finality-stalled",
		fallbackFinalized)

	// Forward-only: never regress below the value already served.
	for i := 0; i < 30; i++ {
		require.GreaterOrEqual(t, network.EvmHighestFinalizedBlockNumber(ctx), fallbackFinalized,
			"finalized must never regress below the served fallback value (iter %d)", i)
	}
}

// TestFinalizedResolution_DoesNotDemoteHealthyPrimaryWithSmallFinalityGap is the
// false-positive guard: a healthy primary whose finalized sits only slightly
// behind latest (gap < margin) must NEVER be demoted, even after the staleness
// window elapses — so a chain with legitimately tight finality is not mistaken
// for stalled. The happy path stays unchanged: the primary serves finalized and
// the higher fallback value is never adopted.
func TestFinalizedResolution_DoesNotDemoteHealthyPrimaryWithSmallFinalityGap(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const chainIdHex = "0x3e7"
	const primaryFinalized = int64(0xFFFC0) // only 64 blocks behind latest
	const fallbackFinalized = int64(0x1000C0)

	// Primary: latest 0x100000, finalized 0xFFFC0 → gap 64, below the 100-block
	// margin, so even with finalized frozen it is NOT finality-stalled. Fallback
	// holds a higher finalized that must NOT be adopted.
	mockJsonRpcUpstream("rpc1.localhost", chainIdHex, "0x100000", "0xFFFC0")
	mockJsonRpcUpstream("rpc2.localhost", chainIdHex, "0x100000", "0xFFFC0")
	mockJsonRpcUpstream("rpc3.localhost", chainIdHex, "0x100100", "0x1000C0")
	mockJsonRpcUpstream("rpc4.localhost", chainIdHex, "0x100100", "0x1000C0")

	network, upr, _ := buildFailoverNetwork(t, ctx, finalityStallConfigs(), true)
	require.Len(t, upr.GetNetworkUpstreams(ctx, util.EvmNetworkId(999)), 4)
	setFinalityStallThresholds(network, 200*time.Millisecond, 100)

	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == primaryFinalized
	}, 3*time.Second, 50*time.Millisecond,
		"baseline: healthy primary serves its finalized (%d)", primaryFinalized)

	// Wait well past the staleness window; the small gap must keep the primary
	// trusted (the margin guard, not the window, is what protects it).
	time.Sleep(400 * time.Millisecond)

	for i := 0; i < 20; i++ {
		got := network.EvmHighestFinalizedBlockNumber(ctx)
		require.Equal(t, primaryFinalized, got,
			"healthy small-gap primary must not be demoted; must keep serving %d, not the fallback's %d (iter %d)",
			primaryFinalized, fallbackFinalized, i)
		time.Sleep(20 * time.Millisecond)
	}
}
