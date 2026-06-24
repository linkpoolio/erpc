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
