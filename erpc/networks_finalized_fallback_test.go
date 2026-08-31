package erpc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/erpc/erpc/architecture/evm"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
)

// finalityStallConfigs builds two circuit-CLOSED primaries and two fallback-tier
// upstreams, all with the state poller on. probe:off fallbacks in production are
// still state-polled (probe:off only opts out of selection-policy shadow-mirror
// traffic), so the fallbacks here poll their latest/finalized exactly as prod
// does. The primaries are healthy and serve requests normally; the test mocks
// freeze their finalized while latest stays at tip to simulate a node finality bug.
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
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
			},
		},
		{
			Type: common.UpstreamTypeEvm, Id: "fallback-2",
			Endpoint: "http://rpc4.localhost",
			Tags:     []string{common.TagTierFallback},
			Evm: &common.EvmUpstreamConfig{
				ChainId:             999,
				StatePollerInterval: common.Duration(100 * time.Millisecond),
				StatePollerDebounce: common.Duration(20 * time.Millisecond),
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

// mockGetBlockByNumberConcrete answers eth_getBlockByNumber for a specific hex
// block number (the value eRPC re-fetches / interpolates to once it knows the
// true finalized height). Hex must be lowercase to match eRPC's NormalizeHex.
func mockGetBlockByNumberConcrete(host, hexNum string) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			b := util.SafeReadBody(r)
			return strings.Contains(b, "eth_getBlockByNumber") && strings.Contains(b, hexNum)
		}).
		Reply(200).
		JSON([]byte(fmt.Sprintf(
			`{"jsonrpc":"2.0","id":1,"result":{"number":"%s","hash":"0xabc","timestamp":"0x6702a8e0"}}`, hexNum)))
}

// TestEthGetBlockByNumber_Finalized_ReturnsHealthyNodeBlockWhenPrimaryStalled is
// the end-to-end, RPC-method-level proof of the Base incident fix: a client that
// calls eth_getBlockByNumber("finalized") — exactly what Chainlink MultiNode polls
// for finality — must receive the HEALTHY node's higher finalized block when the
// primary is finality-stalled, not the primary's frozen value.
//
// This drives the real network.Forward path, so it exercises the
// enforce-highest-finalized / tag-interpolation machinery on top of the stall
// detection — the whole chain a downstream node actually hits.
func TestEthGetBlockByNumber_Finalized_ReturnsHealthyNodeBlockWhenPrimaryStalled(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const chainIdHex = "0x3e7"
	const stalledFinalized = "0xf0000"      // frozen, far behind tip (the bug)
	const healthyFinalizedHex = "0xfff00"   // the true, higher finalized
	const healthyFinalized = int64(0xfff00) // == 1048320

	// Primaries: circuit-closed, latest at tip, finalized FROZEN far behind.
	// Fallbacks: healthy, poller on, holding the true higher finalized.
	mockJsonRpcUpstream("rpc1.localhost", chainIdHex, "0x100000", stalledFinalized)
	mockJsonRpcUpstream("rpc2.localhost", chainIdHex, "0x100000", stalledFinalized)
	mockJsonRpcUpstream("rpc3.localhost", chainIdHex, "0x100010", healthyFinalizedHex)
	mockJsonRpcUpstream("rpc4.localhost", chainIdHex, "0x100010", healthyFinalizedHex)
	// Any upstream may be asked for the concrete higher block (interpolation or
	// the enforce-highest-finalized re-fetch, which excludes the stale server).
	for _, h := range []string{"rpc1.localhost", "rpc2.localhost", "rpc3.localhost", "rpc4.localhost"} {
		mockGetBlockByNumberConcrete(h, healthyFinalizedHex)
	}

	// All four pollers on so the fallbacks' finalized is known without waiting on
	// the on-demand refresh (that path is covered by the other tests).
	network, upr, _ := buildFailoverNetwork(t, ctx, failoverUpstreamConfigs(), true)
	require.Len(t, upr.GetNetworkUpstreams(ctx, util.EvmNetworkId(999)), 4)
	setFinalityStallThresholds(network, 100*time.Millisecond, 100)

	// Resolution-level guard: once the primaries are demoted as finality-stalled,
	// the network finalized resolves to the healthy fallback's higher value.
	require.Eventually(t, func() bool {
		return network.EvmHighestFinalizedBlockNumber(ctx) == healthyFinalized
	}, 5*time.Second, 100*time.Millisecond,
		"network finalized must fail over to the healthy node's value (%d) once the primary is stalled", healthyFinalized)

	// Method-level proof: a client's eth_getBlockByNumber("finalized") returns the
	// healthy node's block, not the stalled primary's frozen one.
	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["finalized",false]}`))
	req.SetNetwork(network)
	// eth_getBlockByNumber finality enforcement is gated on this directive
	// (defaults to true in production; set explicitly here since the test builds
	// requests programmatically rather than via the HTTP server).
	req.SetDirectives(&common.RequestDirectives{EnforceHighestBlock: true})
	// Replicate the project's request path: network.Forward wrapped by the EVM
	// network post-forward hook (which is where enforce-highest-finalized lives).
	resp, err := network.Forward(ctx, req)
	resp, err = evm.HandleNetworkPostForward(ctx, network, req, resp, err)
	require.NoError(t, err, "eth_getBlockByNumber(finalized) must succeed")
	require.NotNil(t, resp)
	defer resp.Release()

	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	numHex, err := jrr.PeekStringByPath(ctx, "number")
	require.NoError(t, err)
	got, err := common.HexToInt64(numHex)
	require.NoError(t, err)
	require.Equal(t, healthyFinalized, got,
		"eth_getBlockByNumber(finalized) must return the healthy node's block %d, not the stalled primary's frozen finalized", healthyFinalized)
}
