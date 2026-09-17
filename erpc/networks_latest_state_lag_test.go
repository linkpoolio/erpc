package erpc

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/thirdparty"
	"github.com/erpc/erpc/upstream"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	util.ConfigureTestLogger()
}

// TestNetwork_EthCallLatest_SkipsLaggingUpstream is the regression for the
// Priority Pool totalQueued incident:
//
//	contract-events-slack → eRPC eth_call("latest") totalQueued()
//	selected stalled internal-eth-mainnet-reth-ws-0 (~8k behind TipHW)
//	→ returned 16355 instead of ~692
//
// Setup mirrors that failure mode: selection order prefers the lagging peer
// (rpc1), which would answer successfully with the stale ABI uint256, while a
// near-tip sibling (rpc2) has the correct tip-state value. The lag hard-gate
// must skip rpc1 and return rpc2's result.
func TestNetwork_EthCallLatest_SkipsLaggingUpstream(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	// ABI-encoded uint256 totalQueued values from the incident.
	const (
		staleTotalQueued = "0x0000000000000000000000000000000000000000000000000000000000003fe3" // 16355
		tipTotalQueued   = "0x00000000000000000000000000000000000000000000000000000000000002b4" // 692
	)

	// rpc1 must NOT receive eth_call — it is tip-lagged like reth-ws-0.
	// Times(0) fails the test if the gate fails open and routes here.
	gock.New("http://rpc1.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Times(0).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  staleTotalQueued,
		})

	gock.New("http://rpc2.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  tipTotalQueued,
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network := setupLatestStateLagTestNetwork(t, ctx)
	// Prefer the stalled peer first — same trap as selection picking reth-ws-0.
	network.PinUpstreamOrderForTest("rpc1", "rpc2")

	// Lag comes from the standard poller mocks: rpc1's head is 0x11118888
	// while rpc2 (TipHW) is at 0x22228888 — rpc1 lags by 286,326,784 blocks,
	// far beyond defaultMaxLatestStateLagBlocks=16 (incident: ~8k behind).

	// Priority Pool totalQueued() selector (0xa4baa10c) at "latest".
	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x362fa9d0bca5d19f743db50738345ce2b40ec99f","data":"0xa4baa10c"},"latest"]}`,
	))
	req.SetNetwork(network)

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	require.NotNil(t, resp.Upstream())
	assert.Equal(t, "rpc2", resp.Upstream().Id(),
		"lagging rpc1 (reth-ws-0 stand-in) must be skipped for eth_call(latest)")

	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	got := jrr.GetResultString()
	assert.Contains(t, got, tipTotalQueued, "must return tip-state totalQueued (~692), not stale 16355")
	assert.NotContains(t, got, staleTotalQueued, "must not return stalled-peer totalQueued 16355")
}

// TestNetwork_EthCallConcreteBlock_AllowsLaggingArchive ensures the lag gate
// only applies to moving latest/pending tags — a concrete block hex on an
// archive peer behind TipHW must still be allowed through.
func TestNetwork_EthCallConcreteBlock_AllowsLaggingArchive(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	gock.New("http://rpc1.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  "0xARCHIVE",
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network := setupLatestStateLagTestNetwork(t, ctx)
	network.PinUpstreamOrderForTest("rpc1", "rpc2")

	// rpc1 lags TipHW by ~286M blocks per the standard poller mocks; a
	// concrete historical block must still be servable by the lagging peer.
	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x123"},"0x100"]}`,
	))
	req.SetNetwork(network)

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	require.NotNil(t, resp.Upstream())
	assert.Equal(t, "rpc1", resp.Upstream().Id(), "concrete-block eth_call must still reach lagging archive peer")

	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	assert.Contains(t, jrr.GetResultString(), "0xARCHIVE")
}

// TestNetwork_EthCallLatest_ConfigurableLagThreshold verifies that
// evm.maxLatestStateLagBlocks overrides the default 16-block hard gate:
// with the threshold raised above the actual lag, the lagging (but
// selection-preferred) peer is allowed to serve again.
func TestNetwork_EthCallLatest_ConfigurableLagThreshold(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	gock.New("http://rpc1.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  "0xLAGGED_OK",
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The standard poller mocks pin rpc1 at 0x11118888 and rpc2 (TipHW) at
	// 0x22228888 — rpc1 lags by 0x11110000 = 286,326,784 blocks. A threshold
	// above that lag must let rpc1 (selection-preferred) serve again.
	network := setupLatestStateLagTestNetwork(t, ctx, func(cfg *common.NetworkConfig) {
		cfg.Evm.MaxLatestStateLagBlocks = i64(300_000_000)
	})
	network.PinUpstreamOrderForTest("rpc1", "rpc2")

	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x123"},"latest"]}`,
	))
	req.SetNetwork(network)

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	require.NotNil(t, resp.Upstream())
	assert.Equal(t, "rpc1", resp.Upstream().Id(),
		"lag 100 within configured threshold 200 must not be gated")
}

// TestNetwork_EthCallLatest_LagGateDisabled verifies that a non-positive
// evm.maxLatestStateLagBlocks disables the hard gate entirely.
func TestNetwork_EthCallLatest_LagGateDisabled(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	gock.New("http://rpc1.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  "0xGATE_OFF",
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// rpc1 lags TipHW by ~286M blocks per the standard poller mocks; with the
	// gate disabled it must still serve (pre-fix fail-open behavior).
	network := setupLatestStateLagTestNetwork(t, ctx, func(cfg *common.NetworkConfig) {
		cfg.Evm.MaxLatestStateLagBlocks = i64(-1)
	})
	network.PinUpstreamOrderForTest("rpc1", "rpc2")

	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x123"},"latest"]}`,
	))
	req.SetNetwork(network)

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	require.NotNil(t, resp.Upstream())
	assert.Equal(t, "rpc1", resp.Upstream().Id(),
		"disabled gate (≤0) must restore pre-fix fail-open routing")
}

func setupLatestStateLagTestNetwork(t *testing.T, ctx context.Context, cfgMut ...func(*common.NetworkConfig)) *Network {
	t.Helper()

	upstreamConfigs := []*common.UpstreamConfig{
		{
			Id:       "rpc1",
			Type:     common.UpstreamTypeEvm,
			Endpoint: "http://rpc1.localhost",
			Evm: &common.EvmUpstreamConfig{
				ChainId:             123,
				StatePollerInterval: common.Duration(10 * time.Second),
				StatePollerDebounce: common.Duration(50 * time.Millisecond),
			},
		},
		{
			Id:       "rpc2",
			Type:     common.UpstreamTypeEvm,
			Endpoint: "http://rpc2.localhost",
			Evm: &common.EvmUpstreamConfig{
				ChainId:             123,
				StatePollerInterval: common.Duration(10 * time.Second),
				StatePollerDebounce: common.Duration(50 * time.Millisecond),
			},
		},
	}

	networkConfig := &common.NetworkConfig{
		Architecture: common.ArchitectureEvm,
		Evm: &common.EvmNetworkConfig{
			ChainId: 123,
		},
	}
	for _, mut := range cfgMut {
		mut(networkConfig)
	}

	rateLimitersRegistry, err := upstream.NewRateLimitersRegistry(context.Background(), &common.RateLimiterConfig{}, &log.Logger)
	require.NoError(t, err)

	metricsTracker := health.NewTracker(&log.Logger, "test", time.Minute)

	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(&log.Logger, vr, []*common.ProviderConfig{}, nil)
	require.NoError(t, err)

	ssr, err := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{
				MaxItems:     100_000,
				MaxTotalSize: "1GB",
			},
		},
	})
	require.NoError(t, err)

	upstreamsRegistry := upstream.NewUpstreamsRegistry(
		ctx,
		&log.Logger,
		"test",
		upstreamConfigs,
		ssr,
		rateLimitersRegistry,
		vr,
		pr,
		nil,
		metricsTracker,
		nil,
	)

	upstreamsRegistry.Bootstrap(ctx)
	time.Sleep(100 * time.Millisecond)

	network, err := NewNetwork(ctx, &log.Logger, "test", networkConfig, rateLimitersRegistry, upstreamsRegistry, metricsTracker, nil)
	require.NoError(t, err)

	err = upstreamsRegistry.PrepareUpstreamsForNetwork(ctx, networkConfig.NetworkId())
	require.NoError(t, err)

	err = network.Bootstrap(ctx)
	require.NoError(t, err)

	return network
}
