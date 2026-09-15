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

// TestNetwork_EthCallLatest_SkipsLaggingUpstream covers the Priority Pool
// totalQueued incident: a stalled WS peer answered eth_call("latest") with
// stale state while TipHW was thousands of blocks ahead. The lag hard-gate
// must skip the lagging peer and serve from a near-tip sibling.
func TestNetwork_EthCallLatest_SkipsLaggingUpstream(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	// rpc1 must NOT receive eth_call — it will be tip-lagged. Times(0)
	// ensures AssertNoPendingMocks fails if the gate fails open.
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
			"result":  "0xSTALE",
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
			"result":  "0xNEAR_TIP",
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network := setupLatestStateLagTestNetwork(t, ctx)
	network.PinUpstreamOrderForTest("rpc1", "rpc2")

	upsList := network.upstreamsRegistry.GetNetworkUpstreams(ctx, util.EvmNetworkId(123))
	require.Len(t, upsList, 2)
	byID := map[string]*upstream.Upstream{}
	for _, u := range upsList {
		byID[u.Config().Id] = u
	}
	// TipHW ≈ 1000 via rpc2; rpc1 sits 100 behind (> defaultMaxLatestStateLagBlocks=16).
	byID["rpc1"].EvmStatePoller().SuggestLatestBlock(900)
	byID["rpc2"].EvmStatePoller().SuggestLatestBlock(1000)
	time.Sleep(50 * time.Millisecond)

	req := common.NewNormalizedRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x123"},"latest"]}`,
	))
	req.SetNetwork(network)

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	require.NotNil(t, resp.Upstream())
	assert.Equal(t, "rpc2", resp.Upstream().Id(), "lagging rpc1 must be skipped for eth_call(latest)")

	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	assert.Contains(t, jrr.GetResultString(), "0xNEAR_TIP")
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

	upsList := network.upstreamsRegistry.GetNetworkUpstreams(ctx, util.EvmNetworkId(123))
	byID := map[string]*upstream.Upstream{}
	for _, u := range upsList {
		byID[u.Config().Id] = u
	}
	byID["rpc1"].EvmStatePoller().SuggestLatestBlock(900)
	byID["rpc2"].EvmStatePoller().SuggestLatestBlock(1000)
	time.Sleep(50 * time.Millisecond)

	// Concrete historical block well below both poller heads.
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

func setupLatestStateLagTestNetwork(t *testing.T, ctx context.Context) *Network {
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
