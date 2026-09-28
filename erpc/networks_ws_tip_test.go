package erpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/architecture/evm"
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

// Once a WS newHeads tip N is observed (and about to be fan-out), HTTP
// tip resolution via EvmHighestLatestBlockNumber must not return < N —
// even if every local poller still reports N-1.
func TestNoteObservedLatestBlock_FloorsEvmHighestLatest(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	up := &common.UpstreamConfig{
		Type:     common.UpstreamTypeEvm,
		Id:       "rpc1",
		Endpoint: "http://rpc1.localhost",
		Evm:      &common.EvmUpstreamConfig{ChainId: 123},
	}

	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), `eth_chainId`)
		}).
		Reply(200).
		JSON([]byte(`{"result":"0x7b"}`))

	rateLimitersRegistry, _ := upstream.NewRateLimitersRegistry(context.Background(), &common.RateLimiterConfig{}, &log.Logger)
	metricsTracker := health.NewTracker(&log.Logger, "test", time.Minute)

	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(&log.Logger, vr, []*common.ProviderConfig{}, nil)
	require.NoError(t, err)

	ssr, err := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"},
		},
	})
	require.NoError(t, err)

	upstreamsRegistry := upstream.NewUpstreamsRegistry(
		ctx, &log.Logger, "test",
		[]*common.UpstreamConfig{up}, ssr, rateLimitersRegistry, vr, pr, nil,
		metricsTracker, nil,
	)

	networkConfig := &common.NetworkConfig{
		Architecture: common.ArchitectureEvm,
		Evm:          &common.EvmNetworkConfig{ChainId: 123},
	}
	network, err := NewNetwork(ctx, &log.Logger, "test", networkConfig,
		rateLimitersRegistry, upstreamsRegistry, metricsTracker, nil)
	require.NoError(t, err)

	upstreamsRegistry.Bootstrap(ctx)
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, upstreamsRegistry.GetInitializer().WaitForTasks(ctx))
	require.NoError(t, network.Bootstrap(ctx))
	time.Sleep(250 * time.Millisecond)

	upsList := upstreamsRegistry.GetNetworkUpstreams(ctx, util.EvmNetworkId(123))
	require.Len(t, upsList, 1)
	u := upsList[0]

	u.EvmStatePoller().SuggestLatestBlock(1000)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int64(1000), network.EvmHighestLatestBlockNumber(ctx))

	// Simulate the WS ingest path: a head arrives that we are about to
	// fan-out, but the lagging HTTP poller view is still at 1000.
	network.NoteObservedLatestBlock(ctx, 1001)

	got := network.EvmHighestLatestBlockNumber(ctx)
	assert.Equal(t, int64(1001), got,
		"after WS tip observation, highest latest must be ≥ delivered head")

	// Even if the network shared counter is somehow still behind (or a
	// local aggregator race computes 1000), the process-local high-water
	// mark from NoteObservedLatestBlock must clamp the return.
	require.NotNil(t, network.latestBlockShared)
	// Shared already at 1001 from NoteObserved; verify lastReturned alone
	// is enough by calling apply path with a lower computed tip via the
	// monotonic guard — EvmHighest after noting must never go backwards.
	assert.GreaterOrEqual(t, network.deliveredLatestBlock.Load(), int64(1001))
	assert.Equal(t, int64(1001), network.EvmHighestLatestBlockNumber(ctx))
}

// End-to-end through networkHandle.SuggestLatestBlock — the Indexer hook
// that runs before fan-out.
func TestNetworkHandle_SuggestLatestBlock_AdvancesNetworkTipBeforeFanOut(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	up := &common.UpstreamConfig{
		Type:     common.UpstreamTypeEvm,
		Id:       "bor-1",
		Endpoint: "http://bor1.localhost",
		Evm:      &common.EvmUpstreamConfig{ChainId: 123},
	}

	gock.New("http://bor1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), `eth_chainId`)
		}).
		Reply(200).
		JSON([]byte(`{"result":"0x7b"}`))

	rateLimitersRegistry, _ := upstream.NewRateLimitersRegistry(context.Background(), &common.RateLimiterConfig{}, &log.Logger)
	metricsTracker := health.NewTracker(&log.Logger, "test", time.Minute)

	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(&log.Logger, vr, []*common.ProviderConfig{}, nil)
	require.NoError(t, err)

	ssr, err := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"},
		},
	})
	require.NoError(t, err)

	upstreamsRegistry := upstream.NewUpstreamsRegistry(
		ctx, &log.Logger, "test",
		[]*common.UpstreamConfig{up}, ssr, rateLimitersRegistry, vr, pr, nil,
		metricsTracker, nil,
	)

	networkConfig := &common.NetworkConfig{
		Architecture: common.ArchitectureEvm,
		Evm:          &common.EvmNetworkConfig{ChainId: 123},
	}
	network, err := NewNetwork(ctx, &log.Logger, "test", networkConfig,
		rateLimitersRegistry, upstreamsRegistry, metricsTracker, nil)
	require.NoError(t, err)

	upstreamsRegistry.Bootstrap(ctx)
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, upstreamsRegistry.GetInitializer().WaitForTasks(ctx))
	require.NoError(t, network.Bootstrap(ctx))
	time.Sleep(250 * time.Millisecond)

	upsList := upstreamsRegistry.GetNetworkUpstreams(ctx, util.EvmNetworkId(123))
	require.Len(t, upsList, 1)
	upsList[0].EvmStatePoller().SuggestLatestBlock(90677358)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int64(90677358), network.EvmHighestLatestBlockNumber(ctx))

	handle := &networkHandle{nw: network}
	// Mirrors indexer.Ingest ordering: SuggestLatestBlock then fan-out.
	wsHeader := []byte(`{"number":"0x56789cf","hash":"0xabc","parentHash":"0xdef"}`)
	handle.SuggestLatestBlock("ws:bor-1", 90677359, wsHeader)

	assert.Equal(t, int64(90677359), upsList[0].EvmStatePoller().LatestBlock(),
		"per-upstream poller must advance")
	assert.Equal(t, int64(90677359), network.EvmHighestLatestBlockNumber(ctx),
		"network tip must advance before any client would see the WS head")
	assert.GreaterOrEqual(t, network.deliveredLatestBlock.Load(), int64(90677359),
		"process-local high-water mark must cover the delivered WS tip")
}

// Only a head from an upstream the selection policy keeps eligible lifts
// "latest": a fallback-tier (cordoned) or unknown source still feeds its own
// poller, but must not advertise a block no eligible upstream reports.
func TestNetworkHandle_SuggestLatestBlock_OnlyTipCandidatesLiftLatest(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, ups, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3e8",
	})
	require.Equal(t, int64(1000), network.EvmHighestLatestBlockNumber(ctx))
	var fallback *upstream.Upstream
	for _, u := range ups {
		if u.Id() == "fallback-1" {
			fallback = u
		}
	}
	require.NotNil(t, fallback)

	handle := &networkHandle{nw: network}
	handle.SuggestLatestBlock("ws:fallback-1", 1010, nil)
	handle.SuggestLatestBlock("ws:unknown", 1020, nil)

	assert.Equal(t, int64(1010), fallback.EvmStatePoller().LatestBlock(),
		"the fallback's own poller still advances")
	assert.Equal(t, int64(1000), network.EvmHighestLatestBlockNumber(ctx),
		"a cordoned or unknown source must not lift latest")

	handle.SuggestLatestBlock("ws:primary-1", 1001, nil)
	assert.Equal(t, int64(1001), network.EvmHighestLatestBlockNumber(ctx),
		"an eligible upstream's head floors latest")
}

// countingSharedCounter counts foreground remote reads of the delivered-head
// floor.
type countingSharedCounter struct {
	data.CounterInt64SharedVariable
	remoteReads atomic.Int32
}

func (c *countingSharedCounter) RefreshFromRemote(ctx context.Context) int64 {
	c.remoteReads.Add(1)
	return c.CounterInt64SharedVariable.RefreshFromRemote(ctx)
}

// At-tip "latest" reads must stay local: the floor receives remote updates via
// the shared counter's background sync, so the request path never waits on it.
func TestDeliveredHeadFloor_AtTipRequestsMakeNoRemoteReads(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, ups := setupServedTipNetworkWith(t, ctx, []servedTipFixture{
		{id: "a", chainID: 123, latestBlock: 1000},
		{id: "b", chainID: 123, latestBlock: 1000},
	}, &common.EvmServedTipConfig{})
	require.NotNil(t, network.latestBlockShared)
	counter := &countingSharedCounter{CounterInt64SharedVariable: network.latestBlockShared}
	network.latestBlockShared = counter

	for _, method := range []string{"eth_blockNumber", "eth_getBlockByNumber"} {
		for i := 0; i < 5; i++ {
			req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":["latest",false]}`))
			req.SetDirectives(&common.RequestDirectives{EnforceHighestBlock: true})
			req.SetNetwork(network)
			var result interface{} = "0x3e8"
			if method == "eth_getBlockByNumber" {
				result = map[string]interface{}{"number": "0x3e8", "hash": "0x01"}
			}
			jrr, err := common.NewJsonRpcResponse(1, result, nil)
			require.NoError(t, err)
			resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
			resp.SetUpstream(ups[0])

			out, err := evm.HandleNetworkPostForward(ctx, network, req, resp, nil)
			require.NoError(t, err, method)
			assert.Same(t, resp, out, method)
		}
	}
	assert.Zero(t, counter.remoteReads.Load())
}

// The delivered-head floor is network-wide: a use-upstream-scoped request is
// answered from its group's own head.
func TestDeliveredHeadFloor_SkipsSelectorScopedRequests(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _ := setupServedTipNetworkWith(t, ctx, []servedTipFixture{
		{id: "fast-1", chainID: 123, latestBlock: 1050, tags: []string{"family:fast"}},
		{id: "fast-2", chainID: 123, latestBlock: 1050, tags: []string{"family:fast"}},
		{id: "slow-1", chainID: 123, latestBlock: 1000, tags: []string{"family:slow"}},
		{id: "slow-2", chainID: 123, latestBlock: 1000, tags: []string{"family:slow"}},
	}, &common.EvmServedTipConfig{})
	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`))
	req.SetDirectives(&common.RequestDirectives{UseUpstream: "family:slow"})
	slowCtx := context.WithValue(ctx, common.RequestContextKey, req)

	network.NoteObservedLatestBlock(ctx, 1051)
	assert.Equal(t, int64(1000), network.EvmHighestLatestBlockNumber(slowCtx))
	assert.Equal(t, int64(1051), network.EvmHighestLatestBlockNumber(ctx))
}

// Projects sharing a process (and so a shared-state registry) must not floor
// each other's "latest" for the same chain.
func TestDeliveredHeadFloor_IsScopedPerProject(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _ := setupServedTipNetworkWith(t, ctx, []servedTipFixture{
		{id: "a", chainID: 123, latestBlock: 1000},
	}, &common.EvmServedTipConfig{})
	sibling, err := NewNetwork(ctx, &log.Logger, "sibling", network.cfg,
		network.rateLimitersRegistry, network.upstreamsRegistry, network.metricsTracker, nil)
	require.NoError(t, err)
	require.NotNil(t, sibling.latestBlockShared)

	network.NoteObservedLatestBlock(ctx, 1001)
	assert.Equal(t, int64(1001), network.latestBlockShared.GetValue())
	assert.Equal(t, int64(0), sibling.latestBlockShared.GetValue())
}
