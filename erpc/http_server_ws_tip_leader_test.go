package erpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/internal/policy"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/upstream"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	promUtil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	util.ConfigureTestLogger()
}

// EnforceHighestBlock must re-fetch the concrete tip via EvmLeaderUpstream
// (the poller advanced by SuggestLatestBlock), not fail-open to a lagging
// sibling that answered "latest" first.
func TestHttpServer_GetBlockByNumberLatest_RefetchPinsEvmLeaderUpstream(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	const tip = int64(0x22228889)
	tipHex := "0x22228889"
	var leaderHits atomic.Int64

	gock.New("http://rpc2.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			if !strings.Contains(body, "eth_getBlockByNumber") || !strings.Contains(body, tipHex) {
				return false
			}
			leaderHits.Add(1)
			return true
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"0x22228889","hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","parentHash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","timestamp":"0x6702a8f1"}}`))

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(100 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
							Integrity: &common.EvmIntegrityConfig{
								EnforceHighestBlock: util.BoolPtr(true),
							},
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{MaxAttempts: 3},
							},
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Endpoint: "http://rpc1.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
					{
						Id:       "rpc2",
						Endpoint: "http://rpc2.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
				},
			},
		},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	prj, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	policy.OverrideAllForTest(prj.policyEngine)
	// Prefer lagging rpc1 for the initial "latest" so EnforceHighestBlock re-fetch runs.
	policy.OverrideOrderForTest(prj.policyEngine, "evm:123", "rpc1", "rpc2")

	time.Sleep(500 * time.Millisecond)

	nw, err := prj.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)

	var leader *upstream.Upstream
	for _, u := range nw.upstreamsRegistry.GetNetworkUpstreams(context.Background(), "evm:123") {
		if u.Id() == "rpc2" {
			leader = u
			break
		}
	}
	require.NotNil(t, leader)

	// Mirror WS ingest: tip-source poller + TipHW before any client sees the head.
	leader.EvmStatePoller().SuggestLatestBlock(tip)
	nw.NoteObservedLatestBlock(context.Background(), tip)

	require.Equal(t, tip, leader.EvmStatePoller().LatestBlock())
	require.Equal(t, "rpc2", nw.EvmLeaderUpstream(context.Background()).Id())
	require.Equal(t, tip, nw.EvmHighestLatestBlockNumber(context.Background()))

	statusCode, _, body := sendRequest(`{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params": ["latest", false]
	}`, nil, nil)

	require.Equal(t, http.StatusOK, statusCode)

	var respObject map[string]interface{}
	require.NoError(t, sonic.UnmarshalString(body, &respObject))
	result, ok := respObject["result"].(map[string]interface{})
	require.True(t, ok, "response should have a result object, got: %s", body)
	assert.Equal(t, tipHex, result["number"])
	assert.Equal(t, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", result["hash"])
	assert.GreaterOrEqual(t, leaderHits.Load(), int64(1),
		"EnforceHighestBlock must pin the tip re-fetch to EvmLeaderUpstream")
}

// Direct eth_getBlockByNumber(tip) must pin to EvmLeaderUpstream on first
// forward (same idea as EnforceHighestBlock tip re-fetch), not hit a lagging
// sibling that is preferred by selection order.
func TestHttpServer_GetBlockByNumber_NearTipPinsEvmLeaderUpstream(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	// rpc1 tip-null Persist mock is intentionally unused when the pin works.
	defer util.AssertNoPendingMocks(t, 1)

	const tip = int64(0x33338889)
	tipHex := "0x33338889"
	var leaderHits atomic.Int64
	var laggingHits atomic.Int64

	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			if r.URL.Host != "rpc1.localhost" {
				return false
			}
			body := util.SafeReadBody(r)
			if strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex) {
				laggingHits.Add(1)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))

	gock.New("http://rpc2.localhost").
		Post("").
		Filter(func(r *http.Request) bool {
			if r.URL.Host != "rpc2.localhost" {
				return false
			}
			body := util.SafeReadBody(r)
			if strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex) {
				leaderHits.Add(1)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"0x33338889","hash":"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","parentHash":"0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","timestamp":"0x6702a8f1"}}`))

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(100 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{MaxAttempts: 2},
							},
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Endpoint: "http://rpc1.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
					{
						Id:       "rpc2",
						Endpoint: "http://rpc2.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
				},
			},
		},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	prj, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	policy.OverrideAllForTest(prj.policyEngine)
	policy.OverrideOrderForTest(prj.policyEngine, "evm:123", "rpc1", "rpc2")

	time.Sleep(500 * time.Millisecond)

	nw, err := prj.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)

	var leader *upstream.Upstream
	for _, u := range nw.upstreamsRegistry.GetNetworkUpstreams(context.Background(), "evm:123") {
		if u.Id() == "rpc2" {
			leader = u
			break
		}
	}
	require.NotNil(t, leader)
	leader.EvmStatePoller().SuggestLatestBlock(tip)
	require.Equal(t, "rpc2", nw.EvmLeaderUpstream(context.Background()).Id())

	statusCode, _, body := sendRequest(`{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params": ["`+tipHex+`", false]
	}`, nil, nil)

	require.Equal(t, http.StatusOK, statusCode, "body=%s", body)
	var respObject map[string]interface{}
	require.NoError(t, sonic.UnmarshalString(body, &respObject))
	result, ok := respObject["result"].(map[string]interface{})
	require.True(t, ok, "got: %s", body)
	assert.Equal(t, tipHex, result["number"])
	assert.GreaterOrEqual(t, leaderHits.Load(), int64(1),
		"near-tip getBlock must pin to EvmLeaderUpstream")
	assert.Equal(t, int64(0), laggingHits.Load(),
		"lagging sibling must not receive the pinned near-tip getBlock")
}

// When TipHW is ahead of every upstream's concrete block response,
// EnforceHighestBlock must NOT fail-open to the stale "latest" — that is
// the MultiNode FOOS trigger once WS has already delivered the higher head.
func TestHttpServer_GetBlockByNumberLatest_RefusesStaleFailOpen(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	// Two Persist tip-null mocks remain pending by design.
	defer util.AssertNoPendingMocks(t, 2)

	const tip = int64(0x22228889)
	tipHex := "0x22228889"
	staleHex := "0x22228888"

	// Tip re-fetch always misses (null) — pinned and unconstrained paths.
	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex)
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex)
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(100 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
							Integrity: &common.EvmIntegrityConfig{
								EnforceHighestBlock: util.BoolPtr(true),
							},
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{MaxAttempts: 2},
							},
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Endpoint: "http://rpc1.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
					{
						Id:       "rpc2",
						Endpoint: "http://rpc2.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
				},
			},
		},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	prj, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	policy.OverrideAllForTest(prj.policyEngine)
	policy.OverrideOrderForTest(prj.policyEngine, "evm:123", "rpc1", "rpc2")

	time.Sleep(500 * time.Millisecond)

	nw, err := prj.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)

	var leader *upstream.Upstream
	for _, u := range nw.upstreamsRegistry.GetNetworkUpstreams(context.Background(), "evm:123") {
		if u.Id() == "rpc2" {
			leader = u
			break
		}
	}
	require.NotNil(t, leader)
	leader.EvmStatePoller().SuggestLatestBlock(tip)
	nw.NoteObservedLatestBlock(context.Background(), tip)

	statusCode, _, body := sendRequest(`{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params": ["latest", false]
	}`, nil, nil)

	var respObject map[string]interface{}
	require.NoError(t, sonic.UnmarshalString(body, &respObject))
	if result, ok := respObject["result"].(map[string]interface{}); ok {
		require.NotEqual(t, staleHex, result["number"],
			"must not fail-open to stale tip below TipHW; status=%d body=%s", statusCode, body)
		require.NotEqual(t, tipHex, result["number"],
			"tip was mocked as null; unexpected tip success: %s", body)
	}
	_, hasErr := respObject["error"]
	require.True(t, hasErr || statusCode >= 400,
		"expected error when tip re-fetch cannot reach TipHW, got status=%d body=%s", statusCode, body)
}

// TipHW tip re-fetch must refuse-stale when primaries miss the concrete tip,
// without escaping to tier:fallback pay-per-call upstreams. Goes through the
// HTTP → project doForward → HandleNetworkPostForward path (not bare
// Network.Forward, which skips TipHW enforcement).
func TestHttpServer_GetBlockByNumberLatest_TipRefetchSkipsFallbackEscape(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	// Persist tip mocks on primary + fallback remain pending by design.
	defer util.AssertNoPendingMocks(t, 2)

	const tip = int64(0x11118889)
	tipHex := "0x11118889"
	staleHex := "0x11118888"

	var fallbackHits atomic.Int64
	// Primary tip re-fetch misses.
	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex)
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))
	// Fallback would serve TipHW — must not be reached via escape on tip re-fetch.
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			if strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, tipHex) {
				fallbackHits.Add(1)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"0x11118889","hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","parentHash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","timestamp":"0x6702a8f1"}}`))

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(100 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
							Integrity: &common.EvmIntegrityConfig{
								EnforceHighestBlock: util.BoolPtr(true),
							},
						},
						// Freeze policy ticks so a lazy method-slot eval cannot
						// re-introduce the cordoned fallback mid-request.
						SelectionPolicy: &common.SelectionPolicyConfig{
							EvalInterval: 0,
						},
						Failover: &common.FailoverConfig{
							OnDefaultsExhausted: util.BoolPtr(true),
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{MaxAttempts: 2},
							},
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Endpoint: "http://rpc1.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
					{
						Id:       "rpc2",
						Endpoint: "http://rpc2.localhost",
						Type:     common.UpstreamTypeEvm,
						Tags:     []string{common.TagTierFallback},
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
				},
			},
		},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	prj, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	// Pin ordered list to primary only — mirrors preferTag cordoning
	// fallbacks while a primary is healthy. Escape would be the only way
	// to reach rpc2; SkipFallbackEscape must block that.
	policy.OverrideOrderForTest(prj.policyEngine, "evm:123", "rpc1")

	time.Sleep(500 * time.Millisecond)

	nw, err := prj.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)
	require.Equal(t, []string{"rpc1"}, nw.PolicyOrderedUpstreams("eth_getBlockByNumber"),
		"fallback must stay cordoned so only escape could reach it")
	nw.NoteObservedLatestBlock(context.Background(), tip)
	require.Equal(t, tip, nw.EvmHighestLatestBlockNumber(context.Background()))

	escapeCounter := telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues(
		"test_project", "evm:123", "eth_getBlockByNumber",
	)
	escapeBefore := promUtil.ToFloat64(escapeCounter)

	statusCode, _, body := sendRequest(`{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params": ["latest", false]
	}`, nil, nil)

	var respObject map[string]interface{}
	require.NoError(t, sonic.UnmarshalString(body, &respObject))
	if result, ok := respObject["result"].(map[string]interface{}); ok {
		require.NotEqual(t, staleHex, result["number"],
			"must not fail-open to stale tip below TipHW; status=%d body=%s", statusCode, body)
		require.NotEqual(t, tipHex, result["number"],
			"must not serve TipHW from fallback escape; body=%s", body)
	}
	_, hasErr := respObject["error"]
	require.True(t, hasErr || statusCode >= 400,
		"expected refuse-stale error when tip re-fetch misses without fallback escape; status=%d body=%s",
		statusCode, body)

	assert.Equal(t, escapeBefore, promUtil.ToFloat64(escapeCounter),
		"TipHW tip re-fetch must not fire fallback escape")
	// fallbackHits may still move from background pollers probing tip hex;
	// escape counter + refuse-stale response are the request-path proofs.
	_ = fallbackHits
}

// When the leader poller has not caught up to TipHW at first resolve (e.g.
// TipHW adopted from Redis before the local WS delivery), the SECOND tip
// re-fetch must re-resolve the leader — whose forced poll now returns the
// tip — and pin to it, instead of sweeping lagging siblings unpinned.
//
// Discriminator: rpc1 (the stale responder) must never receive the concrete
// tip fetch. Without the second-resolve pin, re-fetch #2 goes out unpinned
// and hits rpc1 first.
func TestHttpServer_GetBlockByNumberLatest_SecondRefetchResolvesLeaderPin(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()

	const tipHex = "0x3eb"     // 1003 — TipHW
	const rpc2Latest = "0x3ea" // 1002 — leader poller before catch-up
	const rpc1Latest = "0x3e8" // 1000 — lagging stale responder

	var rpc1TipHits atomic.Int64
	var rpc2TipCalls atomic.Int64
	var leaderCaughtUp atomic.Bool

	for _, host := range []string{"rpc1.localhost", "rpc2.localhost"} {
		h := host
		gock.New("http://" + h).
			Post("").
			Persist().
			Filter(func(r *http.Request) bool {
				return strings.Contains(util.SafeReadBody(r), "eth_chainId")
			}).
			Reply(200).
			JSON([]byte(`{"result":"0x7b"}`))
		gock.New("http://" + h).
			Post("").
			Persist().
			Filter(func(r *http.Request) bool {
				return strings.Contains(util.SafeReadBody(r), "eth_syncing")
			}).
			Reply(200).
			JSON([]byte(`{"result":false}`))
		gock.New("http://" + h).
			Post("").
			Persist().
			Filter(func(r *http.Request) bool {
				body := util.SafeReadBody(r)
				return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, "finalized")
			}).
			Reply(200).
			JSON([]byte(`{"result":{"number":"0x300","timestamp":"0x6702a8e0"}}`))
	}

	// rpc1: "latest" polls and the client "latest" both see a lagging head.
	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, `"latest"`)
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"` + rpc1Latest + `","hash":"0x1111111111111111111111111111111111111111111111111111111111111111","parentHash":"0x2222222222222222222222222222222222222222222222222222222222222222","timestamp":"0x6702a8f0"}}`))
	// rpc1: concrete tip fetch — must never happen when re-fetch #2 is pinned.
	gock.New("http://rpc1.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			if r.URL.Host != "rpc1.localhost" {
				return false
			}
			body := util.SafeReadBody(r)
			if strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, `"`+tipHex+`"`) {
				rpc1TipHits.Add(1)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))

	// rpc2 "latest": stale until the leader "catches up" (flips after its
	// first concrete-tip miss), then the forced poll returns the tip.
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, `"latest"`) && !leaderCaughtUp.Load()
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"` + rpc2Latest + `","hash":"0x3333333333333333333333333333333333333333333333333333333333333333","parentHash":"0x4444444444444444444444444444444444444444444444444444444444444444","timestamp":"0x6702a8f0"}}`))
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			return strings.Contains(body, "eth_getBlockByNumber") && strings.Contains(body, `"latest"`) && leaderCaughtUp.Load()
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"` + tipHex + `","hash":"0x5555555555555555555555555555555555555555555555555555555555555555","parentHash":"0x6666666666666666666666666666666666666666666666666666666666666666","timestamp":"0x6702a8f1"}}`))
	// rpc2 concrete tip: first call misses (and marks the node caught-up so
	// the next forced poll sees the tip); subsequent calls serve the block.
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			if r.URL.Host != "rpc2.localhost" {
				return false
			}
			body := util.SafeReadBody(r)
			if !strings.Contains(body, "eth_getBlockByNumber") || !strings.Contains(body, `"`+tipHex+`"`) {
				return false
			}
			if rpc2TipCalls.Add(1) == 1 {
				// The miss itself marks the node caught-up: the next forced
				// poll (second leader resolve) sees the tip.
				leaderCaughtUp.Store(true)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(`{"result":null}`))
	gock.New("http://rpc2.localhost").
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			if r.URL.Host != "rpc2.localhost" {
				return false
			}
			body := util.SafeReadBody(r)
			if !strings.Contains(body, "eth_getBlockByNumber") || !strings.Contains(body, `"`+tipHex+`"`) {
				return false
			}
			return rpc2TipCalls.Load() >= 1
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"` + tipHex + `","hash":"0x7777777777777777777777777777777777777777777777777777777777777777","parentHash":"0x8888888888888888888888888888888888888888888888888888888888888888","timestamp":"0x6702a8f1"}}`))

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(100 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
							Integrity: &common.EvmIntegrityConfig{
								EnforceHighestBlock: util.BoolPtr(true),
							},
						},
						SelectionPolicy: &common.SelectionPolicyConfig{
							EvalInterval: 0,
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{MaxAttempts: 1},
							},
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Endpoint: "http://rpc1.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
					{
						Id:       "rpc2",
						Endpoint: "http://rpc2.localhost",
						Type:     common.UpstreamTypeEvm,
						Evm: &common.EvmUpstreamConfig{
							ChainId:             123,
							StatePollerInterval: common.Duration(10 * time.Second),
						},
					},
				},
			},
		},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	prj, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	nw, err := prj.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	policy.OverrideAllForTest(prj.policyEngine)
	policy.OverrideOrderForTest(prj.policyEngine, "evm:123", "rpc1", "rpc2")
	require.Equal(t, []string{"rpc1", "rpc2"}, nw.PolicyOrderedUpstreams("eth_getBlockByNumber"),
		"rpc1 must serve the stale latest so it becomes the excluded responder")

	nw.NoteObservedLatestBlock(context.Background(), 1003)
	require.Equal(t, int64(1003), nw.EvmHighestLatestBlockNumber(context.Background()))

	statusCode, _, body := sendRequest(`{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params": ["latest", false]
	}`, nil, nil)

	require.Equal(t, http.StatusOK, statusCode, "expected tip served via leader-pinned second re-fetch; body=%s", body)
	var respObject map[string]interface{}
	require.NoError(t, sonic.UnmarshalString(body, &respObject))
	result, ok := respObject["result"].(map[string]interface{})
	require.True(t, ok, "response should have a result object, got: %s", body)
	assert.Equal(t, tipHex, result["number"], "must serve the TipHW block")
	assert.Equal(t, int64(0), rpc1TipHits.Load(),
		"second re-fetch must pin to the caught-up leader, not sweep the lagging stale responder")
	assert.GreaterOrEqual(t, rpc2TipCalls.Load(), int64(2),
		"leader must be re-tried via pin after its first tip miss")
}
