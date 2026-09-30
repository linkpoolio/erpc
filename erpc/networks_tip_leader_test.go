package erpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	promUtil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unboundedPrimaries drops the availability bound from the primaries, so they
// are called (and answer) for blocks above their polled head, like a local
// node whose poller trails the head a fallback just announced.
func unboundedPrimaries(cfgs []*common.UpstreamConfig) {
	for _, cfg := range cfgs {
		if !cfg.HasTag(common.TagTierFallback) {
			cfg.Evm.BlockAvailability = nil
		}
	}
}

// countEthCalls counts eth_call requests reaching host, ahead of its standard
// mock.
func countEthCalls(host string, hits *atomic.Int64) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			// Filters run before host matching.
			if r.URL.Host == host && strings.Contains(util.SafeReadBody(r), "eth_call") {
				hits.Add(1)
			}
			return false
		}).
		Reply(200)
}

// missingOnPrimariesSlowOnFallbacks makes the primaries answer eth_call with
// missing data and the fallbacks serve it after delay.
func missingOnPrimariesSlowOnFallbacks(delay time.Duration) func() {
	return func() {
		for _, host := range []string{"rpc1.localhost", "rpc2.localhost"} {
			host := host
			gock.New("http://" + host).
				Post("").
				Persist().
				Filter(func(r *http.Request) bool {
					return r.URL.Host == host && strings.Contains(util.SafeReadBody(r), "eth_call")
				}).
				Reply(200).
				JSON([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`))
		}
		for _, host := range []string{"rpc3.localhost", "rpc4.localhost"} {
			host := host
			gock.New("http://" + host).
				Post("").
				Persist().
				Filter(func(r *http.Request) bool {
					return r.URL.Host == host && strings.Contains(util.SafeReadBody(r), "eth_call")
				}).
				Reply(200).
				Delay(delay).
				JSON([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x3333"}`))
		}
	}
}

// hedgeFaster hedges well before a fallback answers.
func hedgeFaster() []*common.FailsafeConfig {
	return []*common.FailsafeConfig{{
		MatchMethod: "*",
		Hedge:       &common.HedgePolicyConfig{Delay: common.NewStaticDuration(20 * time.Millisecond), MaxCount: 1},
	}}
}

func forwardEthCall(t *testing.T, ctx context.Context, network *Network, id int, blockHex string) (string, error) {
	t.Helper()
	req := ethCallRequest(id, blockHex)
	req.SetNetwork(network)
	resp, err := network.Forward(ctx, req)
	if err != nil {
		return "", err
	}
	require.NotNil(t, resp)
	defer resp.Release()
	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	return strings.Trim(jrr.GetResultString(), `"`), nil
}

func TestFailover_TipLeaderRouting(t *testing.T) {
	leaderCounter := func() float64 {
		return promUtil.ToFloat64(telemetry.MetricNetworkTipLeaderRouteTotal.WithLabelValues("main", "evm:999", "eth_call"))
	}
	escapeCounter := func() float64 {
		return promUtil.ToFloat64(telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues("main", "evm:999", "eth_call"))
	}

	t.Run("RoutesToFallbackThatHasTheBlock", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Primaries polled at 1000 would still answer for 1002; the fallbacks
		// already have 1002, so they go first.
		var primaryHits atomic.Int64
		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002
			enableFailover: true,
			configure:      unboundedPrimaries,
			mocks: func() {
				countEthCalls("rpc1.localhost", &primaryHits)
				countEthCalls("rpc2.localhost", &primaryHits)
			},
		})

		leaderBefore, escapeBefore := leaderCounter(), escapeCounter()
		result, err := forwardEthCall(t, ctx, network, 1, "0x3ea")
		require.NoError(t, err)
		assert.Contains(t, []string{"0x3333", "0x4444"}, result, "a fallback that has the block must serve it")
		assert.Equal(t, int64(0), primaryHits.Load(), "primaries must not be tried before the leader")
		assert.Equal(t, leaderBefore+1, leaderCounter())
		assert.Equal(t, escapeBefore, escapeCounter(), "leader routing is not an escape")
	})

	t.Run("NoLeaderWhenRoutedHasTheBlock", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3ea", // 1002
			fallbackLatest: "0x3eb", // 1003
			enableFailover: true,
		})

		leaderBefore := leaderCounter()
		for i := 0; i < 10; i++ {
			result, err := forwardEthCall(t, ctx, network, i, "0x3ea")
			require.NoError(t, err)
			assert.Contains(t, []string{"0x1111", "0x2222"}, result, "a primary that has the block must serve it (iter %d)", i)
		}
		assert.Equal(t, leaderBefore, leaderCounter())
	})

	t.Run("NoLeaderWhenFallbackIsBehindToo", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3e9", // 1001
			enableFailover: true,
			configure:      unboundedPrimaries,
		})

		leaderBefore := leaderCounter()
		result, err := forwardEthCall(t, ctx, network, 1, "0x3ea")
		require.NoError(t, err)
		assert.Contains(t, []string{"0x1111", "0x2222"}, result)
		assert.Equal(t, leaderBefore, leaderCounter())
	})

	t.Run("NoLeaderWhenFailoverDisabled", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002
			enableFailover: false,
			configure:      unboundedPrimaries,
		})

		leaderBefore := leaderCounter()
		_, _ = forwardEthCall(t, ctx, network, 1, "0x3ea")
		assert.Equal(t, leaderBefore, leaderCounter())
	})

	t.Run("FallsBackToRoutedWhenLeaderFails", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002
			enableFailover: true,
			configure:      unboundedPrimaries,
			mocks: func() {
				for _, host := range []string{"rpc3.localhost", "rpc4.localhost"} {
					host := host
					gock.New("http://" + host).
						Post("").
						Persist().
						Filter(func(r *http.Request) bool {
							return r.URL.Host == host && strings.Contains(util.SafeReadBody(r), "eth_call")
						}).
						Reply(200).
						JSON([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`))
				}
			},
		})

		leaderBefore, escapeBefore := leaderCounter(), escapeCounter()
		result, err := forwardEthCall(t, ctx, network, 1, "0x3ea")
		require.NoError(t, err)
		assert.Contains(t, []string{"0x1111", "0x2222"}, result, "routed upstreams must still be tried after the leaders fail")
		assert.Equal(t, leaderBefore+1, leaderCounter())
		assert.Equal(t, escapeBefore, escapeCounter(), "the escalation is already spent")
	})

	t.Run("HedgeDoesNotCancelSlowLeader", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// The hedge leg sweeps the primaries (missing data) while the leader
		// leg still waits on the fallback; the leader must win.
		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002
			enableFailover: true,
			configure:      unboundedPrimaries,
			failsafe:       hedgeFaster(),
			mocks:          missingOnPrimariesSlowOnFallbacks(80 * time.Millisecond),
		})

		for i := 0; i < 5; i++ {
			result, err := forwardEthCall(t, ctx, network, i, "0x3ea")
			require.NoError(t, err, "iter %d", i)
			assert.Equal(t, "0x3333", result, "iter %d", i)
		}
	})
}

// A hedge leg that finds every routed upstream missing the data must not
// cancel a sibling leg that escalated to the fallbacks and is still waiting.
func TestFailover_HedgeKeepsEscalatedSibling(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Nobody's head reaches 1002, so there is no tip leader: the primaries
	// miss, the escape sends one leg to the (slow) fallbacks, and the hedge
	// leg misses on the primaries again.
	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3e8", // 1000
		enableFailover: true,
		configure: func(cfgs []*common.UpstreamConfig) {
			for _, cfg := range cfgs {
				cfg.Evm.BlockAvailability = nil
			}
		},
		failsafe: hedgeFaster(),
		mocks:    missingOnPrimariesSlowOnFallbacks(80 * time.Millisecond),
	})

	escapeBefore := promUtil.ToFloat64(telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues("main", "evm:999", "eth_call"))
	for i := 0; i < 5; i++ {
		result, err := forwardEthCall(t, ctx, network, i, "0x3ea")
		require.NoError(t, err, "iter %d", i)
		assert.Equal(t, "0x3333", result, "iter %d", i)
	}
	assert.Equal(t, escapeBefore+5, promUtil.ToFloat64(telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues("main", "evm:999", "eth_call")))
}

// ethCallMock answers eth_call on host with body after delay.
func ethCallMock(host string, delay time.Duration, body string) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			return r.URL.Host == host && strings.Contains(util.SafeReadBody(r), "eth_call")
		}).
		Reply(200).
		Delay(delay).
		JSON([]byte(body))
}

// latestHeadMock pins host's polled latest block, ahead of its standard mock.
func latestHeadMock(host, latestHex string) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			b := util.SafeReadBody(r)
			return r.URL.Host == host && strings.Contains(b, "eth_getBlockByNumber") && strings.Contains(b, `"latest"`)
		}).
		Reply(200).
		JSON([]byte(`{"result":{"number":"` + latestHex + `","timestamp":"0x6702a8f0"}}`))
}

func unboundedAll(cfgs []*common.UpstreamConfig) {
	for _, cfg := range cfgs {
		cfg.Evm.BlockAvailability = nil
	}
}

const missingDataBody = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`

// A fallback that already answered missing data must not let a hedge leg
// cancel another fallback that is still working on the request.
func TestFailover_HedgeKeepsSlowFallbackAfterFastFallbackMiss(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3e8", // 1000, no tip leader
		enableFailover: true,
		configure:      unboundedAll,
		failsafe:       hedgeFaster(),
		mocks: func() {
			ethCallMock("rpc1.localhost", 0, missingDataBody)
			ethCallMock("rpc2.localhost", 0, missingDataBody)
			ethCallMock("rpc3.localhost", 0, missingDataBody)
			ethCallMock("rpc4.localhost", 80*time.Millisecond, `{"jsonrpc":"2.0","id":1,"result":"0x4444"}`)
		},
	})

	for i := 0; i < 5; i++ {
		result, err := forwardEthCall(t, ctx, network, i, "0x3ea")
		require.NoError(t, err, "iter %d", i)
		assert.Equal(t, "0x4444", result, "iter %d", i)
	}
}

// When the tip leader fails, the sweep still escapes to the fallbacks it has
// not tried, even one whose polled head trails the block.
func TestFailover_TipLeaderKeepsEscapeToOtherFallbacks(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3ea", // fallback-1 at 1002 (leader)
		enableFailover: true,
		configure:      unboundedAll,
		mocks: func() {
			latestHeadMock("rpc4.localhost", "0x3e8") // fallback-2 at 1000
			ethCallMock("rpc1.localhost", 0, missingDataBody)
			ethCallMock("rpc2.localhost", 0, missingDataBody)
			ethCallMock("rpc3.localhost", 0, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)
		},
	})

	leader := telemetry.MetricNetworkTipLeaderRouteTotal.WithLabelValues("main", "evm:999", "eth_call")
	escape := telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues("main", "evm:999", "eth_call")
	leaderBefore, escapeBefore := promUtil.ToFloat64(leader), promUtil.ToFloat64(escape)

	result, err := forwardEthCall(t, ctx, network, 1, "0x3ea")
	require.NoError(t, err)
	assert.Equal(t, "0x4444", result, "the untried fallback must still serve the request")
	assert.Equal(t, leaderBefore+1, promUtil.ToFloat64(leader))
	assert.Equal(t, escapeBefore+1, promUtil.ToFloat64(escape))
}

// A request pinned to an upstream by the use-upstream directive never takes
// the tip-leader route to a fallback outside it.
func TestFailover_TipLeaderRespectsUseUpstream(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3ea", // 1002
		enableFailover: true,
		configure:      unboundedPrimaries,
	})

	leader := telemetry.MetricNetworkTipLeaderRouteTotal.WithLabelValues("main", "evm:999", "eth_call")
	before := promUtil.ToFloat64(leader)

	req := ethCallRequest(1, "0x3ea")
	req.SetDirectives(&common.RequestDirectives{UseUpstream: "primary-1"})
	req.SetNetwork(network)
	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()
	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	assert.Equal(t, "0x1111", strings.Trim(jrr.GetResultString(), `"`))
	assert.Equal(t, before, promUtil.ToFloat64(leader))
}

// With served-tip on, a numbered eth_getBlockByNumber above every eligible
// head is short-circuited to null, unless a reachable fallback already has
// the block.
func TestFailover_FutureBlockShortCircuitSparesFallbackThatHasIt(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3ea", // 1002
		enableFailover: true,
		network: func(cfg *common.NetworkConfig) {
			cfg.Evm.ServedTip = &common.EvmServedTipConfig{EnabledFor: []string{"latest"}}
		},
		mocks: func() {
			for _, host := range []string{"rpc3.localhost", "rpc4.localhost"} {
				host := host
				gock.New("http://" + host).
					Post("").
					Persist().
					Filter(func(r *http.Request) bool {
						b := util.SafeReadBody(r)
						return r.URL.Host == host && strings.Contains(b, "eth_getBlockByNumber") && strings.Contains(b, `"0x3ea"`)
					}).
					Reply(200).
					JSON([]byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x3ea","hash":"0xfb","timestamp":"0x6702a8f2"}}`))
			}
		},
	})

	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x3ea",false]}`))
	req.SetNetwork(network)
	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()
	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	assert.Contains(t, jrr.GetResultString(), `"0x3ea"`, "the fallback's block must be returned, not a synthetic null")
}

// servedTipLatest turns served-tip on for the latest axis.
func servedTipLatest(cfg *common.NetworkConfig) {
	cfg.Evm.ServedTip = &common.EvmServedTipConfig{EnabledFor: []string{"latest"}}
}

func getBlockRequest(blockHex string) *common.NormalizedRequest {
	return common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["` + blockHex + `",false]}`))
}

// Consensus never reaches a fallback, so a fallback that has the block must
// not stop the future-block short-circuit for it.
func TestFailover_FutureBlockShortCircuitStillAppliesToConsensus(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3ea", // 1002
		enableFailover: true,
		network:        servedTipLatest,
		failsafe: []*common.FailsafeConfig{{
			MatchMethod: "*",
			Consensus:   &common.ConsensusPolicyConfig{MaxParticipants: 2, AgreementThreshold: 2},
		}},
	})

	req := getBlockRequest("0x3ea")
	req.SetNetwork(network)
	resp, err := network.Forward(ctx, req)
	require.NoError(t, err, "consensus must still get the truthful null")
	require.NotNil(t, resp)
	defer resp.Release()
	assert.True(t, resp.IsResultEmptyish(), "expected the short-circuit null")
}

// A fallback whose configured availability excludes the block is neither a
// tip leader nor a reason to skip the future-block short-circuit.
func TestFailover_FallbackAvailabilityBoundsExcludeLeader(t *testing.T) {
	leader := telemetry.MetricNetworkTipLeaderRouteTotal.WithLabelValues("main", "evm:999", "eth_call")
	capFallbacks := func(cfgs []*common.UpstreamConfig) {
		unboundedPrimaries(cfgs)
		for _, cfg := range cfgs {
			if cfg.HasTag(common.TagTierFallback) {
				cfg.Evm.BlockAvailability = &common.EvmBlockAvailabilityConfig{
					Upper: &common.EvmAvailabilityBoundConfig{ExactBlock: i64(1000)},
				}
			}
		}
	}

	t.Run("NotALeader", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002, but capped at 1000
			enableFailover: true,
			configure:      capFallbacks,
		})

		before := promUtil.ToFloat64(leader)
		result, err := forwardEthCall(t, ctx, network, 1, "0x3ea")
		require.NoError(t, err)
		assert.Contains(t, []string{"0x1111", "0x2222"}, result)
		assert.Equal(t, before, promUtil.ToFloat64(leader))
	})

	t.Run("ShortCircuitStillApplies", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3ea", // 1002, but capped at 1000
			enableFailover: true,
			network:        servedTipLatest,
			configure: func(cfgs []*common.UpstreamConfig) {
				capFallbacks(cfgs)
				for _, cfg := range cfgs {
					if !cfg.HasTag(common.TagTierFallback) {
						cfg.Evm.BlockAvailability = headBoundedAvailability()
					}
				}
			},
		})

		req := getBlockRequest("0x3ea")
		req.SetNetwork(network)
		resp, err := network.Forward(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer resp.Release()
		assert.True(t, resp.IsResultEmptyish(), "expected the short-circuit null")
	})
}
