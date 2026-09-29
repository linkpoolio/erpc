package erpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

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

}
