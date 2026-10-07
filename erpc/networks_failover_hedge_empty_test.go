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

// getBlockByNumberMock answers eth_getBlockByNumber for blockHex on host with
// body, counting each hit.
func getBlockByNumberMock(host, blockHex, body string, hits *atomic.Int64) {
	gock.New("http://" + host).
		Post("").
		Persist().
		Filter(func(r *http.Request) bool {
			if r.URL.Host != host {
				return false
			}
			b := util.SafeReadBody(r)
			if strings.Contains(b, "eth_getBlockByNumber") && strings.Contains(b, `"`+blockHex+`"`) {
				hits.Add(1)
				return true
			}
			return false
		}).
		Reply(200).
		JSON([]byte(body))
}

const nullResultBody = `{"jsonrpc":"2.0","id":1,"result":null}`

// ethMainnetLikeFailsafe mirrors internal-erpc's eth-mainnet failsafe: retry
// on empty with a short delay, one hedge.
func ethMainnetLikeFailsafe() []*common.FailsafeConfig {
	return []*common.FailsafeConfig{{
		MatchMethod: "*",
		Retry: &common.RetryPolicyConfig{
			MaxAttempts:      5,
			Delay:            common.Duration(5 * time.Millisecond),
			EmptyResultDelay: common.Duration(5 * time.Millisecond),
		},
		Hedge: &common.HedgePolicyConfig{Delay: common.NewStaticDuration(20 * time.Millisecond), MaxCount: 1},
	}}
}

func forwardGetBlockByNumber(t *testing.T, ctx context.Context, network *Network, blockHex string) string {
	t.Helper()
	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["` + blockHex + `",false]}`))
	req.SetNetwork(network)
	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()
	jrr, err := resp.JsonRpcResponse()
	require.NoError(t, err)
	return jrr.GetResultString()
}

// Clients poll eth_getBlockByNumber for the block after the tip before it is
// produced. Every upstream answers null, so the first null is the answer:
// sweeping the rest, hedging or escaping only spends fallback (paid) quota.
func TestFailover_EmptyBeyondHeadDoesNotSweepToFallbacks(t *testing.T) {
	setup := func(t *testing.T, ctx context.Context, noPolicy bool, primaryHits, fallbackHits *atomic.Int64) *Network {
		network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
			primaryLatest:  "0x3e8", // 1000
			fallbackLatest: "0x3e8", // 1000
			enableFailover: true,
			noPolicy:       noPolicy,
			configure:      unboundedAll,
			failsafe:       ethMainnetLikeFailsafe(),
			mocks: func() {
				getBlockByNumberMock("rpc1.localhost", "0x3e9", nullResultBody, primaryHits)
				getBlockByNumberMock("rpc2.localhost", "0x3e9", nullResultBody, primaryHits)
				getBlockByNumberMock("rpc3.localhost", "0x3e9", nullResultBody, fallbackHits)
				getBlockByNumberMock("rpc4.localhost", "0x3e9", nullResultBody, fallbackHits)
			},
		})
		return network
	}
	const n = 10

	// eth-mainnet routes every tier in one ordered list, so a sweep reaches
	// the fallbacks directly.
	t.Run("AllTiersRouted", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var primaryHits, fallbackHits atomic.Int64
		network := setup(t, ctx, true, &primaryHits, &fallbackHits)
		for i := 0; i < n; i++ {
			assert.Equal(t, "null", forwardGetBlockByNumber(t, ctx, network, "0x3e9"), "iter %d", i)
		}
		assert.Zero(t, fallbackHits.Load(), "a block above every head must not reach the fallbacks")
		assert.LessOrEqual(t, primaryHits.Load(), int64(n), "one upstream answers each request")
	})

	// With the fallbacks cordoned only the escape reaches them. (The
	// policy's probeExcluded still mirrors to them in the background, so
	// upstream hits are not counted here.)
	t.Run("FallbacksCordonedByPolicy", func(t *testing.T) {
		defer util.ResetGock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var primaryHits, fallbackHits atomic.Int64
		network := setup(t, ctx, false, &primaryHits, &fallbackHits)
		escape := telemetry.MetricNetworkFallbackEscapeTotal.WithLabelValues("main", "evm:999", "eth_getBlockByNumber")
		escapeBefore := promUtil.ToFloat64(escape)
		for i := 0; i < n; i++ {
			assert.Equal(t, "null", forwardGetBlockByNumber(t, ctx, network, "0x3e9"), "iter %d", i)
		}
		assert.Equal(t, escapeBefore, promUtil.ToFloat64(escape))
		assert.LessOrEqual(t, primaryHits.Load(), int64(n), "one upstream answers each request")
	})
}

// The primaries missing a block they should have still escapes to the
// fallback that serves it.
func TestFailover_EmptyAtHeadStillEscapesToFallbacks(t *testing.T) {
	defer util.ResetGock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var primaryHits, fallbackHits atomic.Int64
	const block = `{"jsonrpc":"2.0","id":1,"result":{"number":"0x3e8","hash":"0x00000000000000000000000000000000000000000000000000000000000003e8"}}`
	network, _, _ := setupFailoverFixture(t, ctx, failoverFixtureOpts{
		primaryLatest:  "0x3e8", // 1000
		fallbackLatest: "0x3e8", // 1000
		enableFailover: true,
		configure:      unboundedAll,
		failsafe:       ethMainnetLikeFailsafe(),
		mocks: func() {
			getBlockByNumberMock("rpc1.localhost", "0x3e8", nullResultBody, &primaryHits)
			getBlockByNumberMock("rpc2.localhost", "0x3e8", nullResultBody, &primaryHits)
			getBlockByNumberMock("rpc3.localhost", "0x3e8", block, &fallbackHits)
			getBlockByNumberMock("rpc4.localhost", "0x3e8", block, &fallbackHits)
		},
	})

	assert.Contains(t, forwardGetBlockByNumber(t, ctx, network, "0x3e8"), `"number":"0x3e8"`)
	assert.Positive(t, fallbackHits.Load())
}
