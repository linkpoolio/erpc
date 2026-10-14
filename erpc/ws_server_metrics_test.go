package erpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsRequests reads websocket_requests_total. The agent is only known once the
// request is authenticated (the Go dialer's User-Agent maps to "go").
func wsRequests(category, agent, outcome string) float64 {
	return testutil.ToFloat64(telemetry.CounterHandle(telemetry.MetricWebsocketRequestsTotal,
		"test_ws", "evm:123", category, "n/a", agent, outcome))
}

// TestWebSocket_RequestAccounting: every client request over WebSocket is
// counted once in websocket_requests_total, including those rejected before they
// reach the network, and forwarded ones carry transport="ws" on
// network_request_received_total.
func TestWebSocket_RequestAccounting(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	cfg := httpOnlyConfig()
	cfg.Projects[0].IgnoreMethods = []string{"debug_*"}
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	activeConns := func() float64 {
		return testutil.ToFloat64(telemetry.GaugeHandle(telemetry.MetricWebsocketConnectionsActive, "test_ws", "evm:123"))
	}
	closedByClient := func() float64 {
		return testutil.ToFloat64(telemetry.CounterHandle(telemetry.MetricWebsocketConnectionsClosedTotal,
			"test_ws", "evm:123", "1000", "client"))
	}
	received := func() float64 {
		return testutil.ToFloat64(telemetry.CounterHandle(telemetry.MetricNetworkRequestsReceived,
			"test_ws", "evm:123", "eth_getBalance", "realtime", "n/a", "go", "ws"))
	}

	okBefore := wsRequests("eth_getBalance", "go", "ok")
	notAllowedBefore := wsRequests("n/a", "unknown", "method_not_allowed")
	invalidBefore := wsRequests("n/a", "unknown", "invalid")
	invalidBatchBefore := wsRequests("n/a", "unknown", "invalid_batch")
	receivedBefore := received()
	activeBefore := activeConns()
	closedBefore := closedByClient()

	conn := dialWs(t, addr)
	require.Eventually(t, func() bool { return activeConns() == activeBefore+1 }, 2*time.Second, 10*time.Millisecond)

	resp := sendAndReceive(t, conn, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1234567890abcdef1234567890abcdef12345678","latest"]}`)
	assert.Equal(t, "0xabc123", resp["result"])

	resp = sendAndReceive(t, conn, `{"jsonrpc":"2.0","id":2,"method":"debug_traceTransaction","params":["0xabc"]}`)
	assert.NotNil(t, resp["error"])

	resp = sendAndReceive(t, conn, `{"jsonrpc":"2.0","id":3}`)
	assert.NotNil(t, resp["error"])

	resp = sendAndReceive(t, conn, `[{"jsonrpc":"2.0"`)
	assert.NotNil(t, resp["error"])

	assert.Equal(t, okBefore+1, wsRequests("eth_getBalance", "go", "ok"))
	assert.Equal(t, notAllowedBefore+1, wsRequests("n/a", "unknown", "method_not_allowed"))
	assert.Equal(t, invalidBefore+1, wsRequests("n/a", "unknown", "invalid"))
	assert.Equal(t, invalidBatchBefore+1, wsRequests("n/a", "unknown", "invalid_batch"))
	assert.Equal(t, receivedBefore+1, received(), "forwarded WS request must carry transport=ws")

	require.NoError(t, conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)))
	conn.Close()

	require.Eventually(t, func() bool {
		return activeConns() == activeBefore && closedByClient() == closedBefore+1
	}, 5*time.Second, 20*time.Millisecond)
}

// A request rejected by the consumer's rate limit budget is counted as
// rate_limited, with category "n/a" since it never authenticated.
func TestWebSocket_RequestAccountingAuthRateLimited(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	cfg := httpOnlyConfig()
	cfg.Projects[0].Auth = &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
		Type:            common.AuthTypeSecret,
		Secret:          &common.SecretStrategyConfig{Value: "s3cret"},
		RateLimitBudget: "one-per-minute",
	}}}
	cfg.RateLimiters = &common.RateLimiterConfig{Budgets: []*common.RateLimitBudgetConfig{{
		Id:    "one-per-minute",
		Rules: []*common.RateLimitRuleConfig{{Method: "*", MaxCount: 1, Period: common.RateLimitPeriodMinute}},
	}}}
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	limitedBefore := wsRequests("n/a", "unknown", "rate_limited")

	conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/test_ws/evm/123?secret=s3cret", addr), nil)
	require.NoError(t, err)
	defer conn.Close()

	const req = `{"jsonrpc":"2.0","id":%d,"method":"eth_getBalance","params":["0x1234567890abcdef1234567890abcdef12345678","latest"]}`
	resp := sendAndReceive(t, conn, fmt.Sprintf(req, 1))
	assert.Equal(t, "0xabc123", resp["result"])
	resp = sendAndReceive(t, conn, fmt.Sprintf(req, 2))
	assert.NotNil(t, resp["error"])

	assert.Equal(t, limitedBefore+1, wsRequests("n/a", "unknown", "rate_limited"))
}

func TestWebSocket_CloseCodeLabelIsBounded(t *testing.T) {
	assert.Equal(t, "1000", closeCodeLabel(websocket.CloseNormalClosure))
	assert.Equal(t, "1006", closeCodeLabel(websocket.CloseAbnormalClosure))
	assert.Equal(t, "1013", closeCodeLabel(websocket.CloseTryAgainLater))
	assert.Equal(t, "other", closeCodeLabel(4321))
	assert.Equal(t, "other", closeCodeLabel(0))
}
