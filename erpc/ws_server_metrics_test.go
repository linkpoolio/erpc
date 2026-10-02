package erpc

import (
	"testing"
	"time"

	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsRequests reads ws_requests_total. The agent is only known once the
// request is authenticated (the Go dialer's User-Agent maps to "go").
func wsRequests(category, agent, outcome string) float64 {
	return testutil.ToFloat64(telemetry.CounterHandle(telemetry.MetricWsRequestsTotal,
		"test_ws", "evm:123", category, "n/a", agent, outcome))
}

// TestWebSocket_RequestAccounting: every client request over WebSocket is
// counted once in ws_requests_total, including those rejected before they
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
		return testutil.ToFloat64(telemetry.GaugeHandle(telemetry.MetricWsConnectionsActive, "test_ws", "evm:123"))
	}
	closedByClient := func() float64 {
		return testutil.ToFloat64(telemetry.CounterHandle(telemetry.MetricWsConnectionsClosedTotal,
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
