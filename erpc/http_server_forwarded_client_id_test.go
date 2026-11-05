package erpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const forwardedGetBalance = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"]}`

func forwardedClientIdConfig(fc *common.ForwardedClientIdStrategyConfig) *common.Config {
	cfg := httpOnlyConfig()
	cfg.Projects[0].Auth = &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
		Type:              common.AuthTypeForwardedClientId,
		ForwardedClientId: fc,
	}}}
	return cfg
}

func postForwarded(t *testing.T, addr string, headers map[string]string) (int, map[string]interface{}) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/test_ws/evm/123", addr), strings.NewReader(forwardedGetBalance))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]interface{}
	require.NoError(t, json.Unmarshal(body, &out), "body: %s", body)
	return resp.StatusCode, out
}

func TestForwardedClientIdAuth_Http(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	addr, cleanup := setupTestERPCServer(t, forwardedClientIdConfig(&common.ForwardedClientIdStrategyConfig{
		AllowedProxies: []string{"127.0.0.0/8", "::1"},
		AllowedIds:     []string{"client-a"},
	}))
	defer cleanup()

	status, out := postForwarded(t, addr, map[string]string{"X-Client-Id": "client-a"})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "0xabc123", out["result"])

	status, out = postForwarded(t, addr, nil)
	assert.Equal(t, http.StatusUnauthorized, status, "a request without the header must be rejected: %v", out)

	status, out = postForwarded(t, addr, map[string]string{"X-Client-Id": "client-b"})
	assert.Equal(t, http.StatusUnauthorized, status, "an id outside allowedIds must be rejected: %v", out)
}

func TestForwardedClientIdAuth_UntrustedPeerRejected(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	// The test client connects over loopback, which is not an allowed proxy.
	addr, cleanup := setupTestERPCServer(t, forwardedClientIdConfig(&common.ForwardedClientIdStrategyConfig{
		AllowedProxies: []string{"10.0.0.0/8"},
	}))
	defer cleanup()

	status, out := postForwarded(t, addr, map[string]string{"X-Client-Id": "client-a"})
	assert.Equal(t, http.StatusUnauthorized, status, "%v", out)

	// X-Forwarded-For must not be able to impersonate the proxy: the check
	// uses the direct TCP peer.
	status, out = postForwarded(t, addr, map[string]string{"X-Client-Id": "client-a", "X-Forwarded-For": "10.1.2.3"})
	assert.Equal(t, http.StatusUnauthorized, status, "%v", out)
}

func TestForwardedClientIdAuth_WebSocket(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	addr, cleanup := setupTestERPCServer(t, forwardedClientIdConfig(&common.ForwardedClientIdStrategyConfig{
		Header: "X-Gateway-Consumer",
	}))
	defer cleanup()

	wsURL := fmt.Sprintf("ws://%s/test_ws/evm/123", addr)

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Error(t, err, "an upgrade without the header must be refused")
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"X-Gateway-Consumer": []string{"client-a"}})
	require.NoError(t, err)
	defer conn.Close()

	out := sendAndReceive(t, conn, forwardedGetBalance)
	assert.Equal(t, "0xabc123", out["result"])
}
