package erpc

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/gorilla/websocket"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//
// --- Upgrade detection ---
//

// A client advertising gzip (every browser, most WS libraries) must still be
// able to upgrade under the default config, where response gzip is enabled.
func TestWebSocket_UpgradeWithAcceptEncodingGzip(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	cfg := httpOnlyConfig()
	cfg.Server.EnableGzip = util.BoolPtr(true)
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	hdr := http.Header{}
	hdr.Set("Accept-Encoding", "gzip, deflate, br")
	conn, resp, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/test_ws/evm/123", addr), hdr)
	require.NoError(t, err, "upgrade must succeed when the client accepts gzip")
	defer conn.Close()
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	res := sendAndReceive(t, conn, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xaaaa","latest"]}`)
	assert.Equal(t, "0xabc123", res["result"])
}

// A plain JSON-RPC POST that merely carries an Upgrade header is not a
// WebSocket handshake and must stay bounded by server.maxTimeout.
func TestWebSocket_UpgradeHeaderDoesNotBypassMaxTimeout(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	gock.New("http://rpc1.localhost").
		Post("/").
		Persist().
		Filter(func(r *http.Request) bool {
			return strings.Contains(util.SafeReadBody(r), "eth_getBalance")
		}).
		Reply(200).
		Delay(3 * time.Second).
		JSON(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "0x1"})

	cfg := httpOnlyConfig()
	d := common.Duration(300 * time.Millisecond)
	cfg.Server.MaxTimeout = &d
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/test_ws/evm/123", addr),
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xaaaa","latest"]}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Upgrade", "websocket")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 2*time.Second, "request must be cut off near maxTimeout, took %s (body=%s)", elapsed, body)
	assert.Contains(t, string(body), "timeout")
}

// Header tokens are case-insensitive (RFC 6455 section 4.2.1); a handshake
// spelled "Upgrade: WebSocket" must upgrade, not be answered as a
// healthcheck.
func TestWebSocket_UpgradeHeaderIsCaseInsensitive(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	addr, cleanup := setupTestERPCServer(t, httpOnlyConfig())
	defer cleanup()

	raw, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))

	_, err = fmt.Fprintf(raw, "GET /test_ws/evm/123 HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: WebSocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"+
		"Sec-WebSocket-Version: 13\r\n\r\n", addr)
	require.NoError(t, err)

	resp, err := http.ReadResponse(bufio.NewReader(raw), nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
}
