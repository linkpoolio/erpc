package erpc

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/gorilla/websocket"
	promUtil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pathSecretValue = "path-secret-value"

func pathSecretConfig(rule *common.AliasingRuleConfig) *common.Config {
	cfg := httpOnlyConfig()
	cfg.Server.Aliasing = &common.AliasingConfig{Rules: []*common.AliasingRuleConfig{rule}}
	strategies := func() *common.AuthConfig {
		return &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
			Type:   common.AuthTypeSecret,
			Secret: &common.SecretStrategyConfig{Id: "client-a", Value: pathSecretValue, AdditionalLocations: []common.SecretLocation{common.SecretLocationPath}},
		}}}
	}
	cfg.Projects[0].Auth = strategies()
	cfg.HealthCheck = &common.HealthCheckConfig{Auth: strategies()}
	return cfg
}

func doPathSecretRequest(t *testing.T, method, url string, headers map[string]string) (int, string) {
	t.Helper()
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader(forwardedGetBalance)
	}
	req, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(b)
}

// With a domain alias that preselects project, architecture and chain, the
// URL path is not used for routing and can carry the secret: https://host/<secret>.
func TestPathSecretAuth_FullyAliasedDomain(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	cfg := pathSecretConfig(&common.AliasingRuleConfig{
		MatchDomain: "*", ServeProject: "test_ws", ServeArchitecture: "evm", ServeChain: "123",
	})
	cfg.Projects[0].CORS = &common.CORSConfig{AllowedOrigins: []string{"*"}}
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	t.Run("http", func(t *testing.T) {
		status, body := doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/%s", addr, pathSecretValue), nil)
		assert.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, "0xabc123")

		status, body = doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/wrong-secret", addr), nil)
		assert.Equal(t, http.StatusUnauthorized, status, body)
		assert.NotContains(t, body, pathSecretValue)

		status, body = doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/", addr), nil)
		assert.Equal(t, http.StatusUnauthorized, status, body)
	})

	t.Run("websocket", func(t *testing.T) {
		_, resp, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/wrong-secret", addr), nil)
		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/%s", addr, pathSecretValue), nil)
		require.NoError(t, err)
		defer conn.Close()
		out := sendAndReceive(t, conn, forwardedGetBalance)
		assert.Equal(t, "0xabc123", out["result"])
	})

	t.Run("healthcheck", func(t *testing.T) {
		status, body := doPathSecretRequest(t, http.MethodGet, fmt.Sprintf("http://%s/wrong-secret", addr), nil)
		assert.Equal(t, http.StatusUnauthorized, status, body)

		status, body = doPathSecretRequest(t, http.MethodGet, fmt.Sprintf("http://%s/%s", addr, pathSecretValue), nil)
		assert.NotEqual(t, http.StatusUnauthorized, status, body)
	})

	t.Run("cors metric does not record the secret", func(t *testing.T) {
		const origin = "https://path-secret-test.example"
		status, body := doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/%s", addr, pathSecretValue), map[string]string{"Origin": origin})
		assert.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, float64(0), promUtil.ToFloat64(telemetry.MetricCORSRequestsTotal.WithLabelValues("/"+pathSecretValue, origin)))
		assert.Equal(t, float64(1), promUtil.ToFloat64(telemetry.MetricCORSRequestsTotal.WithLabelValues(common.RedactedPath, origin)))
	})
}

// When routing consumes the path (here the chain id), a single segment must
// never be read as a secret, even if it happens to equal one.
func TestPathSecretAuth_IgnoredWhenPathIsRouted(t *testing.T) {
	setupGock()
	defer util.ResetGock()

	cfg := pathSecretConfig(&common.AliasingRuleConfig{
		MatchDomain: "*", ServeProject: "test_ws", ServeArchitecture: "evm",
	})
	cfg.Projects[0].Auth.Strategies[0].Secret.Value = "123"
	addr, cleanup := setupTestERPCServer(t, cfg)
	defer cleanup()

	status, body := doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/123", addr), nil)
	assert.Equal(t, http.StatusUnauthorized, status, body)

	status, body = doPathSecretRequest(t, http.MethodPost, fmt.Sprintf("http://%s/123?secret=123", addr), nil)
	assert.Equal(t, http.StatusOK, status, body)
}
