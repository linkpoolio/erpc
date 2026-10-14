package erpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// wireRecorder captures the JSON-RPC bodies erpc sends to mocked upstreams so
// tests can assert on the exact wire shape (and that no EVM probes were sent).
type wireRecorder struct {
	mu     sync.Mutex
	bodies map[string][]string // host -> bodies
}

func newWireRecorder() *wireRecorder {
	return &wireRecorder{bodies: map[string][]string{}}
}

func (w *wireRecorder) filter(host string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		if r.URL.Host != host {
			return false
		}
		body := util.SafeReadBody(r)
		w.mu.Lock()
		w.bodies[host] = append(w.bodies[host], body)
		w.mu.Unlock()
		return true
	}
}

func (w *wireRecorder) get(host string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.bodies[host]...)
}

func jsonRpcPassthroughConfig(networks []*common.NetworkConfig, upstreams ...*common.UpstreamConfig) *common.Config {
	return &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(5 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id:        "test_project",
				Networks:  networks,
				Upstreams: upstreams,
			},
		},
		RateLimiters: &common.RateLimiterConfig{},
	}
}

func jsonRpcPassthroughUpstream(id, host string) *common.UpstreamConfig {
	return &common.UpstreamConfig{
		Id:       id,
		Type:     common.UpstreamTypeJsonRpc,
		Endpoint: "http://" + host,
		JsonRpc: &common.JsonRpcUpstreamConfig{
			NetworkId:     "bitcoin-mainnet",
			SupportsBatch: &common.FALSE,
		},
	}
}

func postJsonRpc(t *testing.T, url, body string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(respBody)
}

func TestHttpServer_JsonRpcPassthrough(t *testing.T) {
	t.Run("ForwardsArbitraryMethodUnchanged", func(t *testing.T) {
		util.ResetGock()
		defer util.ResetGock()

		rec := newWireRecorder()
		gock.New("http://rpc1.localhost").
			Post("").
			Filter(rec.filter("rpc1.localhost")).
			Reply(200).
			// JSON-RPC 1.0-style success: "error" is present and null.
			BodyString(`{"result":{"chain":"main","blocks":840000},"error":null,"id":1}`)

		cfg := jsonRpcPassthroughConfig(
			[]*common.NetworkConfig{{
				Architecture: common.ArchitectureJsonRpc,
				JsonRpc:      &common.JsonRpcNetworkConfig{Id: "bitcoin-mainnet"},
			}},
			jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost"),
		)
		_, _, baseURL, shutdown, _ := createServerTestFixtures(cfg, t)
		defer shutdown()

		status, body := postJsonRpc(t, baseURL+"/test_project/jsonrpc/bitcoin-mainnet",
			`{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":["verbose",1]}`)

		assert.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, `"blocks":840000`)
		assert.NotContains(t, body, `"error"`)

		sent := rec.get("rpc1.localhost")
		require.Len(t, sent, 1, "exactly one upstream call: no chain detection or state polling")
		assert.Contains(t, sent[0], `"method":"getblockchaininfo"`)
		assert.Contains(t, sent[0], `"params":["verbose",1]`)
	})

	t.Run("OmitsEmptyParamsAndServesByAlias", func(t *testing.T) {
		util.ResetGock()
		defer util.ResetGock()

		rec := newWireRecorder()
		gock.New("http://rpc1.localhost").
			Post("").
			Filter(rec.filter("rpc1.localhost")).
			Reply(200).
			BodyString(`{"jsonrpc":"2.0","id":1,"result":{"status":"healthy"}}`)

		cfg := jsonRpcPassthroughConfig(
			[]*common.NetworkConfig{{
				Architecture: common.ArchitectureJsonRpc,
				// No jsonRpc.id: the alias doubles as the network slug.
				Alias: "bitcoin-mainnet",
			}},
			jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost"),
		)
		_, _, baseURL, shutdown, _ := createServerTestFixtures(cfg, t)
		defer shutdown()

		status, body := postJsonRpc(t, baseURL+"/test_project/bitcoin-mainnet",
			`{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}`)

		assert.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, `"status":"healthy"`)

		sent := rec.get("rpc1.localhost")
		require.Len(t, sent, 1)
		assert.Contains(t, sent[0], `"method":"getHealth"`)
		assert.NotContains(t, sent[0], `"params"`)
	})

	t.Run("FailsOverToNextUpstream", func(t *testing.T) {
		util.ResetGock()
		defer util.ResetGock()

		rec := newWireRecorder()
		gock.New("http://rpc1.localhost").
			Post("").
			Filter(rec.filter("rpc1.localhost")).
			Persist().
			Reply(503).
			BodyString(`service unavailable`)
		gock.New("http://rpc2.localhost").
			Post("").
			Filter(rec.filter("rpc2.localhost")).
			Reply(200).
			BodyString(`{"jsonrpc":"2.0","id":1,"result":"0000000000000000000abc"}`)

		cfg := jsonRpcPassthroughConfig(
			[]*common.NetworkConfig{{
				Architecture: common.ArchitectureJsonRpc,
				JsonRpc:      &common.JsonRpcNetworkConfig{Id: "bitcoin-mainnet"},
				Failsafe: []*common.FailsafeConfig{{
					Retry: &common.RetryPolicyConfig{MaxAttempts: 3},
				}},
			}},
			jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost"),
			jsonRpcPassthroughUpstream("rpc2", "rpc2.localhost"),
		)
		_, _, baseURL, shutdown, _ := createServerTestFixtures(cfg, t)
		defer shutdown()

		status, body := postJsonRpc(t, baseURL+"/test_project/jsonrpc/bitcoin-mainnet",
			`{"jsonrpc":"2.0","id":1,"method":"getbestblockhash"}`)

		assert.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, `"0000000000000000000abc"`)
		assert.Len(t, rec.get("rpc2.localhost"), 1)
	})

	t.Run("LazilyCreatesNetworkFromUpstreams", func(t *testing.T) {
		util.ResetGock()
		defer util.ResetGock()

		rec := newWireRecorder()
		gock.New("http://rpc1.localhost").
			Post("").
			Filter(rec.filter("rpc1.localhost")).
			Reply(200).
			BodyString(`{"jsonrpc":"2.0","id":1,"result":42}`)

		// No networks configured: the network is created on first request
		// from the jsonrpc:<id> path and matched to upstreams by networkId.
		cfg := jsonRpcPassthroughConfig(nil, jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost"))
		_, _, baseURL, shutdown, _ := createServerTestFixtures(cfg, t)
		defer shutdown()

		status, body := postJsonRpc(t, baseURL+"/test_project/jsonrpc/bitcoin-mainnet",
			`{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}`)

		assert.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, `"result":42`)
		assert.Len(t, rec.get("rpc1.localhost"), 1)
	})

	t.Run("RejectsInvalidNetworkId", func(t *testing.T) {
		util.ResetGock()
		defer util.ResetGock()

		cfg := jsonRpcPassthroughConfig(nil, jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost"))
		_, _, baseURL, shutdown, _ := createServerTestFixtures(cfg, t)
		defer shutdown()

		status, body := postJsonRpc(t, baseURL+"/test_project/jsonrpc/not.valid",
			`{"jsonrpc":"2.0","id":1,"method":"getblockcount"}`)

		assert.NotEqual(t, http.StatusOK, status, body)
		assert.Contains(t, body, "invalid network id")
	})
}

// TestHealthCheckJsonRpcNetworkScope pins that the healthcheck scopes jsonrpc
// networks by architecture+id and counts their statically configured
// upstreams for the all:activeUpstreams evaluation.
func TestHealthCheckJsonRpcNetworkScope(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := &log.Logger
	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(logger, vr, nil, nil)
	require.NoError(t, err)
	ssr, err := data.NewSharedStateRegistry(ctx, logger, &common.SharedStateConfig{
		ClusterKey: "test",
		Connector: &common.ConnectorConfig{
			Driver: common.DriverMemory,
			Memory: &common.MemoryConnectorConfig{
				MaxItems: 100_000, MaxTotalSize: "1GB",
			},
		},
	})
	require.NoError(t, err)
	mtk := health.NewTracker(logger, "test", time.Second)

	ups := jsonRpcPassthroughUpstream("rpc1", "rpc1.localhost")
	pp := &PreparedProject{
		Config: &common.ProjectConfig{
			Id:        "test_project",
			Upstreams: []*common.UpstreamConfig{ups},
			Networks: []*common.NetworkConfig{
				{
					Architecture: common.ArchitectureJsonRpc,
					JsonRpc:      &common.JsonRpcNetworkConfig{Id: "bitcoin-mainnet"},
				},
			},
		},
		upstreamsRegistry: upstream.NewUpstreamsRegistry(ctx, logger, "", []*common.UpstreamConfig{ups}, ssr, nil, vr, pr, nil, mtk, nil),
	}
	pp.upstreamsRegistry.Bootstrap(ctx)
	pp.networksRegistry = NewNetworksRegistry(pp, ctx, pp.upstreamsRegistry, mtk, nil, nil, nil, nil, logger)

	s := &HttpServer{
		logger: logger,
		erpc: &ERPC{
			projectsRegistry: &ProjectsRegistry{
				preparedProjects: map[string]*PreparedProject{"test_project": pp},
			},
		},
		healthCheckCfg: &common.HealthCheckConfig{
			Mode:        common.HealthCheckModeNetworks,
			DefaultEval: common.EvalAllActiveUpstreams,
		},
		draining: &atomic.Bool{},
	}

	time.Sleep(200 * time.Millisecond)

	w := httptest.NewRecorder()
	startTime := time.Now()
	encoder := common.SonicCfg.NewEncoder(w)
	s.handleHealthCheck(
		ctx, w,
		&http.Request{Method: "GET", URL: &url.URL{Path: "/"}},
		&startTime,
		"test_project", "jsonrpc", "bitcoin-mainnet",
		encoder,
		func(ctx context.Context, statusCode int, body error) {
			w.WriteHeader(statusCode)
			_ = encoder.Encode(map[string]string{"error": body.Error()})
		},
	)

	resp := w.Result()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Contains(t, string(body), `"id":"jsonrpc:bitcoin-mainnet"`)
	assert.Contains(t, string(body), `"state":"OK"`)
}
