package auth

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forwardedPayload(t *testing.T, remoteAddr string, headers http.Header) *AuthPayload {
	t.Helper()
	ap, err := NewPayloadFromHttp("eth_blockNumber", remoteAddr, headers, url.Values{})
	require.NoError(t, err)
	return ap
}

func newForwardedStrategy(t *testing.T, cfg *common.ForwardedClientIdStrategyConfig) *ForwardedClientIdStrategy {
	t.Helper()
	require.NoError(t, cfg.SetDefaults())
	require.NoError(t, cfg.Validate())
	s, err := NewForwardedClientIdStrategy(cfg)
	require.NoError(t, err)
	return s
}

func TestForwardedClientIdStrategy_DefaultHeader(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{RateLimitBudget: "gateway-tier"})

	h := http.Header{}
	h.Set("x-client-id", " client-a ")
	ap := forwardedPayload(t, "10.0.0.1:1234", h)

	// No credential was extracted, so the payload keeps the network type, but
	// the strategy still supports it because its header is present.
	assert.Equal(t, common.AuthTypeNetwork, ap.Type)
	require.True(t, s.Supports(ap))

	user, err := s.Authenticate(context.Background(), nil, ap)
	require.NoError(t, err)
	assert.Equal(t, "client-a", user.Id)
	assert.Equal(t, "gateway-tier", user.RateLimitBudget)
}

func TestForwardedClientIdStrategy_CustomHeader(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{Header: "X-Gateway-Consumer"})

	h := http.Header{}
	h.Set("X-Client-Id", "client-a")
	assert.False(t, s.Supports(forwardedPayload(t, "10.0.0.1:1234", h)), "default header must be ignored when another header is configured")

	h = http.Header{}
	h.Set("x-gateway-consumer", "client-b")
	ap := forwardedPayload(t, "10.0.0.1:1234", h)
	require.True(t, s.Supports(ap))
	user, err := s.Authenticate(context.Background(), nil, ap)
	require.NoError(t, err)
	assert.Equal(t, "client-b", user.Id)
}

func TestForwardedClientIdStrategy_NotSupportedWithoutHeader(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{})
	assert.False(t, s.Supports(forwardedPayload(t, "10.0.0.1:1234", http.Header{})))
	assert.False(t, s.Supports(nil))
	assert.False(t, s.Supports(&AuthPayload{Type: common.AuthTypeNetwork}), "payloads without headers (e.g. gRPC) are not supported")
}

func TestForwardedClientIdStrategy_RejectsInvalidValues(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{})

	cases := map[string][]string{
		"empty":           {""},
		"whitespace only": {"   "},
		"inner space":     {"client a"},
		"comma joined":    {"client-a,client-b"},
		"non ascii":       {"client-é"},
		"too long":        {strings.Repeat("a", maxForwardedClientIdLength+1)},
		"multiple values": {"client-a", "client-b"},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range values {
				h.Add("X-Client-Id", v)
			}
			ap := forwardedPayload(t, "10.0.0.1:1234", h)
			require.True(t, s.Supports(ap))
			user, err := s.Authenticate(context.Background(), nil, ap)
			require.Error(t, err)
			assert.Nil(t, user)
			assert.True(t, common.HasErrorCode(err, common.ErrCodeAuthUnauthorized))
		})
	}
}

func TestForwardedClientIdStrategy_AllowedIds(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{AllowedIds: []string{"client-a"}})

	h := http.Header{}
	h.Set("X-Client-Id", "client-a")
	user, err := s.Authenticate(context.Background(), nil, forwardedPayload(t, "10.0.0.1:1234", h))
	require.NoError(t, err)
	assert.Equal(t, "client-a", user.Id)

	h.Set("X-Client-Id", "client-b")
	_, err = s.Authenticate(context.Background(), nil, forwardedPayload(t, "10.0.0.1:1234", h))
	require.Error(t, err)
}

func TestForwardedClientIdStrategy_AllowedProxies(t *testing.T) {
	s := newForwardedStrategy(t, &common.ForwardedClientIdStrategyConfig{
		AllowedProxies: []string{"10.0.0.0/8", "192.168.1.10", "fd00::/8"},
	})

	h := http.Header{}
	h.Set("X-Client-Id", "client-a")

	for _, peer := range []string{"10.1.2.3:4567", "192.168.1.10:80", "[fd00::1]:443"} {
		user, err := s.Authenticate(context.Background(), nil, forwardedPayload(t, peer, h))
		require.NoError(t, err, "peer %s", peer)
		assert.Equal(t, "client-a", user.Id)
	}
	for _, peer := range []string{"203.0.113.7:4567", "192.168.1.11:80", "", "not-an-ip"} {
		_, err := s.Authenticate(context.Background(), nil, forwardedPayload(t, peer, h))
		require.Error(t, err, "peer %q must be rejected", peer)
	}
}

func TestNewForwardedClientIdStrategy_InvalidProxy(t *testing.T) {
	_, err := NewForwardedClientIdStrategy(&common.ForwardedClientIdStrategyConfig{AllowedProxies: []string{"10.0.0.0/33"}})
	require.Error(t, err)
}

func TestAuthRegistry_ForwardedClientId(t *testing.T) {
	registry := newSecretAuthRegistry(t, &common.AuthConfig{
		Strategies: []*common.AuthStrategyConfig{
			{
				Type:              common.AuthTypeForwardedClientId,
				ForwardedClientId: &common.ForwardedClientIdStrategyConfig{Header: common.DefaultForwardedClientIdHeader},
			},
			{
				Type:   common.AuthTypeSecret,
				Secret: &common.SecretStrategyConfig{Id: "backend", Value: "backend-token"},
			},
		},
	})

	t.Run("header authenticates and is attached to the request", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Client-Id", "client-a")
		req := common.NewNormalizedRequest(nil)
		user, err := registry.Authenticate(context.Background(), req, "eth_blockNumber", forwardedPayload(t, "10.0.0.1:1234", h))
		require.NoError(t, err)
		assert.Equal(t, "client-a", user.Id)
		assert.Equal(t, "client-a", req.UserId())
	})

	t.Run("other strategies still work without the header", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-ERPC-Secret-Token", "backend-token")
		user, err := registry.Authenticate(context.Background(), nil, "eth_blockNumber", forwardedPayload(t, "10.0.0.1:1234", h))
		require.NoError(t, err)
		assert.Equal(t, "backend", user.Id)
	})

	t.Run("no header and no credential is rejected", func(t *testing.T) {
		_, err := registry.Authenticate(context.Background(), nil, "eth_blockNumber", forwardedPayload(t, "10.0.0.1:1234", http.Header{}))
		require.Error(t, err)
	})
}

func TestAuthRegistry_ForwardedClientIdNotTrustedUnlessConfigured(t *testing.T) {
	registry := newSecretAuthRegistry(t, &common.AuthConfig{
		Strategies: []*common.AuthStrategyConfig{
			{
				Type:   common.AuthTypeSecret,
				Secret: &common.SecretStrategyConfig{Id: "backend", Value: "backend-token"},
			},
		},
	})
	h := http.Header{}
	h.Set("X-Client-Id", "client-a")
	_, err := registry.Authenticate(context.Background(), nil, "eth_blockNumber", forwardedPayload(t, "10.0.0.1:1234", h))
	require.Error(t, err)
}
