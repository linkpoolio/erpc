package auth

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/require"
)

func TestNewPayloadFromHttp_ForwardedClientId(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Client-Id", "cl-no-alpha")
	ap, err := NewPayloadFromHttp("eth_blockNumber", "1.2.3.4:1234", headers, url.Values{}, "/")
	require.NoError(t, err)
	require.Equal(t, common.AuthTypeForwardedClientId, ap.Type)
	require.NotNil(t, ap.ForwardedClientId)
	require.Equal(t, "cl-no-alpha", ap.ForwardedClientId.Value)
}

func TestNewPayloadFromHttp_PathSecret(t *testing.T) {
	ap, err := NewPayloadFromHttp("eth_blockNumber", "1.2.3.4:1234", http.Header{}, url.Values{}, "/my-secret-key")
	require.NoError(t, err)
	require.Equal(t, common.AuthTypeSecret, ap.Type)
	require.NotNil(t, ap.Secret)
	require.Equal(t, "my-secret-key", ap.Secret.Value)
}

func TestNewPayloadFromHttp_PathSecretIgnoredForMultiSegment(t *testing.T) {
	ap, err := NewPayloadFromHttp("eth_blockNumber", "1.2.3.4:1234", http.Header{}, url.Values{}, "/main/evm/1")
	require.NoError(t, err)
	require.Equal(t, common.AuthTypeNetwork, ap.Type)
}

func TestNewPayloadFromHttp_ApiKeyQueryAndHeader(t *testing.T) {
	ap, err := NewPayloadFromHttp("eth_blockNumber", "1.2.3.4:1234", http.Header{}, url.Values{"apikey": []string{"q-key"}}, "/")
	require.NoError(t, err)
	require.Equal(t, common.AuthTypeSecret, ap.Type)
	require.Equal(t, "q-key", ap.Secret.Value)

	headers := http.Header{}
	headers.Set("apikey", "h-key")
	ap, err = NewPayloadFromHttp("eth_blockNumber", "1.2.3.4:1234", headers, url.Values{}, "/")
	require.NoError(t, err)
	require.Equal(t, common.AuthTypeSecret, ap.Type)
	require.Equal(t, "h-key", ap.Secret.Value)
}

func TestForwardedClientIdStrategy_Authenticate(t *testing.T) {
	s := NewForwardedClientIdStrategy(&common.ForwardedClientIdStrategyConfig{
		Header:          "X-Client-Id",
		RateLimitBudget: "default-budget",
	})
	ap := &AuthPayload{
		Type: common.AuthTypeForwardedClientId,
		ForwardedClientId: &ForwardedClientIdPayload{
			Value: "cl-no-beta",
		},
	}
	user, err := s.Authenticate(context.Background(), nil, ap)
	require.NoError(t, err)
	require.Equal(t, "cl-no-beta", user.Id)
	require.Equal(t, "default-budget", user.RateLimitBudget)
}

func TestForwardedClientIdStrategy_MissingHeader(t *testing.T) {
	s := NewForwardedClientIdStrategy(&common.ForwardedClientIdStrategyConfig{})
	ap := &AuthPayload{Type: common.AuthTypeForwardedClientId}
	_, err := s.Authenticate(context.Background(), nil, ap)
	require.Error(t, err)
}
