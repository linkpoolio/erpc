package auth

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathSegmentSecret(t *testing.T) {
	cases := map[string]string{
		"":                "",
		"/":               "",
		"/my-secret-key":  "my-secret-key",
		"/my-secret-key/": "my-secret-key",
		"my-secret-key":   "my-secret-key",
		"/main/evm/1":     "",
		"/main/evm":       "",
		"/a/../b":         "b",
		"/a/..":           "",
		"/admin":          "",
		"/healthcheck":    "",
		"/metrics":        "",
		"/admin/":         "",
	}
	for in, want := range cases {
		assert.Equal(t, want, PathSegmentSecret(in), "path %q", in)
	}
}

func TestNewPayloadFromHttp_SecondarySecretLocations(t *testing.T) {
	t.Run("apikey query", func(t *testing.T) {
		ap, err := NewPayloadFromHttp("eth_blockNumber", "", http.Header{}, url.Values{"apikey": {"q-key"}}, "")
		require.NoError(t, err)
		require.NotNil(t, ap.Secret)
		assert.Equal(t, "q-key", ap.Secret.Value)
		assert.Equal(t, common.AuthTypeNetwork, ap.Type, "secondary locations must not change the payload type")
	})

	for _, name := range []string{"X-Api-Key", "x-api-key", "apikey"} {
		t.Run("header "+name, func(t *testing.T) {
			h := http.Header{}
			h.Set(name, "h-key")
			ap, err := NewPayloadFromHttp("eth_blockNumber", "", h, url.Values{}, "")
			require.NoError(t, err)
			require.NotNil(t, ap.Secret)
			assert.Equal(t, "h-key", ap.Secret.Value)
			assert.Equal(t, common.AuthTypeNetwork, ap.Type)
		})
	}

	t.Run("path segment", func(t *testing.T) {
		ap, err := NewPayloadFromHttp("eth_blockNumber", "", http.Header{}, url.Values{}, "/my-secret-key")
		require.NoError(t, err)
		require.NotNil(t, ap.Secret)
		assert.Equal(t, "my-secret-key", ap.Secret.Value)
		assert.Equal(t, common.AuthTypeNetwork, ap.Type)
	})

	t.Run("multi-segment and reserved paths are ignored", func(t *testing.T) {
		for _, p := range []string{"/main/evm/1", "/admin", "/healthcheck", "/metrics", "/"} {
			ap, err := NewPayloadFromHttp("eth_blockNumber", "", http.Header{}, url.Values{}, p)
			require.NoError(t, err)
			assert.Nil(t, ap.Secret, "path %s", p)
			assert.Equal(t, common.AuthTypeNetwork, ap.Type, "path %s", p)
		}
	})

	t.Run("precedence: primary locations win, then query, header, path", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Api-Key", "h-key")
		h.Set("X-ERPC-Secret-Token", "primary")
		ap, err := NewPayloadFromHttp("eth_blockNumber", "", h, url.Values{"apikey": {"q-key"}}, "/path-key")
		require.NoError(t, err)
		assert.Equal(t, "primary", ap.Secret.Value)
		assert.Equal(t, common.AuthTypeSecret, ap.Type)

		h.Del("X-ERPC-Secret-Token")
		ap, err = NewPayloadFromHttp("eth_blockNumber", "", h, url.Values{"apikey": {"q-key"}}, "/path-key")
		require.NoError(t, err)
		assert.Equal(t, "q-key", ap.Secret.Value)

		ap, err = NewPayloadFromHttp("eth_blockNumber", "", h, url.Values{}, "/path-key")
		require.NoError(t, err)
		assert.Equal(t, "h-key", ap.Secret.Value)
	})

	t.Run("a bearer token keeps the jwt type and the api key is still available", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer a.b.c")
		h.Set("X-Api-Key", "h-key")
		ap, err := NewPayloadFromHttp("eth_blockNumber", "", h, url.Values{}, "")
		require.NoError(t, err)
		assert.Equal(t, common.AuthTypeJwt, ap.Type)
		require.NotNil(t, ap.Jwt)
		require.NotNil(t, ap.Secret)
		assert.Equal(t, "h-key", ap.Secret.Value)
	})
}

func TestAuthRegistry_SecondarySecretLocations(t *testing.T) {
	secretOnly := newSecretAuthRegistry(t, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
		Type: common.AuthTypeSecret,
		Secret: &common.SecretStrategyConfig{Id: "backend", Value: "backend-token", AdditionalLocations: []common.SecretLocation{
			common.SecretLocationApikeyQuery, common.SecretLocationApikeyHeader, common.SecretLocationPath,
		}},
	}}})

	h := http.Header{}
	h.Set("X-Api-Key", "backend-token")
	for name, ap := range map[string]func() (*AuthPayload, error){
		"query": func() (*AuthPayload, error) {
			return NewPayloadFromHttp("m", "", http.Header{}, url.Values{"apikey": {"backend-token"}}, "")
		},
		"header": func() (*AuthPayload, error) { return NewPayloadFromHttp("m", "", h, url.Values{}, "") },
		"path": func() (*AuthPayload, error) {
			return NewPayloadFromHttp("m", "", http.Header{}, url.Values{}, "/backend-token")
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := ap()
			require.NoError(t, err)
			user, err := secretOnly.Authenticate(context.Background(), nil, "m", p)
			require.NoError(t, err)
			assert.Equal(t, "backend", user.Id)
		})
	}

	t.Run("locations are off unless listed", func(t *testing.T) {
		standardOnly := newSecretAuthRegistry(t, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
			Type:   common.AuthTypeSecret,
			Secret: &common.SecretStrategyConfig{Id: "backend", Value: "backend-token"},
		}}})
		headerOnly := newSecretAuthRegistry(t, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
			Type: common.AuthTypeSecret,
			Secret: &common.SecretStrategyConfig{Id: "backend", Value: "backend-token", AdditionalLocations: []common.SecretLocation{
				common.SecretLocationApikeyHeader,
			}},
		}}})

		p, err := NewPayloadFromHttp("m", "", h, url.Values{}, "")
		require.NoError(t, err)
		_, err = standardOnly.Authenticate(context.Background(), nil, "m", p)
		require.Error(t, err)
		user, err := headerOnly.Authenticate(context.Background(), nil, "m", p)
		require.NoError(t, err)
		assert.Equal(t, "backend", user.Id)

		p, err = NewPayloadFromHttp("m", "", http.Header{}, url.Values{}, "/backend-token")
		require.NoError(t, err)
		_, err = headerOnly.Authenticate(context.Background(), nil, "m", p)
		require.Error(t, err)

		// The standard locations keep working without additionalLocations.
		p, err = NewPayloadFromHttp("m", "", http.Header{}, url.Values{"secret": {"backend-token"}}, "")
		require.NoError(t, err)
		user, err = standardOnly.Authenticate(context.Background(), nil, "m", p)
		require.NoError(t, err)
		assert.Equal(t, "backend", user.Id)
	})

	t.Run("wrong path secret is rejected", func(t *testing.T) {
		p, err := NewPayloadFromHttp("m", "", http.Header{}, url.Values{}, "/wrong")
		require.NoError(t, err)
		_, err = secretOnly.Authenticate(context.Background(), nil, "m", p)
		require.Error(t, err)
	})

	t.Run("network strategy still serves requests that only carry a secondary secret", func(t *testing.T) {
		networkOnly := newSecretAuthRegistry(t, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{
			Type:    common.AuthTypeNetwork,
			Network: &common.NetworkStrategyConfig{AllowLocalhost: true},
		}}})
		p, err := NewPayloadFromHttp("m", "", h, url.Values{}, "")
		require.NoError(t, err)
		req := common.NewNormalizedRequest(nil)
		req.SetClientIP("127.0.0.1")
		user, err := networkOnly.Authenticate(context.Background(), req, "m", p)
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1", user.Id)
	})
}
