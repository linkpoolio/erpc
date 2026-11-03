package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestForwardedClientIdStrategyConfig_DefaultsFromYaml(t *testing.T) {
	var cfg AuthConfig
	require.NoError(t, yaml.Unmarshal([]byte(`
strategies:
  - type: forwardedClientId
  - forwardedClientId:
      header: X-Gateway-Consumer
      allowedIds: [client-a, client-b]
      allowedProxies: [10.0.0.0/8, 192.168.1.10]
      rateLimitBudget: gateway-tier
`), &cfg))
	require.Len(t, cfg.Strategies, 2)

	for _, s := range cfg.Strategies {
		require.NoError(t, s.SetDefaults())
		require.NoError(t, s.Validate())
		assert.Equal(t, AuthTypeForwardedClientId, s.Type, "type is inferred from the strategy block")
		require.NotNil(t, s.ForwardedClientId)
	}

	assert.Equal(t, DefaultForwardedClientIdHeader, cfg.Strategies[0].ForwardedClientId.Header)
	assert.Equal(t, "X-Gateway-Consumer", cfg.Strategies[1].ForwardedClientId.Header)
	assert.Equal(t, []string{"client-a", "client-b"}, cfg.Strategies[1].ForwardedClientId.AllowedIds)
	assert.Equal(t, "gateway-tier", cfg.Strategies[1].ForwardedClientId.RateLimitBudget)
}

func TestForwardedClientIdStrategyConfig_Validate(t *testing.T) {
	cases := map[string]struct {
		cfg     ForwardedClientIdStrategyConfig
		wantErr string
	}{
		"empty header":        {cfg: ForwardedClientIdStrategyConfig{}, wantErr: "forwardedClientId.header"},
		"header with space":   {cfg: ForwardedClientIdStrategyConfig{Header: "X Client"}, wantErr: "forwardedClientId.header"},
		"header with colon":   {cfg: ForwardedClientIdStrategyConfig{Header: "X-Client:"}, wantErr: "forwardedClientId.header"},
		"empty allowed id":    {cfg: ForwardedClientIdStrategyConfig{Header: "X-Client-Id", AllowedIds: []string{"client-a", " "}}, wantErr: "allowedIds"},
		"invalid proxy":       {cfg: ForwardedClientIdStrategyConfig{Header: "X-Client-Id", AllowedProxies: []string{"10.0.0.0/33"}}, wantErr: "allowedProxies"},
		"hostname as proxy":   {cfg: ForwardedClientIdStrategyConfig{Header: "X-Client-Id", AllowedProxies: []string{"proxy.local"}}, wantErr: "allowedProxies"},
		"valid ip and cidr":   {cfg: ForwardedClientIdStrategyConfig{Header: "X-Client-Id", AllowedProxies: []string{"10.0.0.1", "fd00::/8"}}},
		"valid default style": {cfg: ForwardedClientIdStrategyConfig{Header: "x-client-id"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestAuthStrategyConfig_Validate_ForwardedClientIdRequiresBlock(t *testing.T) {
	err := (&AuthStrategyConfig{Type: AuthTypeForwardedClientId}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth.*.forwardedClientId is required")
}
