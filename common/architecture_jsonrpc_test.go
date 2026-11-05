package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestJsonRpcArchitecture_NetworkId(t *testing.T) {
	n := &NetworkConfig{
		Architecture: ArchitectureJsonRpc,
		JsonRpc:      &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"},
	}
	assert.Equal(t, "jsonrpc:bitcoin-mainnet", n.NetworkId())

	assert.Equal(t, "", (&NetworkConfig{Architecture: ArchitectureJsonRpc}).NetworkId(),
		"no id yet means no network id")
	assert.Equal(t, "", (&NetworkConfig{Architecture: ArchitectureJsonRpc, JsonRpc: &JsonRpcNetworkConfig{}}).NetworkId())
}

func TestJsonRpcArchitecture_IsValid(t *testing.T) {
	assert.True(t, IsValidArchitecture(string(ArchitectureJsonRpc)))

	for _, valid := range []string{"jsonrpc:bitcoin-mainnet", "jsonrpc:stellar_testnet", "jsonrpc:x1"} {
		assert.True(t, IsValidNetwork(valid), valid)
	}
	for _, invalid := range []string{"jsonrpc:", "jsonrpc:a:b", "jsonrpc:has space", "jsonrpc:a.b", "jsonrpc"} {
		assert.False(t, IsValidNetwork(invalid), invalid)
	}
}

func TestJsonRpcArchitecture_UnmarshalYAML(t *testing.T) {
	t.Run("current format", func(t *testing.T) {
		const raw = `
architecture: jsonrpc
alias: bitcoin
jsonRpc:
  id: bitcoin-mainnet
failsafe:
  - matchMethod: "*"
    timeout:
      duration: 30s
`
		var n NetworkConfig
		require.NoError(t, yaml.Unmarshal([]byte(raw), &n))
		assert.Equal(t, ArchitectureJsonRpc, n.Architecture)
		require.NotNil(t, n.JsonRpc)
		assert.Equal(t, "bitcoin-mainnet", n.JsonRpc.Id)
		assert.Equal(t, "jsonrpc:bitcoin-mainnet", n.NetworkId())
	})

	t.Run("legacy single-object failsafe keeps jsonRpc block", func(t *testing.T) {
		const raw = `
architecture: jsonrpc
jsonRpc:
  id: bitcoin-mainnet
failsafe:
  timeout:
    duration: 30s
`
		var n NetworkConfig
		require.NoError(t, yaml.Unmarshal([]byte(raw), &n))
		require.NotNil(t, n.JsonRpc)
		assert.Equal(t, "bitcoin-mainnet", n.JsonRpc.Id)
		require.Len(t, n.Failsafe, 1)
		assert.NotNil(t, n.Failsafe[0].Timeout)
	})

	t.Run("upstream jsonRpc.networkId", func(t *testing.T) {
		const raw = `
id: node-1
endpoint: https://rpc.example.com
jsonRpc:
  networkId: bitcoin-mainnet
`
		var u UpstreamConfig
		require.NoError(t, yaml.Unmarshal([]byte(raw), &u))
		require.NotNil(t, u.JsonRpc)
		assert.Equal(t, "bitcoin-mainnet", u.JsonRpc.NetworkId)
	})
}

func TestJsonRpcArchitecture_NetworkSetDefaults(t *testing.T) {
	evmDefaults := func() *NetworkDefaults {
		return &NetworkDefaults{Evm: &EvmNetworkConfig{GetLogsMaxAllowedRange: 30000}}
	}

	t.Run("architecture inferred from jsonRpc block", func(t *testing.T) {
		n := &NetworkConfig{JsonRpc: &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"}}
		require.NoError(t, n.SetDefaults(nil, nil))
		assert.Equal(t, ArchitectureJsonRpc, n.Architecture)
		assert.Equal(t, "jsonrpc:bitcoin-mainnet", n.NetworkId())
	})

	t.Run("id defaults to alias", func(t *testing.T) {
		n := &NetworkConfig{Architecture: ArchitectureJsonRpc, Alias: "stellar-mainnet"}
		require.NoError(t, n.SetDefaults(nil, nil))
		require.NotNil(t, n.JsonRpc)
		assert.Equal(t, "stellar-mainnet", n.JsonRpc.Id)
		assert.Equal(t, "jsonrpc:stellar-mainnet", n.NetworkId())
	})

	t.Run("explicit id wins over alias", func(t *testing.T) {
		n := &NetworkConfig{Architecture: ArchitectureJsonRpc, Alias: "btc", JsonRpc: &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"}}
		require.NoError(t, n.SetDefaults(nil, nil))
		assert.Equal(t, "jsonrpc:bitcoin-mainnet", n.NetworkId())
	})

	t.Run("networkDefaults.evm is not injected (explicit architecture)", func(t *testing.T) {
		n := &NetworkConfig{Architecture: ArchitectureJsonRpc, JsonRpc: &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"}}
		require.NoError(t, n.SetDefaults(nil, evmDefaults()))
		assert.Nil(t, n.Evm)
		assert.Equal(t, ArchitectureJsonRpc, n.Architecture)
	})

	t.Run("networkDefaults.evm is not injected (inferred architecture)", func(t *testing.T) {
		n := &NetworkConfig{JsonRpc: &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"}}
		require.NoError(t, n.SetDefaults(nil, evmDefaults()))
		assert.Nil(t, n.Evm, "an evm block would flip the derived architecture to evm")
		assert.Equal(t, ArchitectureJsonRpc, n.Architecture)
	})
}

func TestJsonRpcArchitecture_NetworkValidate(t *testing.T) {
	cfg := &Config{}

	n := &NetworkConfig{Architecture: ArchitectureJsonRpc}
	require.NoError(t, n.SetDefaults(nil, nil))
	assert.ErrorContains(t, n.Validate(cfg), "network.*.jsonRpc.id")

	n = &NetworkConfig{Architecture: ArchitectureJsonRpc, JsonRpc: &JsonRpcNetworkConfig{Id: "bad:id"}}
	require.NoError(t, n.SetDefaults(nil, nil))
	assert.ErrorContains(t, n.Validate(cfg), "is invalid")

	n = &NetworkConfig{Architecture: ArchitectureJsonRpc, JsonRpc: &JsonRpcNetworkConfig{Id: "bitcoin-mainnet"}}
	require.NoError(t, n.SetDefaults(nil, nil))
	assert.NoError(t, n.Validate(cfg))
}

func TestJsonRpcArchitecture_UpstreamDefaultsAndValidate(t *testing.T) {
	cfg := &Config{}

	t.Run("type inferred from jsonRpc.networkId", func(t *testing.T) {
		u := &UpstreamConfig{
			Endpoint: "https://rpc.example.com",
			JsonRpc:  &JsonRpcUpstreamConfig{NetworkId: "bitcoin-mainnet"},
		}
		require.NoError(t, u.SetDefaults(nil))
		assert.Equal(t, UpstreamTypeJsonRpc, u.Type)
		assert.NoError(t, u.Validate(cfg, false))
	})

	t.Run("jsonRpc block without networkId keeps the evm default", func(t *testing.T) {
		u := &UpstreamConfig{
			Endpoint: "https://rpc.example.com",
			JsonRpc:  &JsonRpcUpstreamConfig{SupportsBatch: &FALSE},
		}
		require.NoError(t, u.SetDefaults(nil))
		assert.Equal(t, UpstreamTypeEvm, u.Type)
	})

	t.Run("networkId is not inherited from upstreamDefaults", func(t *testing.T) {
		// A project-wide jsonRpc block is shared with EVM upstreams; inheriting
		// networkId would silently retype them as jsonrpc.
		u := &UpstreamConfig{Endpoint: "https://rpc.example.com"}
		require.NoError(t, u.SetDefaults(&UpstreamConfig{JsonRpc: &JsonRpcUpstreamConfig{NetworkId: "bitcoin-mainnet"}}))
		assert.Equal(t, UpstreamTypeEvm, u.Type)
	})

	t.Run("explicit type requires networkId", func(t *testing.T) {
		u := &UpstreamConfig{Type: UpstreamTypeJsonRpc, Endpoint: "https://rpc.example.com"}
		require.NoError(t, u.SetDefaults(nil))
		assert.ErrorContains(t, u.Validate(cfg, false), "upstream.*.jsonRpc.networkId is required")
	})

	t.Run("networkId must be an identifier", func(t *testing.T) {
		u := &UpstreamConfig{
			Type:     UpstreamTypeJsonRpc,
			Endpoint: "https://rpc.example.com",
			JsonRpc:  &JsonRpcUpstreamConfig{NetworkId: "jsonrpc:bitcoin-mainnet"},
		}
		require.NoError(t, u.SetDefaults(nil))
		assert.ErrorContains(t, u.Validate(cfg, false), "is invalid")
	})
}
