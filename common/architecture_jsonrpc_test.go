package common

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestJsonRpcNetworkId(t *testing.T) {
	n := &NetworkConfig{
		Architecture: ArchitectureJsonRpc,
		Alias:        "solana-mainnet",
		JsonRpc:      &JsonRpcNetworkConfig{Id: "solana-mainnet"},
	}
	if got := n.NetworkId(); got != "jsonrpc:solana-mainnet" {
		t.Fatalf("NetworkId()=%q", got)
	}
	if !IsValidArchitecture(string(ArchitectureJsonRpc)) {
		t.Fatal("ArchitectureJsonRpc should be valid")
	}
	if !IsValidNetwork("jsonrpc:solana-mainnet") {
		t.Fatal("jsonrpc:solana-mainnet should be valid")
	}
	if IsValidNetwork("jsonrpc:") || IsValidNetwork("jsonrpc:a:b") {
		t.Fatal("invalid jsonrpc ids accepted")
	}
}

func TestJsonRpcNetworkConfig_UnmarshalYAML_FailsafeObject(t *testing.T) {
	// Chart emits failsafe as a single object (not a list). NetworkConfig must
	// still accept architecture/jsonRpc via the oldNetworkConfig fallback.
	const raw = `
architecture: jsonrpc
alias: solana-mainnet
jsonRpc:
  id: solana-mainnet
failsafe:
  timeout:
    duration: 30s
  retry:
    maxAttempts: 5
    delay: 50ms
`
	var n NetworkConfig
	if err := yaml.Unmarshal([]byte(raw), &n); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if n.Architecture != ArchitectureJsonRpc {
		t.Fatalf("architecture=%q", n.Architecture)
	}
	if n.JsonRpc == nil || n.JsonRpc.Id != "solana-mainnet" {
		t.Fatalf("jsonRpc=%v", n.JsonRpc)
	}
	if n.Alias != "solana-mainnet" {
		t.Fatalf("alias=%q", n.Alias)
	}
	if len(n.Failsafe) != 1 || n.Failsafe[0].Timeout == nil {
		t.Fatalf("failsafe not converted from object: %+v", n.Failsafe)
	}
	if got := n.NetworkId(); got != "jsonrpc:solana-mainnet" {
		t.Fatalf("NetworkId()=%q", got)
	}
}
