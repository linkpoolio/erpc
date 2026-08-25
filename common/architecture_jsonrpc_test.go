package common

import "testing"

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
