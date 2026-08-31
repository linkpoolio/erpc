package common

const (
	UpstreamTypeJsonRpc UpstreamType = "jsonrpc"
)

// JsonRpcNetworkConfig identifies a non-EVM JSON-RPC network (Solana, Starknet, …).
// Network id becomes jsonrpc:<id>. No chainId / state poller / EVM method hooks.
type JsonRpcNetworkConfig struct {
	// Id is a stable slug (usually the CLL alias), e.g. solana-mainnet.
	Id string `yaml:"id" json:"id"`
}
