package common

const (
	// UpstreamTypeJsonRpc marks an upstream that speaks plain JSON-RPC and is
	// served through the "jsonrpc" passthrough architecture. Such upstreams
	// get no chain-specific detection, state polling or method hooks.
	UpstreamTypeJsonRpc UpstreamType = "jsonrpc"
)

// JsonRpcNetworkConfig identifies a generic JSON-RPC network served by the
// "jsonrpc" passthrough architecture. It is meant for non-EVM, non-SVM nodes
// (for example Bitcoin-family or Stellar RPC servers) where eRPC should only
// provide routing, failover, rate limiting and metrics, and forward every
// method unchanged.
//
// The resulting network id is "jsonrpc:<id>". Upstreams join the network by
// setting the same value in jsonRpc.networkId.
type JsonRpcNetworkConfig struct {
	// Id is the network slug, e.g. "bitcoin-mainnet". It may contain only
	// alphanumeric characters, dash and underscore. When omitted it defaults
	// to the network alias.
	Id string `yaml:"id,omitempty" json:"id,omitempty"`
}
