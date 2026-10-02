package common

import (
	"context"
	"net/http"
)

const (
	UpstreamTypeJsonRpc UpstreamType = "jsonrpc"
)

// JsonRpcNetworkConfig identifies a non-EVM JSON-RPC network (Solana, Starknet, …).
// Network id becomes jsonrpc:<id>. No chainId / state poller / EVM method hooks.
type JsonRpcNetworkConfig struct {
	// Id is a stable slug (usually the CLL alias), e.g. solana-mainnet.
	Id string `yaml:"id" json:"id"`
}

func init() {
	RegisterArchitecture(ArchitectureJsonRpc, &JsonRpcArchitectureHandler{})
}

// JsonRpcArchitectureHandler is the passthrough handler for jsonrpc networks:
// no architecture hooks. Its error extractor claims nothing, so upstream errors
// fall through to the EVM extractor in the composite (as before the registry).
type JsonRpcArchitectureHandler struct{}

func (h *JsonRpcArchitectureHandler) HandleProjectPreForward(ctx context.Context, network Network, req *NormalizedRequest) (bool, *NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleNetworkPreForward(ctx context.Context, network Network, upstreams []Upstream, req *NormalizedRequest) (bool, *NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleNetworkPostForward(ctx context.Context, network Network, req *NormalizedRequest, resp *NormalizedResponse, err error) (*NormalizedResponse, error) {
	return resp, err
}

func (h *JsonRpcArchitectureHandler) HandleUpstreamPreForward(ctx context.Context, network Network, upstream Upstream, req *NormalizedRequest, skipCacheRead bool) (bool, *NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleUpstreamPostForward(ctx context.Context, network Network, upstream Upstream, req *NormalizedRequest, resp *NormalizedResponse, err error, skipCacheRead bool) (*NormalizedResponse, error) {
	return resp, err
}

func (h *JsonRpcArchitectureHandler) NewJsonRpcErrorExtractor() JsonRpcErrorExtractor {
	return noopJsonRpcErrorExtractor{}
}

type noopJsonRpcErrorExtractor struct{}

func (noopJsonRpcErrorExtractor) Extract(*http.Response, *NormalizedResponse, *JsonRpcResponse, Upstream) error {
	return nil
}
