// Package jsonrpc implements the "jsonrpc" network architecture: a
// chain-agnostic passthrough for generic JSON-RPC upstreams.
//
// Networks of this architecture get the generic pipeline (routing, failsafe
// policies, rate limiting, selection policies, multiplexing, metrics) but no
// chain-specific behavior: no chain-id or genesis detection, no state poller,
// no request normalization and no per-method hooks. Every method is forwarded
// to the selected upstream unchanged.
package jsonrpc

import (
	"context"
	"net/http"

	"github.com/erpc/erpc/common"
)

func init() {
	common.RegisterArchitecture(common.ArchitectureJsonRpc, &JsonRpcArchitectureHandler{})
}

// JsonRpcArchitectureHandler registers the passthrough architecture with the
// generic pipeline. All hooks are no-ops.
type JsonRpcArchitectureHandler struct{}

var _ common.ArchitectureHandler = (*JsonRpcArchitectureHandler)(nil)

func (h *JsonRpcArchitectureHandler) HandleProjectPreForward(ctx context.Context, network common.Network, req *common.NormalizedRequest) (bool, *common.NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleNetworkPreForward(ctx context.Context, network common.Network, upstreams []common.Upstream, req *common.NormalizedRequest) (bool, *common.NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleNetworkPostForward(ctx context.Context, network common.Network, req *common.NormalizedRequest, resp *common.NormalizedResponse, err error) (*common.NormalizedResponse, error) {
	return resp, err
}

func (h *JsonRpcArchitectureHandler) HandleUpstreamPreForward(ctx context.Context, network common.Network, upstream common.Upstream, req *common.NormalizedRequest, skipCacheRead bool) (bool, *common.NormalizedResponse, error) {
	return false, nil, nil
}

func (h *JsonRpcArchitectureHandler) HandleUpstreamPostForward(ctx context.Context, network common.Network, upstream common.Upstream, req *common.NormalizedRequest, resp *common.NormalizedResponse, err error, skipCacheRead bool) (*common.NormalizedResponse, error) {
	return resp, err
}

// NewJsonRpcErrorExtractor returns an extractor that claims no errors. The
// composite extractor then falls through to the EVM extractor (which only
// skips svm upstreams), so jsonrpc upstreams get the default mapping of HTTP
// status codes and standard JSON-RPC error codes into eRPC's error taxonomy.
func (h *JsonRpcArchitectureHandler) NewJsonRpcErrorExtractor() common.JsonRpcErrorExtractor {
	return passthroughErrorExtractor{}
}

type passthroughErrorExtractor struct{}

func (passthroughErrorExtractor) Extract(*http.Response, *common.NormalizedResponse, *common.JsonRpcResponse, common.Upstream) error {
	return nil
}
