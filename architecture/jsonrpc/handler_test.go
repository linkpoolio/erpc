package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonRpcArchitectureHandler_RegisteredAsPassthrough(t *testing.T) {
	h, err := common.GetArchitectureHandler(common.ArchitectureJsonRpc)
	require.NoError(t, err)
	require.IsType(t, &JsonRpcArchitectureHandler{}, h)

	ctx := context.Background()
	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"getblockcount"}`))

	handled, resp, err := h.HandleProjectPreForward(ctx, nil, req)
	assert.False(t, handled)
	assert.Nil(t, resp)
	assert.NoError(t, err)

	handled, resp, err = h.HandleNetworkPreForward(ctx, nil, nil, req)
	assert.False(t, handled)
	assert.Nil(t, resp)
	assert.NoError(t, err)

	handled, resp, err = h.HandleUpstreamPreForward(ctx, nil, nil, req, false)
	assert.False(t, handled)
	assert.Nil(t, resp)
	assert.NoError(t, err)

	// Post-forward hooks hand back exactly what they were given.
	in := common.NewNormalizedResponse().WithRequest(req)
	inErr := errors.New("upstream failed")
	out, outErr := h.HandleNetworkPostForward(ctx, nil, req, in, inErr)
	assert.Same(t, in, out)
	assert.Same(t, inErr, outErr)
	out, outErr = h.HandleUpstreamPostForward(ctx, nil, nil, req, in, inErr, false)
	assert.Same(t, in, out)
	assert.Same(t, inErr, outErr)

	// The extractor claims nothing, deferring to the default extractor.
	assert.NoError(t, h.NewJsonRpcErrorExtractor().Extract(nil, nil, nil, nil))
}
