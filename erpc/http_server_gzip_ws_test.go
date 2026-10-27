package erpc

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzipHandler_SkipsWebSocketUpgrade(t *testing.T) {
	t.Parallel()

	var sawWrapped bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawWrapped = w.(*conditionalGzipWriter)
	})
	h := gzipHandler(next)

	t.Run("websocket_with_accept_encoding_gzip_not_wrapped", func(t *testing.T) {
		sawWrapped = true // fail closed if next never runs
		req := httptest.NewRequest(http.MethodGet, "/main/evm/1", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		req.Header.Set("Accept-Encoding", "gzip")
		h.ServeHTTP(httptest.NewRecorder(), req)
		require.False(t, sawWrapped, "WS upgrade must not wrap ResponseWriter (needs Hijacker)")
	})

	t.Run("http_with_accept_encoding_gzip_wrapped", func(t *testing.T) {
		sawWrapped = false
		req := httptest.NewRequest(http.MethodPost, "/main/evm/1", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		h.ServeHTTP(httptest.NewRecorder(), req)
		require.True(t, sawWrapped, "normal HTTP with gzip Accept-Encoding should wrap")
	})
}
