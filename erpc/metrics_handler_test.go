package erpc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewMetricsHandler_HealthzIsLightweight(t *testing.T) {
	h := newMetricsHandler()

	for _, path := range []string{"/healthz", "/health"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("%s: status=%d want 200", path, rr.Code)
		}
		body, _ := io.ReadAll(rr.Body)
		if string(body) != "OK" {
			t.Fatalf("%s: body=%q want OK", path, body)
		}
		ct := rr.Header().Get("Content-Type")
		if strings.Contains(ct, "openmetrics") || strings.Contains(ct, "text/plain; version=") {
			t.Fatalf("%s: got prometheus content-type %q; health must not Gather", path, ct)
		}
	}
}

func TestNewMetricsHandler_MetricsStillExposition(t *testing.T) {
	h := newMetricsHandler()

	for _, path := range []string{"/metrics", "/"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("%s: status=%d want 200", path, rr.Code)
		}
		body, _ := io.ReadAll(rr.Body)
		if !strings.Contains(string(body), "# HELP") && !strings.Contains(string(body), "# TYPE") {
			// Empty registry still usually emits process/go collectors via default registerer.
			t.Fatalf("%s: expected prometheus exposition, got %q", path, truncate(string(body), 200))
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
