package indexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthyIngress wraps fakeIngress with a controllable HealthReporter
// implementation.
type healthyIngress struct {
	fakeIngress
	healthy bool
}

func (i *healthyIngress) Healthy() bool { return i.healthy }

func TestIndexer_IngressHealth(t *testing.T) {
	idx := newIndexer(t)
	nw := newFakeNetwork("evm:324", 0)
	idx.RegisterNetwork(nw)

	t.Run("unknown network", func(t *testing.T) {
		live, total := idx.IngressHealth("evm:999")
		assert.Equal(t, 0, live)
		assert.Equal(t, 0, total)
	})

	t.Run("no ingresses yet", func(t *testing.T) {
		live, total := idx.IngressHealth("evm:324")
		assert.Equal(t, 0, live)
		assert.Equal(t, 0, total)
	})

	up := &healthyIngress{fakeIngress: fakeIngress{name: "ws:up"}, healthy: true}
	down := &healthyIngress{fakeIngress: fakeIngress{name: "ws:down"}, healthy: false}
	// An ingress that doesn't implement HealthReporter counts as live —
	// the indexer can't assess transports it doesn't understand.
	opaque := &fakeIngress{name: "kafka:topic"}

	require.NoError(t, idx.AddIngress(context.Background(), "evm:324", up))
	require.NoError(t, idx.AddIngress(context.Background(), "evm:324", down))
	require.NoError(t, idx.AddIngress(context.Background(), "evm:324", opaque))

	t.Run("mixed health", func(t *testing.T) {
		live, total := idx.IngressHealth("evm:324")
		assert.Equal(t, 2, live, "healthy reporter + opaque ingress")
		assert.Equal(t, 3, total)
	})

	t.Run("all reporters down", func(t *testing.T) {
		up.healthy = false
		live, total := idx.IngressHealth("evm:324")
		assert.Equal(t, 1, live, "only the opaque ingress remains assumed-live")
		assert.Equal(t, 3, total)
	})
}
