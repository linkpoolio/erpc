// Package indexer hosts the transport-neutral event-stream core: dedup,
// per-source subscription bookkeeping, and the canonical event shape.
// Packages in this tree
// MUST NOT import erpc, clients, or any transport-specific library — that
// invariant is what keeps ingress/egress implementations swappable.
package indexer

// Supported eth_subscribe subscription types. These are surfaced by the
// indexer's public API rather than being transport-specific (JSON-RPC
// method names live in the WS adapter), so a future Kafka / gRPC egress
// can describe events in the same vocabulary.
const (
	SubTypeNewHeads               = "newHeads"
	SubTypeLogs                   = "logs"
	SubTypeNewPendingTransactions = "newPendingTransactions"
)

// DefaultDedupWindowSize bounds the per-filter seen-set used to drop
// duplicate notifications delivered by multiple sources. The value is
// sized to cover realistic burst windows on fast chains without keeping
// an unbounded memory footprint per filter.
const DefaultDedupWindowSize = 8192
