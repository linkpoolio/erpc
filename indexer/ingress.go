package indexer

import (
	"context"
	"encoding/json"
)

// Sink is the interface an ingress uses to push StreamEvents into the
// indexer pipeline. The indexer itself implements Sink — a pointer to the
// indexer is what adapters call on every upstream notification.
//
// Ingest is non-blocking and best-effort. Dedup and per-egress drop policy
// live downstream; an ingress that re-delivers an already-seen notification
// is expected and handled.
type Sink interface {
	Ingest(ev StreamEvent)
}

// NetworkHandle is the narrow slice of network state an ingress is
// allowed to touch. It intentionally excludes almost everything on
// *erpc.Network — the invariant is that an ingress only needs
// identification and per-source bookkeeping hooks.
type NetworkHandle interface {
	// Id returns the network identifier ("evm:<chainId>").
	Id() string
	// SuggestLatestBlock advances the per-source latest-block tracker
	// (and the network-level latest tip) before the indexer dedupes /
	// fans out. payload is the verbatim newHeads header JSON when known
	// (empty is allowed) so HTTP "latest" can serve the same tip when
	// concrete tip re-fetch misses. Preserving "update-before-dedup" and
	// "tip-before-fanout" ordering is critical.
	SuggestLatestBlock(sourceId string, blockNumber int64, payload json.RawMessage)
}

// EventIngress is an adapter that converts some transport-specific
// subscription (WS eth_subscribe, Kafka consumer, HTTP long-poll, …)
// into StreamEvents pushed at a Sink.
//
// Lifecycle expectations:
//
//   - Start is called once when the indexer takes ownership. The ingress
//     is expected to spin up its own goroutine(s) and push events at the
//     sink for as long as its transport lives.
//   - EnsureFilter / RemoveFilter are invoked when the first/last client
//     subscribes to a filter. Idempotent: repeated EnsureFilter calls for
//     the same (subType, paramsHash) are no-ops.
type EventIngress interface {
	// Name is a human-readable identifier used in logs/metrics
	// ("ws:<upstreamId>", "kafka:<topic>"). Must be stable for the life
	// of the ingress.
	Name() string
	// Start begins pumping events. Returns only once the ingress has
	// established its background workers — not once the first event
	// arrives (that would race with upstream connectivity).
	Start(ctx context.Context, nw NetworkHandle, sink Sink) error
	// EnsureFilter subscribes (on the ingress's transport) to the given
	// filter. The paramsHash is precomputed by the caller to match
	// BuildParamsKey(params). Subsequent EnsureFilter calls with the
	// same hash are no-ops.
	EnsureFilter(ctx context.Context, subType string, paramsHash string, params []interface{}) error
	// RemoveFilter unsubscribes the given filter from the transport.
	// A no-op if the filter was never subscribed. Called when the last
	// client for a filter unsubscribes, and after a failed subscribe.
	RemoveFilter(ctx context.Context, subType string, paramsHash string) error
}

// IngressSelector chooses which of a network's pooled ingresses should
// carry a filter subscription. Select returns ingress names (matching
// EventIngress.Name()) split into two tiers:
//
//   - defaults: tried first, as a group. At least one must succeed for
//     the subscription to be considered established.
//   - fallbacks: tried only if every default failed.
//
// Ingresses named in neither slice are excluded. A nil selector treats
// every registered ingress as a default.
type IngressSelector interface {
	Select(networkId, subType string, params []interface{}) (defaults, fallbacks []string)
}
