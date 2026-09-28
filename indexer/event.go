package indexer

import (
	"encoding/json"
	"time"
)

// EventKind is a tagged-union discriminant for events flowing through the
// indexer pipeline. Separate types for each kind would force sum-type
// dispatch at every stage (dedup, fan-out) — the discriminant lets every
// stage operate on a uniform value, which is worth the modest loss of
// compile-time guarantees on per-kind fields.
type EventKind uint8

const (
	KindUnknown EventKind = iota
	// KindNewHead: a canonical block header. One-shot per new block.
	KindNewHead
	// KindLog: an event log delivered by a filter subscription.
	KindLog
	// KindPendingTx: a newly-seen pending transaction hash or object.
	KindPendingTx
)

// String returns the name matching Ethereum's eth_subscribe surface (or
// "unknown").
func (k EventKind) String() string {
	switch k {
	case KindNewHead:
		return "newHeads"
	case KindLog:
		return "logs"
	case KindPendingTx:
		return "newPendingTransactions"
	default:
		return "unknown"
	}
}

// BlockRef identifies a block on a network. Zero-valued for KindPendingTx
// (pending txs don't carry a block reference until mined).
type BlockRef struct {
	Number     int64
	Hash       string
	ParentHash string
}

// Zero reports whether the BlockRef is the zero value — i.e. no block
// reference is attached (pending tx).
func (b BlockRef) Zero() bool {
	return b.Number == 0 && b.Hash == "" && b.ParentHash == ""
}

// StreamEvent is what an ingress emits. It is pre-dedup and may duplicate
// events delivered by sibling sources covering the same network. The
// Indexer converts StreamEvents → IndexedEvents.
type StreamEvent struct {
	Kind      EventKind
	NetworkId string
	// SourceId names the ingress that produced this event ("ws:<upstreamId>",
	// "kafka:<topic>", …). Used for per-source bookkeeping inside the
	// indexer (e.g. state-poller updates are per upstream) but never
	// surfaced to egresses.
	SourceId string
	Block    BlockRef
	// FilterHash identifies the filter subscription this event belongs to
	// for Kind{Log,PendingTx}. Empty for KindNewHead. Matches the hash
	// returned by BuildParamsKey when clients subscribe.
	FilterHash string
	// Payload is the upstream-provided notification result, verbatim JSON.
	// Adapters must not re-marshal — both to preserve upstream formatting
	// quirks and to let non-JSON egresses (protobuf, flatbuf) decode once
	// and cache beside the event.
	Payload json.RawMessage
	// ObservedAt is set by ingresses; the indexer does not read it.
	ObservedAt time.Time
}

// IndexedEvent is what the Indexer emits to every registered egress after
// dedup: one per observed StreamEvent, minus duplicates. The indexer never
// synthesizes events — reorged-out logs reach clients only as the
// upstream's own removed:true notifications, passed through verbatim.
type IndexedEvent struct {
	StreamEvent

	// Removed mirrors the upstream-asserted "removed" flag of a log
	// payload. Always false for other kinds.
	Removed bool
}
