// Package wsclient adapts a single client WebSocket connection into an
// indexer.EventEgress. One adapter per WsConnection. The adapter owns
// per-subscription write buffers and writer goroutines — the overflow
// policy lives here rather than in the indexer core because future
// egresses (Kafka, webhook) will want different policies.
package wsclient

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/erpc/erpc/indexer"
	"github.com/rs/zerolog"
)

// ErrClosed is returned by AddSubscription once the adapter was drained.
var ErrClosed = errors.New("connection closed")

// ErrLimitExceeded is returned by AddSubscription when the connection
// already holds the maximum number of subscriptions.
var ErrLimitExceeded = errors.New("subscription limit exceeded")

// NotificationWriter is what the adapter uses to push a delivered event
// onto the wire. The indirection lets the adapter stay unaware of
// gorilla/websocket specifically — any transport that can deliver a
// (clientSubId, result) pair to a single consumer works.
type NotificationWriter interface {
	WriteSubscriptionNotification(clientSubId string, result json.RawMessage) error
	// NotificationDropped is called, from the delivery path, each time a
	// full buffer costs a notification. lossy is false when the dropped
	// notification was data the client cannot re-derive (see isLossy), in
	// which case the transport should end the connection. Must not block.
	NotificationDropped(sub Subscription, lossy bool)
}

// isLossy reports whether a kind may shed its oldest queued notification
// on overflow: a newer head supersedes an older one, and pending
// transactions are best-effort by nature. Every other kind (logs, and any
// kind added later) is treated as data that must not silently vanish.
func isLossy(kind indexer.EventKind) bool {
	return kind == indexer.KindNewHead || kind == indexer.KindPendingTx
}

// Adapter is the per-connection EventEgress. Subscribe/Unsubscribe calls
// on the underlying connection translate into AddSubscription /
// RemoveSubscription here; the indexer calls InterestedIn on every event
// and Deliver for matches.
type Adapter struct {
	connID     string
	writer     NotificationWriter
	logger     *zerolog.Logger
	bufferSize int

	mu      sync.RWMutex
	drained bool
	// subs keyed by clientSubId. A single map supports both newHeads
	// and filter subs.
	subs map[string]*clientSub
	// routes is an interest-index: (kind, networkId, filterHash) -> set
	// of clientSubIds. Kept in sync with subs for O(1) InterestedIn.
	routes map[routeKey]map[string]struct{}
}

type routeKey struct {
	kind       indexer.EventKind
	networkID  string
	filterHash string
}

type clientSub struct {
	id        string
	kind      indexer.EventKind
	networkID string
	// filterHash is "" for newHeads.
	filterHash string

	notify chan json.RawMessage
	done   chan struct{}
	closed atomic.Bool
}

// New creates an adapter for a single client connection. The writer is
// invoked from the per-subscription goroutine; it must be safe for
// concurrent use (typically an internal sync.Mutex on the WS connection).
// bufferSize is the per-subscription notification queue depth.
func New(connID string, writer NotificationWriter, logger *zerolog.Logger, bufferSize int) *Adapter {
	lg := logger.With().Str("connId", connID).Logger()
	return &Adapter{
		connID:     connID,
		writer:     writer,
		logger:     &lg,
		bufferSize: bufferSize,
		subs:       make(map[string]*clientSub),
		routes:     make(map[routeKey]map[string]struct{}),
	}
}

// Name identifies the adapter in indexer registries and logs.
func (a *Adapter) Name() string { return "ws:client:" + a.connID }

// InterestedIn: O(1) map lookup. Called by the indexer on every event.
func (a *Adapter) InterestedIn(kind indexer.EventKind, networkID, filterHash string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.routes[routeKey{kind: kind, networkID: networkID, filterHash: filterHash}]
	return ok
}

// Deliver enqueues the event on every matching per-sub channel without
// blocking the indexer. See enqueue for what happens when a buffer is full.
func (a *Adapter) Deliver(ev indexer.IndexedEvent) {
	a.mu.RLock()
	routes, ok := a.routes[routeKey{kind: ev.Kind, networkID: ev.NetworkId, filterHash: ev.FilterHash}]
	if !ok || len(routes) == 0 {
		a.mu.RUnlock()
		return
	}
	ids := make([]string, 0, len(routes))
	for id := range routes {
		ids = append(ids, id)
	}
	a.mu.RUnlock()

	for _, id := range ids {
		a.mu.RLock()
		sub, ok := a.subs[id]
		a.mu.RUnlock()
		if !ok {
			continue
		}
		if !enqueue(sub, ev.Payload) {
			a.writer.NotificationDropped(sub.snapshot(), isLossy(sub.kind))
		}
	}
}

// AddSubscription registers a client subscription on this connection and
// starts its writer goroutine. clientSubId is the erpc-generated opaque
// ID the caller will return to the client. filterHash is "" for newHeads.
// It fails with ErrLimitExceeded when the connection already holds max
// subscriptions, and with ErrClosed once the adapter was drained.
func (a *Adapter) AddSubscription(clientSubID, networkID string, kind indexer.EventKind, filterHash string, max int) error {
	sub := &clientSub{
		id:         clientSubID,
		kind:       kind,
		networkID:  networkID,
		filterHash: filterHash,
		notify:     make(chan json.RawMessage, a.bufferSize),
		done:       make(chan struct{}),
	}

	a.mu.Lock()
	if a.drained {
		a.mu.Unlock()
		return ErrClosed
	}
	if len(a.subs) >= max {
		a.mu.Unlock()
		return ErrLimitExceeded
	}
	a.subs[clientSubID] = sub
	key := routeKey{kind: kind, networkID: networkID, filterHash: filterHash}
	set, ok := a.routes[key]
	if !ok {
		set = make(map[string]struct{})
		a.routes[key] = set
	}
	set[clientSubID] = struct{}{}
	a.mu.Unlock()

	go a.runWriter(sub)
	return nil
}

// RemoveSubscription deregisters a subscription and stops its writer. It
// returns the (kind, filterHash) of the removed sub so the caller can
// decide whether to call indexer.ReleaseFilter (for filter subs once the
// refcount drops to zero on its side).
func (a *Adapter) RemoveSubscription(clientSubID string) (kind indexer.EventKind, networkID, filterHash string, existed bool) {
	a.mu.Lock()
	sub, ok := a.subs[clientSubID]
	if !ok {
		a.mu.Unlock()
		return 0, "", "", false
	}
	delete(a.subs, clientSubID)
	key := routeKey{kind: sub.kind, networkID: sub.networkID, filterHash: sub.filterHash}
	if set, ok := a.routes[key]; ok {
		delete(set, clientSubID)
		if len(set) == 0 {
			delete(a.routes, key)
		}
	}
	a.mu.Unlock()

	if sub.closed.CompareAndSwap(false, true) {
		close(sub.done)
	}
	return sub.kind, sub.networkID, sub.filterHash, true
}

// Drain stops every sub's writer and makes later AddSubscription calls
// fail — call on connection close before detaching from the indexer. It
// returns the subscriptions it removed, so the caller releases exactly
// those (a concurrent RemoveSubscription owns any sub it removed first).
func (a *Adapter) Drain() []Subscription {
	a.mu.Lock()
	subs := a.subs
	a.drained = true
	a.subs = make(map[string]*clientSub)
	a.routes = make(map[routeKey]map[string]struct{})
	a.mu.Unlock()

	out := make([]Subscription, 0, len(subs))
	for _, sub := range subs {
		if sub.closed.CompareAndSwap(false, true) {
			close(sub.done)
		}
		out = append(out, sub.snapshot())
	}
	return out
}

func (sub *clientSub) snapshot() Subscription {
	return Subscription{
		ClientSubID: sub.id,
		Kind:        sub.kind,
		NetworkID:   sub.networkID,
		FilterHash:  sub.filterHash,
	}
}

// Subscription is a caller-visible snapshot of one active subscription.
type Subscription struct {
	ClientSubID string
	Kind        indexer.EventKind
	NetworkID   string
	FilterHash  string
}

// Count returns the number of active subscriptions — used by the
// client-facing manager to enforce per-connection limits.
func (a *Adapter) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.subs)
}

// --- writer goroutine -------------------------------------------------

func (a *Adapter) runWriter(sub *clientSub) {
	for {
		select {
		case <-sub.done:
			return
		case payload, ok := <-sub.notify:
			if !ok {
				return
			}
			if err := a.writer.WriteSubscriptionNotification(sub.id, payload); err != nil {
				a.logger.Debug().Err(err).Str("clientSubId", sub.id).
					Msg("failed to write subscription notification")
				// Errors are per-sub; the connection-close path will
				// Drain us when the peer is truly gone.
			}
		}
	}
}

// enqueue pushes a payload onto the sub's buffer without blocking. When the
// buffer is full a lossy sub evicts its oldest notification to make room;
// any other sub drops the new one. Returns false if a notification was
// dropped either way.
func enqueue(sub *clientSub, payload json.RawMessage) bool {
	dropped := false
	for {
		select {
		case sub.notify <- payload:
			return !dropped
		default:
		}
		if !isLossy(sub.kind) {
			return false
		}
		select {
		case <-sub.notify:
			dropped = true
		default:
		}
	}
}
