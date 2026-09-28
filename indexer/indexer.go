package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

// Options configures Indexer construction. Defaults (zero values) are
// sensible for all fields; override only for tuning.
type Options struct {
	// DedupWindowSize is the per-filter seen-set capacity. 0 = default.
	DedupWindowSize int
}

// Indexer is the transport-neutral core: ingresses push StreamEvents via
// Sink.Ingest; the indexer dedupes them, then fans out to every interested
// egress.
//
// The Indexer itself implements Sink so adapters can call indexer.Ingest
// directly — the choice is deliberate: synchronous Ingest keeps
// "update-before-dedup" ordering guarantees from the ingress goroutine
// without goroutine-per-event channel gymnastics.
type Indexer struct {
	logger *zerolog.Logger
	opts   Options

	networks sync.Map // networkId -> *networkState
	egresses sync.Map // egress Name() -> EventEgress
}

// networkState holds the indexer's per-network bookkeeping: the
// headMarker packages the most-recent head seen for a network so
// networkState can store (num, hash) atomically. Treated as immutable
// once published via lastHead.Store / CompareAndSwap; readers load
// and may see nil before the first head arrives.
type headMarker struct {
	num  int64
	hash string
}

// networkState holds a network's NetworkHandle, the ingresses feeding it,
// the newHeads dedup marker, and the dedup windows for each active filter.
type networkState struct {
	handle NetworkHandle

	ingressMu sync.RWMutex
	ingresses map[string]EventIngress // Name() -> ingress
	// selector, when non-nil, narrows per-filter fan-out to a chosen
	// subset of ingresses (defaults first, fallbacks on total failure).
	// Nil means "treat every registered ingress as a default."
	selector IngressSelector

	// newHeads dedup: most-recently-delivered (blockNumber, blockHash)
	// packed into a single pointer so the check+advance is a single
	// atomic CAS — separate atomics on num and hash would leave readers
	// able to see num updated before hash, and producers able to both
	// pass the "load, check, store" sequence with the same stale num.
	// We saw the latter in prod against evm:1101 where four upstream WS
	// sources delivered the same head within ~1ms and both raced past
	// the dedup.
	lastHead atomic.Pointer[headMarker]

	filterMu sync.RWMutex
	filters  map[string]*filterState // paramsHash -> state
}

// filterState is one filter subscription shared by every client with the
// same params.
type filterState struct {
	// mu is held across this filter's ingress calls so a subscribe and the
	// last client's teardown never interleave.
	mu         sync.Mutex
	subscribed bool // guarded by mu
	refs       int  // guarded by networkState.filterMu
	dedup      *DedupWindow
}

// New returns an empty Indexer. Networks must be registered via
// RegisterNetwork before any ingress can push events.
func New(logger *zerolog.Logger, opts Options) *Indexer {
	if opts.DedupWindowSize <= 0 {
		opts.DedupWindowSize = DefaultDedupWindowSize
	}
	return &Indexer{
		logger: logger,
		opts:   opts,
	}
}

// RegisterNetwork installs a NetworkHandle for the indexer to consult when
// routing per-source state-poller updates. Safe to call more than once for
// the same network (idempotent on handle identity).
func (i *Indexer) RegisterNetwork(nw NetworkHandle) *networkState {
	if ns, ok := i.networks.Load(nw.Id()); ok {
		return ns.(*networkState)
	}
	ns := &networkState{
		handle:    nw,
		ingresses: make(map[string]EventIngress),
		filters:   make(map[string]*filterState),
	}
	actual, _ := i.networks.LoadOrStore(nw.Id(), ns)
	return actual.(*networkState)
}

// AddIngress starts an ingress and hands it the NetworkHandle registered
// for networkId. The ingress pushes events at the indexer (which
// implements Sink). Returns an error if the network has not been
// registered yet.
func (i *Indexer) AddIngress(ctx context.Context, networkId string, ing EventIngress) error {
	nsRaw, ok := i.networks.Load(networkId)
	if !ok {
		return errNetworkNotRegistered(networkId)
	}
	ns := nsRaw.(*networkState)
	ns.ingressMu.Lock()
	ns.ingresses[ing.Name()] = ing
	ns.ingressMu.Unlock()
	return ing.Start(ctx, ns.handle, i)
}

// Attach registers an egress. The returned detach function removes the
// egress — callers that care about cleanup (client-connection closes)
// must invoke it.
func (i *Indexer) Attach(eg EventEgress) (detach func()) {
	i.egresses.Store(eg.Name(), eg)
	return func() { i.egresses.Delete(eg.Name()) }
}

// RegisterNetworkSelector installs a per-network IngressSelector that
// EnsureFilter consults when deciding which ingresses to subscribe. Passing
// nil restores the legacy "fan out to every registered ingress" behaviour.
// The network must have been registered first.
func (i *Indexer) RegisterNetworkSelector(networkId string, sel IngressSelector) {
	nsRaw, ok := i.networks.Load(networkId)
	if !ok {
		return
	}
	ns := nsRaw.(*networkState)
	ns.ingressMu.Lock()
	ns.selector = sel
	ns.ingressMu.Unlock()
}

// EnsureFilter subscribes a filter on this network's ingresses and tracks
// a per-filter refcount so ReleaseFilter can decide when to tear it down
// upstream. Concurrent callers for the same filter wait for the subscribe
// in flight; if it failed they try again themselves.
func (i *Indexer) EnsureFilter(ctx context.Context, networkId, subType string, params []interface{}) (paramsHash string, err error) {
	nsRaw, ok := i.networks.Load(networkId)
	if !ok {
		return "", errNetworkNotRegistered(networkId)
	}
	ns := nsRaw.(*networkState)
	paramsHash = BuildParamsKey(params)

	ns.filterMu.Lock()
	f := ns.filters[paramsHash]
	if f == nil {
		f = &filterState{dedup: NewDedupWindow(i.opts.DedupWindowSize)}
		ns.filters[paramsHash] = f
	}
	f.refs++
	ns.filterMu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribed {
		return paramsHash, nil
	}
	if err := i.subscribe(ctx, ns, subType, paramsHash, params); err != nil {
		// Ingresses may keep a filter whose subscribe failed (to retry on
		// reconnect), so remove it everywhere.
		i.removeFromIngresses(ctx, ns, subType, paramsHash)
		ns.filterMu.Lock()
		f.refs--
		if f.refs == 0 {
			delete(ns.filters, paramsHash)
		}
		ns.filterMu.Unlock()
		return paramsHash, err
	}
	f.subscribed = true
	return paramsHash, nil
}

// subscribe tries the selector's defaults, then its fallbacks only if every
// default failed. It fails when no ingress subscribed, including when none
// was selected.
func (i *Indexer) subscribe(ctx context.Context, ns *networkState, subType, paramsHash string, params []interface{}) error {
	networkId := ns.handle.Id()

	// Snapshot the ingress set and selector under rlock, then do the
	// potentially slow per-ingress RPC calls without holding any lock.
	ns.ingressMu.RLock()
	ings := make(map[string]EventIngress, len(ns.ingresses))
	for name, ing := range ns.ingresses {
		ings[name] = ing
	}
	sel := ns.selector
	ns.ingressMu.RUnlock()

	defaults, fallbacks := partitionIngresses(sel, ings, networkId, subType, params)

	var errs []error
	for _, tier := range [][]EventIngress{defaults, fallbacks} {
		subscribed := false
		for _, ing := range tier {
			if err := ing.EnsureFilter(ctx, subType, paramsHash, params); err != nil {
				errs = append(errs, err)
				i.logger.Warn().Err(err).Str("ingress", ing.Name()).Str("networkId", networkId).
					Str("subType", subType).Str("paramsHash", paramsHash).
					Msg("ingress EnsureFilter failed")
				continue
			}
			subscribed = true
		}
		if subscribed {
			return nil
		}
	}
	if len(errs) == 0 {
		return fmt.Errorf("indexer: no ingress selected for %s filter on network %q", subType, networkId)
	}
	return errors.Join(errs...)
}

// partitionIngresses resolves the selector's tiers to registered ingresses,
// dropping unknown and repeated names. Without a selector every ingress is
// a default.
func partitionIngresses(sel IngressSelector, ings map[string]EventIngress, networkId, subType string, params []interface{}) (defaults, fallbacks []EventIngress) {
	if sel == nil {
		defaults = make([]EventIngress, 0, len(ings))
		for _, ing := range ings {
			defaults = append(defaults, ing)
		}
		return defaults, nil
	}
	dNames, fNames := sel.Select(networkId, subType, params)
	named := make(map[string]struct{}, len(dNames)+len(fNames))
	pick := func(names []string) []EventIngress {
		out := make([]EventIngress, 0, len(names))
		for _, n := range names {
			if _, dup := named[n]; dup {
				continue
			}
			named[n] = struct{}{}
			if ing, ok := ings[n]; ok {
				out = append(out, ing)
			}
		}
		return out
	}
	return pick(dNames), pick(fNames)
}

// ReleaseFilter decrements the refcount on the filter and, when it hits
// zero, tears the subscription down on every registered ingress. Callers
// supply paramsHash (returned by EnsureFilter) rather than params.
func (i *Indexer) ReleaseFilter(ctx context.Context, networkId, subType, paramsHash string) {
	nsRaw, ok := i.networks.Load(networkId)
	if !ok {
		return
	}
	ns := nsRaw.(*networkState)

	ns.filterMu.Lock()
	f := ns.filters[paramsHash]
	if f == nil || f.refs <= 0 {
		ns.filterMu.Unlock()
		return
	}
	f.refs--
	last := f.refs == 0
	ns.filterMu.Unlock()
	if !last {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	ns.filterMu.Lock()
	// While waiting for f.mu a new client may have joined, or an earlier
	// release already tore the filter down.
	keep := f.refs > 0 || !f.subscribed
	ns.filterMu.Unlock()
	if keep {
		return
	}
	i.removeFromIngresses(ctx, ns, subType, paramsHash)
	f.subscribed = false
	ns.filterMu.Lock()
	if f.refs == 0 {
		delete(ns.filters, paramsHash)
	}
	ns.filterMu.Unlock()
}

// removeFromIngresses calls RemoveFilter on every registered ingress;
// ingresses that never received EnsureFilter for paramsHash are expected
// to no-op.
func (i *Indexer) removeFromIngresses(ctx context.Context, ns *networkState, subType, paramsHash string) {
	ns.ingressMu.RLock()
	ings := make([]EventIngress, 0, len(ns.ingresses))
	for _, ing := range ns.ingresses {
		ings = append(ings, ing)
	}
	ns.ingressMu.RUnlock()

	for _, ing := range ings {
		if err := ing.RemoveFilter(ctx, subType, paramsHash); err != nil {
			i.logger.Warn().Err(err).Str("ingress", ing.Name()).Str("networkId", ns.handle.Id()).
				Str("subType", subType).Str("paramsHash", paramsHash).
				Msg("ingress RemoveFilter failed")
		}
	}
}

// Ingest is the hot-path entry point for ingress adapters. It updates
// per-source state (via NetworkHandle.SuggestLatestBlock for headed
// events), dedupes, and fans out to every interested egress.
func (i *Indexer) Ingest(ev StreamEvent) {
	nsRaw, ok := i.networks.Load(ev.NetworkId)
	if !ok {
		return
	}
	ns := nsRaw.(*networkState)

	// State-poller update before dedup. Every observation feeds the
	// per-upstream latest-block tracker, even if the head is a dup at
	// the indexer level — otherwise a lagging source's state poller
	// stalls on the first dup.
	if ev.Kind == KindNewHead && !ev.Block.Zero() && ev.SourceId != "" {
		ns.handle.SuggestLatestBlock(ev.SourceId, ev.Block.Number, ev.Payload)
	}

	// Upstream-asserted removed flag, passed through: the indexer does not
	// second-guess which chain is canonical.
	removed := ev.Kind == KindLog && logRemoved(ev.Payload)

	if !i.dedupe(ns, &ev, removed) {
		return
	}

	i.fanOut(IndexedEvent{StreamEvent: ev, Removed: removed})
}

// dedupe returns true if the event should be delivered, false if it's a
// dupe. newHeads dedupe on the most recently delivered (number, hash);
// filter events on a bounded per-filter DedupWindow keyed by identity, with
// removed as the per-key state.
func (i *Indexer) dedupe(ns *networkState, ev *StreamEvent, removed bool) bool {
	switch ev.Kind {
	case KindNewHead:
		// CAS-retry on the packed (num, hash) pointer. On the happy path
		// exactly one goroutine per distinct head wins the swap and falls
		// through to deliver; any concurrent ingest of the same head
		// sees its CAS fail, reloads, and drops as a dupe on the next
		// iteration. Reorgs at the same height (same num, different hash)
		// win a second CAS and are delivered.
		next := &headMarker{num: ev.Block.Number, hash: ev.Block.Hash}
		for {
			prev := ns.lastHead.Load()
			if prev != nil {
				if ev.Block.Number < prev.num {
					return false
				}
				if ev.Block.Number == prev.num && prev.hash == ev.Block.Hash {
					return false
				}
			}
			if ns.lastHead.CompareAndSwap(prev, next) {
				return true
			}
		}
	case KindLog, KindPendingTx:
		ns.filterMu.RLock()
		f := ns.filters[ev.FilterHash]
		ns.filterMu.RUnlock()
		if f == nil {
			// Filter not registered with this indexer instance (e.g. an
			// ingress delivered an event for a filter we never EnsureFilter'd).
			// Allow through — upstream subs we didn't request are rare.
			return true
		}
		key := DedupKeyForFilter(ev.Kind.String(), ev.Payload)
		if key == "" {
			// Couldn't extract a key; don't pretend we deduped.
			return true
		}
		return f.dedup.Mark(key, removed)
	default:
		return true
	}
}

// fanOut dispatches to every registered egress whose InterestedIn matches.
func (i *Indexer) fanOut(ev IndexedEvent) {
	i.egresses.Range(func(_, v any) bool {
		eg := v.(EventEgress)
		if !eg.InterestedIn(ev.Kind, ev.NetworkId, ev.FilterHash) {
			return true
		}
		eg.Deliver(ev)
		return true
	})
}
