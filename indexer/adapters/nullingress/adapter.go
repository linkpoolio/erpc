// Package nullingress is a transport-free indexer.EventIngress fed by Push,
// used to drive the indexer in tests.
package nullingress

import (
	"context"
	"sync"

	"github.com/erpc/erpc/indexer"
)

// Adapter implements indexer.EventIngress as an in-memory feed.
type Adapter struct {
	name string

	mu      sync.Mutex
	started bool
	sink    indexer.Sink
	events  chan indexer.StreamEvent
	filters map[string]struct{} // subType:paramsHash
}

func New(name string) *Adapter {
	return &Adapter{
		name:    name,
		events:  make(chan indexer.StreamEvent, 64),
		filters: make(map[string]struct{}),
	}
}

func (a *Adapter) Name() string { return a.name }

// Start forwards Push()ed events to sink until ctx is done.
func (a *Adapter) Start(ctx context.Context, _ indexer.NetworkHandle, sink indexer.Sink) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return nil
	}
	a.sink = sink
	a.started = true
	go a.pump(ctx)
	return nil
}

func (a *Adapter) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-a.events:
			a.sink.Ingest(ev)
		}
	}
}

func (a *Adapter) EnsureFilter(_ context.Context, subType, paramsHash string, _ []interface{}) error {
	a.mu.Lock()
	a.filters[subType+":"+paramsHash] = struct{}{}
	a.mu.Unlock()
	return nil
}

func (a *Adapter) RemoveFilter(_ context.Context, subType, paramsHash string) error {
	a.mu.Lock()
	delete(a.filters, subType+":"+paramsHash)
	a.mu.Unlock()
	return nil
}

// Push injects an event. It blocks when the buffer is full, so a stalled
// indexer shows up as a test timeout rather than silent drops.
func (a *Adapter) Push(ev indexer.StreamEvent) {
	a.events <- ev
}

// ActiveFilters returns the subType:paramsHash keys currently subscribed.
func (a *Adapter) ActiveFilters() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.filters))
	for k := range a.filters {
		out = append(out, k)
	}
	return out
}
