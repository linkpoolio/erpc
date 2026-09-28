package indexer

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/erpc/erpc/common"
)

// DedupKeyForFilter returns a stable identifier for a filter-subscription
// notification payload, or "" when the payload's identity cannot be
// established (caller should then fan out without deduping, since we can't
// prove it's a dupe).
//
//   - logs: blockHash + txHash + logIndex. All three must be present. The
//     removed flag is deliberately NOT part of the key: it is the state of
//     the log, tracked per key by DedupWindow, so add → remove → re-add
//     are each delivered while N upstreams reporting the same transition
//     collapse to one.
//   - newPendingTransactions: the tx hash, whether the upstream returned
//     a raw string or an object with a .hash field.
//
// Keys are lowercased because hex on the wire is case-insensitive; the
// payload itself is never touched.
func DedupKeyForFilter(subType string, result json.RawMessage) string {
	switch subType {
	case SubTypeLogs:
		var log struct {
			BlockHash string `json:"blockHash"`
			TxHash    string `json:"transactionHash"`
			LogIndex  string `json:"logIndex"`
		}
		if err := common.SonicCfg.Unmarshal(result, &log); err != nil {
			return ""
		}
		if log.BlockHash == "" || log.TxHash == "" || log.LogIndex == "" {
			return ""
		}
		return strings.ToLower(log.BlockHash + ":" + log.TxHash + ":" + log.LogIndex)
	case SubTypeNewPendingTransactions:
		var asString string
		if err := common.SonicCfg.Unmarshal(result, &asString); err == nil && asString != "" {
			return strings.ToLower(asString)
		}
		var asObj struct {
			Hash string `json:"hash"`
		}
		if err := common.SonicCfg.Unmarshal(result, &asObj); err == nil {
			return strings.ToLower(asObj.Hash)
		}
	}
	return ""
}

// DedupWindow is a bounded FIFO map from key to the last delivered state
// (the log's removed flag; always false for kinds without one). Keys added
// past the window's capacity evict the oldest entries. It is safe for
// concurrent use; callers typically hold one per (network, subType,
// paramsHash) fan-out group. Storage grows with use rather than being
// preallocated to capacity: most filters see far fewer keys than the cap.
type DedupWindow struct {
	size int

	mu    sync.Mutex
	state map[string]bool
	order []string
}

// NewDedupWindow returns a DedupWindow sized to hold up to `size` keys
// before the oldest entries are evicted. Pass 0 to use DefaultDedupWindowSize.
func NewDedupWindow(size int) *DedupWindow {
	if size <= 0 {
		size = DefaultDedupWindowSize
	}
	return &DedupWindow{
		size:  size,
		state: make(map[string]bool),
	}
}

// Mark records that key is now in `removed` state and returns true if the
// caller should deliver: the key is unseen, or its last delivered state
// differs. Returns false for a repeat of the last delivered state (caller
// should drop the duplicate).
func (w *DedupWindow) Mark(key string, removed bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if last, ok := w.state[key]; ok {
		if last == removed {
			return false
		}
		w.state[key] = removed
		return true
	}
	// Decoded strings may alias the notification buffer; the window
	// outlives it.
	key = strings.Clone(key)
	w.state[key] = removed
	w.order = append(w.order, key)

	if len(w.order) > w.size {
		evict := len(w.order) - w.size
		for _, old := range w.order[:evict] {
			delete(w.state, old)
		}
		w.order = w.order[evict:]
	}
	return true
}
