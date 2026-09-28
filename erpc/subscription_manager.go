package erpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/indexer"
	"github.com/erpc/erpc/indexer/adapters/wsclient"
	"github.com/erpc/erpc/indexer/adapters/wsupstream"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/upstream"
	"github.com/rs/zerolog"
)

// JSON-RPC subscription methods. Kept in erpc/ rather than the indexer
// because they are JSON-RPC-specific — a Kafka or gRPC egress deals in
// SubType only, never in RPC method strings.
const (
	MethodEthSubscribe   = "eth_subscribe"
	MethodEthUnsubscribe = "eth_unsubscribe"
)

// Subscription type aliases for convenient use in the erpc package.
// The canonical definitions live in the indexer package.
const (
	SubTypeNewHeads               = indexer.SubTypeNewHeads
	SubTypeLogs                   = indexer.SubTypeLogs
	SubTypeNewPendingTransactions = indexer.SubTypeNewPendingTransactions
)

const (
	// unsubscribeTimeout is the deadline for best-effort upstream
	// unsubscribe calls during connection cleanup.
	unsubscribeTimeout = 5 * time.Second
)

// SubscriptionManager is the client-facing egress layer. It owns
// per-connection *wsclient.Adapter instances, lazily registers networks +
// ingresses with the indexer the first time a client subscribes on a
// given network, and translates the public eth_subscribe / eth_unsubscribe
// surface into indexer calls.
type SubscriptionManager struct {
	logger *zerolog.Logger
	idx    *indexer.Indexer

	// conns maps connId -> *connEntry: one egress adapter per live WS
	// connection. connMu also guards WsConnection.subsClosed, so no
	// adapter is created for a connection once CleanupConnection ran.
	connMu sync.Mutex
	conns  map[string]*connEntry

	// networks tracks which networkIds have been bootstrapped with
	// ingresses so we don't double-register on every Subscribe call.
	networks sync.Map // networkId -> struct{}

	// bootstrapMu serialises bootstrapNetwork; the indexer's
	// RegisterNetwork is idempotent but ingress creation (WS connects on
	// upstreams) is not, so we avoid duplicate adapters.
	bootstrapMu sync.Mutex
}

// connEntry is the per-connection bookkeeping: the egress adapter and
// the indexer detach handle.
type connEntry struct {
	adapter *wsclient.Adapter
	detach  func()
}

// NewSubscriptionManager creates a client-facing SubscriptionManager
// backed by the given indexer.
func NewSubscriptionManager(logger *zerolog.Logger, idx *indexer.Indexer) *SubscriptionManager {
	return &SubscriptionManager{
		logger: logger,
		idx:    idx,
		conns:  make(map[string]*connEntry),
	}
}

// IsSubscriptionMethod returns true when the JSON-RPC method targets the
// subscription surface (eth_subscribe or eth_unsubscribe).
func IsSubscriptionMethod(method string) bool {
	return method == MethodEthSubscribe || method == MethodEthUnsubscribe
}

// IsSubscribeMethod returns true when the method is eth_subscribe.
func IsSubscribeMethod(method string) bool {
	return method == MethodEthSubscribe
}

// Subscribe handles an eth_subscribe request from a client WS connection.
// Generates a client-facing subscription ID, ensures the corresponding
// upstream subscription exists (via the indexer's EnsureFilter), and
// registers the client with the connection's egress adapter.
func (sm *SubscriptionManager) Subscribe(
	ctx context.Context,
	wsc *WsConnection,
	nq *common.NormalizedRequest,
	project *PreparedProject,
	networkId string,
) (*common.NormalizedResponse, error) {
	start := time.Now()
	method := MethodEthSubscribe
	lg := sm.logger.With().Str("connId", wsc.id).Str("networkId", networkId).Logger()

	nw, err := project.GetNetwork(ctx, networkId)
	if err != nil {
		return nil, err
	}
	nq.SetNetwork(nw)

	// Bootstrap the network if this is the first touch. Returns
	// ErrNoWsUpstreamAvailable if the network has no WS-capable
	// upstreams configured.
	if err := sm.bootstrapNetwork(ctx, nw); err != nil {
		return nil, err
	}

	conn := sm.getOrCreateConn(wsc)
	if conn == nil {
		return nil, wsclient.ErrClosed
	}
	// Fail fast before touching upstream filters; AddSubscription below
	// enforces the limit atomically.
	maxSubs := wsc.server.serverCfg.WebSocket.MaxSubscriptionsPerConnection
	if conn.adapter.Count() >= maxSubs {
		return nil, common.NewErrSubscriptionLimitExceeded(maxSubs)
	}

	if err := sm.acquireRateLimits(ctx, project, nw, nq); err != nil {
		return nil, err
	}

	reqFinality := nq.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkRequestsReceived,
		project.Config.Id, nw.Label(), method,
		reqFinality.String(), nq.UserId(), nq.AgentName(),
	).Inc()

	jrReq, err := nq.JsonRpcRequest()
	if err != nil {
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}

	subType := indexer.ExtractSubscriptionType(jrReq.Params)
	clientSubID, err := indexer.GenerateClientSubID()
	if err != nil {
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, fmt.Errorf("failed to generate subscription ID: %w", err))
		return nil, fmt.Errorf("failed to generate subscription ID: %w", err)
	}

	kind, filterHash, err := sm.resolveSubscription(ctx, networkId, subType, jrReq.Params)
	if err != nil {
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}

	if err := conn.adapter.AddSubscription(clientSubID, networkId, kind, filterHash, maxSubs); err != nil {
		sm.releaseFilter(ctx, networkId, kind, filterHash)
		if errors.Is(err, wsclient.ErrLimitExceeded) {
			err = common.NewErrSubscriptionLimitExceeded(maxSubs)
		}
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}

	lg.Info().
		Str("clientSubId", clientSubID).
		Str("subType", subType).
		Msg("subscription established")

	telemetry.CounterHandle(telemetry.MetricNetworkSuccessfulRequests,
		project.Config.Id, nw.Label(), "proxy", "proxy",
		method, "1", reqFinality.String(), "false", nq.UserId(), nq.AgentName(),
	).Inc()
	telemetry.ObserverHandle(telemetry.MetricNetworkRequestDuration,
		project.Config.Id, nw.Label(), "proxy", "proxy",
		method, reqFinality.String(), nq.UserId(),
	).Observe(time.Since(start).Seconds())

	return sm.buildSubscribeResponse(nq, jrReq, clientSubID), nil
}

// Unsubscribe handles an eth_unsubscribe request.
func (sm *SubscriptionManager) Unsubscribe(
	ctx context.Context,
	wsc *WsConnection,
	nq *common.NormalizedRequest,
	project *PreparedProject,
	networkId string,
) (*common.NormalizedResponse, error) {
	start := time.Now()
	method := MethodEthUnsubscribe
	lg := sm.logger.With().Str("connId", wsc.id).Str("networkId", networkId).Logger()

	nw, err := project.GetNetwork(ctx, networkId)
	if err != nil {
		return nil, err
	}
	nq.SetNetwork(nw)

	if err := sm.acquireRateLimits(ctx, project, nw, nq); err != nil {
		return nil, err
	}

	reqFinality := nq.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkRequestsReceived,
		project.Config.Id, nw.Label(), method,
		reqFinality.String(), nq.UserId(), nq.AgentName(),
	).Inc()

	jrReq, err := nq.JsonRpcRequest()
	if err != nil {
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}

	clientSubID, err := indexer.ExtractClientSubID(jrReq.Params)
	if err != nil {
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}

	// Only the connection that created a subscription can remove it, and
	// only the caller that actually removed it releases its filter.
	sm.connMu.Lock()
	conn := sm.conns[wsc.id]
	sm.connMu.Unlock()
	var kind indexer.EventKind
	var subNetworkID, filterHash string
	existed := false
	if conn != nil {
		kind, subNetworkID, filterHash, existed = conn.adapter.RemoveSubscription(clientSubID)
	}
	if !existed {
		err := common.NewErrSubscriptionNotFound(clientSubID)
		sm.recordFailureMetrics(project, nw, method, reqFinality, start, nq, err)
		return nil, err
	}
	sm.releaseFilter(ctx, subNetworkID, kind, filterHash)

	lg.Info().Str("clientSubId", clientSubID).Str("subType", kind.String()).Msg("subscription removed")

	telemetry.CounterHandle(telemetry.MetricNetworkSuccessfulRequests,
		project.Config.Id, nw.Label(), "proxy", "proxy",
		method, "1", reqFinality.String(), "false", nq.UserId(), nq.AgentName(),
	).Inc()
	telemetry.ObserverHandle(telemetry.MetricNetworkRequestDuration,
		project.Config.Id, nw.Label(), "proxy", "proxy",
		method, reqFinality.String(), nq.UserId(),
	).Observe(time.Since(start).Seconds())

	resp := common.NewNormalizedResponse().WithRequest(nq)
	jrr := &common.JsonRpcResponse{}
	_ = jrr.SetID(jrReq.ID)
	jrr.SetResult([]byte("true"))
	resp.WithJsonRpcResponse(jrr)
	return resp, nil
}

// CleanupConnection is invoked on WS disconnect. It drains the adapter,
// releases the filter refcount of every subscription it removed, and
// detaches it from the indexer's egress set. Later Subscribe calls on the
// same connection fail.
func (sm *SubscriptionManager) CleanupConnection(wsc *WsConnection) {
	sm.connMu.Lock()
	wsc.subsClosed = true
	conn := sm.conns[wsc.id]
	delete(sm.conns, wsc.id)
	sm.connMu.Unlock()
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), unsubscribeTimeout)
	defer cancel()
	for _, sub := range conn.adapter.Drain() {
		sm.releaseFilter(ctx, sub.NetworkID, sub.Kind, sub.FilterHash)
	}
	conn.detach()

	sm.logger.Debug().Str("connId", wsc.id).Msg("cleaned up all subscriptions for connection")
}

// releaseFilter drops one reference on a filter subscription; newHeads
// subscriptions hold none.
func (sm *SubscriptionManager) releaseFilter(ctx context.Context, networkID string, kind indexer.EventKind, filterHash string) {
	if kind != indexer.KindNewHead && filterHash != "" {
		sm.idx.ReleaseFilter(ctx, networkID, subTypeFor(kind), filterHash)
	}
}

// --- internals --------------------------------------------------------

// buildWsAdapterOptions resolves network-level toggles that the wsupstream
// adapter needs into a flat Options struct. Returns nil when no override
// applies so the adapter keeps its defaults.
func buildWsAdapterOptions(cfg *common.NetworkConfig) *wsupstream.Options {
	if cfg == nil || cfg.Evm == nil {
		return nil
	}
	opts := &wsupstream.Options{}
	set := false
	if cfg.Evm.StripSubscribeFromBlockZero != nil && *cfg.Evm.StripSubscribeFromBlockZero {
		opts.StripSubscribeFromBlockZero = true
		set = true
	}
	if !set {
		return nil
	}
	return opts
}

// bootstrapNetwork registers the network with the indexer and attaches a
// wsupstream.Adapter for each WS upstream on the network. Idempotent per
// networkId — subsequent calls are no-ops.
func (sm *SubscriptionManager) bootstrapNetwork(ctx context.Context, nw *Network) error {
	networkID := nw.networkId
	if _, ok := sm.networks.Load(networkID); ok {
		return nil
	}
	sm.bootstrapMu.Lock()
	defer sm.bootstrapMu.Unlock()
	if _, ok := sm.networks.Load(networkID); ok {
		return nil
	}

	wsUpstreams := nw.upstreamsRegistry.GetWsUpstreams(ctx, networkID)
	if len(wsUpstreams) == 0 {
		return common.NewErrNoWsUpstreamAvailable(networkID)
	}

	sm.idx.RegisterNetwork(&networkHandle{nw: nw})
	sm.idx.RegisterNetworkSelector(networkID, &subIngressSelector{nw: nw, networkID: networkID})
	adapterOpts := buildWsAdapterOptions(nw.cfg)
	for _, up := range wsUpstreams {
		adapter := wsupstream.New(up, networkID, sm.logger, adapterOpts)
		if adapter == nil {
			continue
		}
		if err := sm.idx.AddIngress(ctx, networkID, adapter); err != nil {
			sm.logger.Warn().Err(err).Str("upstreamId", up.Id()).
				Msg("failed to register upstream ingress with indexer")
		}
	}
	sm.networks.Store(networkID, struct{}{})
	return nil
}

// subIngressSelector routes filter subscribes through the same upstream
// selector used by the HTTP request path: score-ordered, circuit-breaker
// aware, and group-tiered when failover.onDefaultsExhausted is set. The
// output is a list of EventIngress names (matching wsupstream.Adapter.Name()
// == "ws:" + upstreamId).
type subIngressSelector struct {
	nw        *Network
	networkID string
}

// Select returns (defaults, fallbacks) for a filter subscribe. Both tiers
// are ordered by the upstream registry's score for eth_subscribe; upstreams
// whose circuit breaker is open are skipped. Fallback-group upstreams only
// populate the fallback tier when network-level failover.onDefaultsExhausted
// is enabled — otherwise they mix into the defaults, matching the HTTP
// selector's behaviour for non-failover networks.
func (s *subIngressSelector) Select(_networkId, _subType string, _params []interface{}) (defaults, fallbacks []string) {
	if s == nil || s.nw == nil || s.nw.upstreamsRegistry == nil {
		return nil, nil
	}
	// A single method key keeps subscribe-path scoring warm across subTypes
	// and mirrors how the HTTP path warms method-scoped sort lists.
	ups, err := s.nw.upstreamsRegistry.GetSortedUpstreams(context.Background(), s.networkID, MethodEthSubscribe)
	if err != nil {
		return nil, nil
	}

	failoverOn := s.nw.cfg != nil && s.nw.cfg.Failover.Enabled()
	for _, u := range ups {
		up, ok := u.(*upstream.Upstream)
		if !ok {
			continue
		}
		cfg := up.Config()
		if cfg == nil {
			continue
		}
		if !isWsEndpoint(cfg.Endpoint) {
			continue
		}
		if up.IsDown() {
			continue
		}
		name := "ws:" + up.Id()
		if failoverOn && cfg.HasTag(common.TagTierFallback) {
			fallbacks = append(fallbacks, name)
			continue
		}
		defaults = append(defaults, name)
	}
	return defaults, fallbacks
}

func isWsEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	return parsed.Scheme == "ws" || parsed.Scheme == "wss"
}

// resolveSubscription validates the subType and, for filter subs,
// translates params into a filterHash via the indexer. Returns
// (kind, filterHash) for subsequent AddSubscription.
func (sm *SubscriptionManager) resolveSubscription(ctx context.Context, networkID, subType string, params []interface{}) (indexer.EventKind, string, error) {
	switch subType {
	case indexer.SubTypeNewHeads:
		return indexer.KindNewHead, "", nil
	case indexer.SubTypeLogs, indexer.SubTypeNewPendingTransactions:
		hash, err := sm.idx.EnsureFilter(ctx, networkID, subType, params)
		if err != nil {
			return 0, "", err
		}
		kind := indexer.KindLog
		if subType == indexer.SubTypeNewPendingTransactions {
			kind = indexer.KindPendingTx
		}
		return kind, hash, nil
	default:
		return 0, "", common.NewErrJsonRpcExceptionInternal(0, common.JsonRpcErrorInvalidArgument,
			fmt.Sprintf("unsupported subscription type: %q", subType), nil, nil)
	}
}

// getOrCreateConn returns the egress adapter for a WsConnection,
// attaching a new one to the indexer if this is the first subscription
// on that connection. Returns nil once the connection was cleaned up.
func (sm *SubscriptionManager) getOrCreateConn(wsc *WsConnection) *connEntry {
	sm.connMu.Lock()
	defer sm.connMu.Unlock()
	if wsc.subsClosed {
		return nil
	}
	if existing, ok := sm.conns[wsc.id]; ok {
		return existing
	}
	adapter := wsclient.New(wsc.id, wsc, sm.logger, wsc.server.serverCfg.WebSocket.SubscriptionBufferSize)
	entry := &connEntry{adapter: adapter, detach: sm.idx.Attach(adapter)}
	sm.conns[wsc.id] = entry
	return entry
}

// acquireRateLimits acquires rate-limit permits at both project and
// network level.
func (sm *SubscriptionManager) acquireRateLimits(
	ctx context.Context,
	project *PreparedProject,
	nw *Network,
	nq *common.NormalizedRequest,
) error {
	if err := project.AcquireRateLimitPermit(ctx, nq); err != nil {
		return err
	}
	return nw.acquireRateLimitPermit(ctx, nq)
}

// buildSubscribeResponse constructs the JSON-RPC response carrying the
// client-facing subscription ID.
func (sm *SubscriptionManager) buildSubscribeResponse(
	nq *common.NormalizedRequest,
	jrReq *common.JsonRpcRequest,
	clientSubID string,
) *common.NormalizedResponse {
	resp := common.NewNormalizedResponse().WithRequest(nq)
	jrr := &common.JsonRpcResponse{}
	_ = jrr.SetID(jrReq.ID)
	jrr.SetResult([]byte(fmt.Sprintf(`"%s"`, clientSubID)))
	resp.WithJsonRpcResponse(jrr)
	return resp
}

// recordFailureMetrics emits the failure-path metrics when a subscribe
// or unsubscribe request fails before reaching the indexer.
func (sm *SubscriptionManager) recordFailureMetrics(
	project *PreparedProject,
	nw *Network,
	method string,
	finality common.DataFinalityState,
	start time.Time,
	nq *common.NormalizedRequest,
	err error,
) {
	telemetry.CounterHandle(telemetry.MetricNetworkFailedRequests,
		project.Config.Id, nw.Label(), method,
		"0", // no upstream attempts for client-facing subscription failures
		common.ErrorFingerprint(err),
		string(common.ClassifySeverity(err)),
		finality.String(),
		nq.UserId(),
		nq.AgentName(),
	).Inc()
	telemetry.ObserverHandle(telemetry.MetricNetworkRequestDuration,
		project.Config.Id, nw.Label(), "<error>", "<error>",
		method, finality.String(), nq.UserId(),
	).Observe(time.Since(start).Seconds())
}

// subTypeFor is the inverse of the kind-to-subType mapping done in
// resolveSubscription. Used by CleanupConnection where we've only got
// the adapter's Kind in hand.
func subTypeFor(kind indexer.EventKind) string {
	switch kind {
	case indexer.KindLog:
		return indexer.SubTypeLogs
	case indexer.KindPendingTx:
		return indexer.SubTypeNewPendingTransactions
	}
	return ""
}

// --- NetworkHandle ----------------------------------------------------

// networkHandle adapts *Network to indexer.NetworkHandle. Lives here
// (rather than in indexer/) because it touches Network internals; the
// indexer package deliberately doesn't know about erpc.
type networkHandle struct {
	nw *Network
}

func (h *networkHandle) Id() string { return h.nw.networkId }

// SuggestLatestBlock routes a per-source block observation to the
// upstream's state poller, then advances the network-level latest tip.
// sourceId is the ingress adapter's Name(), which for wsupstream.Adapter
// is "ws:<upstreamId>". payload is unused (kept for indexer.NetworkHandle).
//
// Ordering matters: Indexer.Ingest calls this BEFORE fan-out, so by the
// time any client sees head N on WS, EvmHighestLatestBlockNumber on this
// instance is already ≥ N. That invariant applies to every ingress source,
// including tier:fallback: Ingest fans out all sources, so skipping the tip
// advance for fallback heads while still delivering them to clients leaves
// the WS tip ahead of the HTTP tip, which strict clients treat as out of
// sync. A tip re-fetch for a head that came from a fallback must reach that
// fallback via the emptyish escape hatch instead.
func (h *networkHandle) SuggestLatestBlock(sourceId string, blockNumber int64, payload json.RawMessage) {
	_ = payload
	const prefix = "ws:"
	if !strings.HasPrefix(sourceId, prefix) {
		return
	}
	upstreamID := sourceId[len(prefix):]
	for _, u := range h.nw.upstreamsRegistry.GetNetworkUpstreams(context.Background(), h.nw.networkId) {
		if u.Id() != upstreamID {
			continue
		}
		poller := u.EvmStatePoller()
		if poller != nil && !poller.IsObjectNull() {
			poller.SuggestLatestBlock(blockNumber)
		}
		break
	}
	h.nw.NoteObservedLatestBlock(h.nw.appCtx, blockNumber)
}

// Interface checks: fail the build if either contract drifts.
var (
	_ wsclient.NotificationWriter = (*WsConnection)(nil)
	_ indexer.NetworkHandle       = (*networkHandle)(nil)
)
