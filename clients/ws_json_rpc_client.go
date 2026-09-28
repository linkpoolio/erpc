package clients

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	wsWriteWait        = 10 * time.Second
	wsHandshakeTimeout = 10 * time.Second
	wsReconnectMin     = 1 * time.Second
	wsReconnectMax     = 30 * time.Second
	wsReconnectFactor  = 2.0
)

// Liveness windows. The peer must produce SOME traffic (a pong reply or a
// data frame) within wsPongWait, or the connection is declared dead, torn
// down, and re-dialed. A half-open TCP connection (peer host vanished
// without FIN/RST, or an intermediate proxy black-holing frames) otherwise
// blocks ReadMessage forever while ping writes keep "succeeding" into the
// kernel/proxy buffer — the client then believes it is connected and never
// re-dials. wsPongWait must comfortably exceed wsPingInterval so at least
// two pings fit in the window.
//
// Vars (not consts) so tests can compress time. They are copied into
// per-client fields at construction, so client goroutines never read them
// after NewWsJsonRpcClient returns.
var (
	wsPingInterval = 30 * time.Second
	wsPongWait     = 75 * time.Second
)

var errWsNotConnected = errors.New("websocket connection not established")

// WsJsonRpcClient implements ClientInterface for WebSocket-based JSON-RPC upstream connections.
type WsJsonRpcClient struct {
	Url     *url.URL
	headers http.Header

	projectId string
	upstream  common.Upstream
	appCtx    context.Context
	logger    *zerolog.Logger

	// Liveness windows, snapshotted from wsPingInterval/wsPongWait at
	// construction (before any goroutine starts).
	pingInterval time.Duration
	pongWait     time.Duration

	// Connection state. Only readLoop replaces or tears down conn (besides
	// the initial dial and shutdown); the dial itself runs without connMu so
	// requests never wait on a handshake.
	connMu      sync.Mutex
	conn        *websocket.Conn
	connectedAt time.Time
	// epoch identifies the current connection (0 while disconnected);
	// written under connMu, epochs counts connections ever installed.
	epoch  atomic.Uint64
	epochs uint64

	// Write synchronization (gorilla/websocket requires synchronized writes)
	writeMu sync.Mutex

	// Pending request tracking: wire id -> request. Whoever deletes an
	// entry owns it, so duplicate responses are dropped.
	pendingMu sync.Mutex
	pending   map[string]*wsPending

	// Subscription notification callbacks: upstreamSubID -> handler, for
	// the current connection only (ids are connection-scoped).
	subHandlersMu sync.RWMutex
	subHandlers   map[string]func(params []byte)

	// Disconnect/reconnect callbacks are keyed by caller-supplied IDs so
	// subscribers can replace (on re-subscribe) and remove (on teardown)
	// their hooks, preventing the callback slices from growing unbounded
	// over long-lived connections with subscription churn.
	onDisconnectMu  sync.RWMutex
	onDisconnectCbs map[string]func()

	onReconnectMu  sync.RWMutex
	onReconnectCbs map[string]func()

	// Error extractor for architecture-specific error normalization
	errorExtractor common.JsonRpcErrorExtractor

	// metricLabels are the labels the connectivity gauge was last set under.
	metricMu     sync.Mutex
	metricLabels []string

	// wireIDCounter generates the JSON-RPC ids used on the wire so that
	// concurrent requests with the same caller-supplied id do not collide
	// in pending. The caller's id is restored on the response.
	wireIDCounter atomic.Uint64
}

// WsSubscription ties an eth_subscribe or eth_unsubscribe request to the
// connection it is scoped to. The client only sends these methods when the
// request context carries one (see WithWsSubscription): a subscription
// created by a routed request would have no owner to receive or cancel it.
type WsSubscription struct {
	// Handler receives the subscription's notifications (eth_subscribe
	// only). It is registered before the next frame is read, so nothing
	// sent right after the subscribe response is missed.
	Handler func(params []byte)
	// ID and Epoch are set when an eth_subscribe succeeds. eth_unsubscribe
	// must carry the Epoch of the subscription it cancels; it is not sent
	// once that connection is gone, since ids are reused across connections.
	ID    string
	Epoch uint64
}

type wsSubscriptionCtxKey struct{}

// WithWsSubscription returns ctx carrying sub for an eth_subscribe or
// eth_unsubscribe sent through SendRequest.
func WithWsSubscription(ctx context.Context, sub *WsSubscription) context.Context {
	return context.WithValue(ctx, wsSubscriptionCtxKey{}, sub)
}

type wsPending struct {
	ch chan *wsPendingResult
	// sub is set for eth_subscribe. If the caller gives up, the entry is
	// kept (abandoned, guarded by pendingMu) so a late success is
	// unsubscribed instead of leaked.
	sub       *WsSubscription
	abandoned bool
}

type wsPendingResult struct {
	message []byte
	err     error
}

// wsMessage is a minimal struct for parsing incoming WS messages to determine if they are
// responses (have "id") or notifications (have "method").
type wsMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// wsNotificationParams is the structure of subscription notification params.
type wsNotificationParams struct {
	Subscription string          `json:"subscription"`
	Result       json.RawMessage `json:"result"`
}

func NewWsJsonRpcClient(
	appCtx context.Context,
	logger *zerolog.Logger,
	projectId string,
	upstream common.Upstream,
	parsedUrl *url.URL,
	jsonRpcCfg *common.JsonRpcUpstreamConfig,
	extractor common.JsonRpcErrorExtractor,
) (ClientInterface, error) {
	headers := http.Header{}
	if jsonRpcCfg != nil && jsonRpcCfg.Headers != nil {
		for k, v := range jsonRpcCfg.Headers {
			headers.Set(k, v)
		}
	}

	client := &WsJsonRpcClient{
		Url:             parsedUrl,
		headers:         headers,
		pingInterval:    wsPingInterval,
		pongWait:        wsPongWait,
		projectId:       projectId,
		upstream:        upstream,
		appCtx:          appCtx,
		logger:          logger,
		pending:         make(map[string]*wsPending),
		subHandlers:    make(map[string]func(params []byte)),
		onDisconnectCbs: make(map[string]func()),
		onReconnectCbs:  make(map[string]func()),
		errorExtractor:  extractor,
	}

	if err := client.connect(); err != nil {
		// Don't fail on initial connection — readLoop keeps re-dialing in
		// the background. The upstream may not be available at startup.
		logger.Warn().Err(err).Str("url", parsedUrl.String()).Msg("initial websocket connection failed, will retry in background")
	}

	go client.readLoop()
	go client.pingLoop()
	go func() {
		<-appCtx.Done()
		client.shutdown()
	}()

	return client, nil
}

func (c *WsJsonRpcClient) GetType() ClientType {
	return ClientTypeWsJsonRpc
}

// IsConnected returns true if the upstream WebSocket connection is currently established.
func (c *WsJsonRpcClient) IsConnected() bool {
	return c.epoch.Load() != 0
}

// Epoch identifies the current connection, or is 0 while disconnected. It
// changes on every reconnect.
func (c *WsJsonRpcClient) Epoch() uint64 {
	return c.epoch.Load()
}

func (c *WsJsonRpcClient) SendRequest(ctx context.Context, req *common.NormalizedRequest) (*common.NormalizedResponse, error) {
	ctx, span := common.StartDetailSpan(ctx, "WsJsonRpcClient.SendRequest",
		trace.WithAttributes(
			attribute.String("upstream.id", c.upstream.Id()),
		),
	)
	defer span.End()

	startedAt := time.Now()

	jrReq, err := req.JsonRpcRequest()
	if err != nil {
		return nil, common.NewErrUpstreamRequest(
			err,
			c.upstream,
			req.NetworkId(),
			"",
			0, 0, 0, 0,
		)
	}

	wireID := c.wireIDCounter.Add(1)
	idKey := strconv.FormatUint(wireID, 10)

	// Serialize the JSON-RPC request with the rewritten wire id
	jrReq.RLock()
	originalID := jrReq.ID
	method := jrReq.Method
	requestBody, err := common.SonicCfg.Marshal(map[string]interface{}{
		"jsonrpc": jrReq.JSONRPC,
		"id":      wireID,
		"method":  method,
		"params":  jrReq.Params,
	})
	jrReq.RUnlock()
	if err != nil {
		common.SetTraceSpanError(span, err)
		return nil, common.NewErrUpstreamRequest(
			err,
			c.upstream,
			req.NetworkId(),
			method,
			0, 0, 0, 0,
		)
	}

	pending := &wsPending{ch: make(chan *wsPendingResult, 1)}
	var unsubscribe *WsSubscription
	if method == "eth_subscribe" || method == "eth_unsubscribe" {
		sub, _ := ctx.Value(wsSubscriptionCtxKey{}).(*WsSubscription)
		if sub == nil {
			return nil, common.NewErrUpstreamRequestSkipped(
				fmt.Errorf("%s is scoped to the upstream connection and only sent by its subscription owner", method),
				c.upstream.Id(),
			)
		}
		if method == "eth_subscribe" {
			pending.sub = sub
		} else {
			unsubscribe = sub
		}
	}

	// Register under connMu so the entry belongs to exactly this conn:
	// teardown swaps conn and drains pending under the same lock.
	c.connMu.Lock()
	conn := c.conn
	stale := unsubscribe != nil && unsubscribe.Epoch != c.epoch.Load()
	if conn != nil && !stale {
		c.pendingMu.Lock()
		c.pending[idKey] = pending
		c.pendingMu.Unlock()
	}
	c.connMu.Unlock()

	if stale {
		// The subscription died with its connection; its id may since have
		// been reused on the new one.
		return nil, common.NewErrUpstreamRequestSkipped(errors.New("subscription's connection is already closed"), c.upstream.Id())
	}
	if conn == nil {
		// Re-dial in progress: fail fast so the request fails over.
		err := common.NewErrEndpointTransportFailure(c.Url, errWsNotConnected)
		common.SetTraceSpanError(span, err)
		return nil, err
	}

	if err := c.writeToConn(conn, websocket.TextMessage, requestBody); err != nil {
		c.removePending(idKey)
		common.SetTraceSpanError(span, err)
		return nil, common.NewErrEndpointTransportFailure(c.Url, err)
	}

	c.logger.Debug().
		Str("host", c.Url.Host).
		RawJSON("request", requestBody).
		Msg("sent json rpc websocket request")

	// Wait for response
	select {
	case result := <-pending.ch:
		if result.err != nil {
			common.SetTraceSpanError(span, result.err)
			return nil, result.err
		}
		nr := common.NewNormalizedResponse().WithRequest(req).WithBody(io.NopCloser(bytes.NewReader(result.message)))
		// Restore the caller's original JSON-RPC id on the response.
		jrr, perr := nr.JsonRpcResponse(ctx)
		if perr == nil && jrr != nil {
			_ = jrr.SetID(originalID)
		}
		if err := classifyJsonRpcError(&http.Response{StatusCode: http.StatusOK, Header: http.Header{}}, nr, jrr, perr, c.errorExtractor, c.upstream); err != nil {
			common.SetTraceSpanError(span, err)
			return nr, err
		}
		return nr, nil
	case <-ctx.Done():
		c.abandonPending(idKey)
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			err = common.NewErrEndpointRequestTimeout(time.Since(startedAt), err)
		} else if errors.Is(err, context.Canceled) {
			err = common.NewErrEndpointRequestCanceled(err)
		}
		common.SetTraceSpanError(span, err)
		return nil, err
	case <-c.appCtx.Done():
		c.abandonPending(idKey)
		return nil, common.NewErrEndpointRequestCanceled(c.appCtx.Err())
	}
}

func (c *WsJsonRpcClient) removePending(idKey string) {
	c.pendingMu.Lock()
	delete(c.pending, idKey)
	c.pendingMu.Unlock()
}

// abandonPending drops a request whose caller gave up. A subscribe stays
// registered so readLoop can cancel it if the upstream still creates it.
func (c *WsJsonRpcClient) abandonPending(idKey string) {
	c.pendingMu.Lock()
	if p, ok := c.pending[idKey]; ok {
		if p.sub != nil {
			p.abandoned = true
		} else {
			delete(c.pending, idKey)
		}
	}
	c.pendingMu.Unlock()
}

// UnregisterSubscriptionHandler removes the handler of a subscription made
// on connection epoch. A no-op once that connection is gone: its handlers
// were dropped with it, and the id may be reused by the next connection.
func (c *WsJsonRpcClient) UnregisterSubscriptionHandler(upstreamSubID string, epoch uint64) {
	c.subHandlersMu.Lock()
	if c.epoch.Load() == epoch {
		delete(c.subHandlers, upstreamSubID)
	}
	c.subHandlersMu.Unlock()
}

// SetOnDisconnect registers (or replaces) the callback keyed by id that fires
// when the upstream WS connection drops. Use RemoveOnDisconnect(id) to
// deregister on subscription teardown so long-lived connections don't
// accumulate dead callbacks.
func (c *WsJsonRpcClient) SetOnDisconnect(id string, callback func()) {
	c.onDisconnectMu.Lock()
	c.onDisconnectCbs[id] = callback
	c.onDisconnectMu.Unlock()
}

// RemoveOnDisconnect deregisters a disconnect callback previously set with
// SetOnDisconnect. A no-op if id is not registered.
func (c *WsJsonRpcClient) RemoveOnDisconnect(id string) {
	c.onDisconnectMu.Lock()
	delete(c.onDisconnectCbs, id)
	c.onDisconnectMu.Unlock()
}

// SetOnReconnect registers (or replaces) the callback keyed by id that fires
// after a successful reconnect.
func (c *WsJsonRpcClient) SetOnReconnect(id string, callback func()) {
	c.onReconnectMu.Lock()
	c.onReconnectCbs[id] = callback
	c.onReconnectMu.Unlock()
}

// RemoveOnReconnect deregisters a reconnect callback previously set with
// SetOnReconnect. A no-op if id is not registered.
func (c *WsJsonRpcClient) RemoveOnReconnect(id string) {
	c.onReconnectMu.Lock()
	delete(c.onReconnectCbs, id)
	c.onReconnectMu.Unlock()
}

func (c *WsJsonRpcClient) connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: wsHandshakeTimeout,
	}

	if c.Url.Scheme == "wss" {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	conn, _, err := dialer.DialContext(c.appCtx, c.Url.String(), c.headers)
	if err != nil {
		return err
	}

	// Arm the liveness deadline: if neither a pong nor a data frame arrives
	// within pongWait, ReadMessage fails and readLoop re-dials. The pong
	// handler runs inside ReadMessage's frame processing, so extending the
	// deadline here covers the ping/pong path; readLoop extends it again on
	// every data frame.
	_ = conn.SetReadDeadline(time.Now().Add(c.pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(c.pongWait))
	})

	c.connMu.Lock()
	if c.appCtx.Err() != nil {
		// shutdown already ran; don't install a connection behind it.
		c.connMu.Unlock()
		_ = conn.Close()
		return c.appCtx.Err()
	}
	c.conn = conn
	c.connectedAt = time.Now()
	c.epochs++
	c.epoch.Store(c.epochs)
	c.connMu.Unlock()
	c.setConnectedMetric()

	c.logger.Info().Str("url", c.Url.String()).Msg("websocket connection established")
	return nil
}

// teardownConn closes conn, marks the client disconnected, fails every
// request pending on it and drops its subscription handlers.
func (c *WsJsonRpcClient) teardownConn(conn *websocket.Conn, cause error) {
	c.connMu.Lock()
	c.conn = nil
	c.epoch.Store(0)
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[string]*wsPending)
	c.pendingMu.Unlock()
	c.connMu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	c.subHandlersMu.Lock()
	clear(c.subHandlers)
	c.subHandlersMu.Unlock()
	c.setConnectedMetric()
	for _, p := range pending {
		p.ch <- &wsPendingResult{err: cause}
	}
}

// setConnectedMetric publishes the upstream WS connectivity gauge so
// operators can alert on a wedged/disconnected upstream socket instead of
// discovering it from silent client subscriptions. Labels are resolved on
// every publish (pingLoop republishes each tick) because the upstream's
// network label is only assigned after the client is built; the series
// under the previous labels is deleted when they change.
func (c *WsJsonRpcClient) setConnectedMetric() {
	if c.upstream == nil {
		return
	}
	labels := []string{c.projectId, c.upstream.VendorName(), c.upstream.NetworkLabel(), c.upstream.Id()}

	c.metricMu.Lock()
	defer c.metricMu.Unlock()
	if c.metricLabels != nil && !slices.Equal(c.metricLabels, labels) {
		telemetry.MetricUpstreamWebsocketConnected.DeleteLabelValues(c.metricLabels...)
	}
	c.metricLabels = labels
	v := 0.0
	if c.IsConnected() {
		v = 1
	}
	telemetry.MetricUpstreamWebsocketConnected.WithLabelValues(labels...).Set(v)
}

// readLoop owns the connection lifecycle: it reads the current connection
// and, when there is none, re-dials with backoff. pingLoop only closes a
// broken connection, so a nil conn always has a re-dial behind it.
func (c *WsJsonRpcClient) readLoop() {
	backoff := wsReconnectMin
	for {
		if c.appCtx.Err() != nil {
			return
		}

		c.connMu.Lock()
		conn, connectedAt, epoch := c.conn, c.connectedAt, c.epoch.Load()
		c.connMu.Unlock()

		if conn == nil {
			// Always wait before dialing so a peer that accepts and then
			// immediately drops the connection can't drive a hot loop.
			wait := backoff/2 + rand.N(backoff/2+1)
			c.logger.Info().Dur("backoff", wait).Msg("attempting websocket reconnection")
			select {
			case <-time.After(wait):
			case <-c.appCtx.Done():
				return
			}
			if err := c.connect(); err != nil {
				c.logger.Warn().Err(err).Msg("websocket reconnection failed")
				backoff = min(time.Duration(float64(backoff)*wsReconnectFactor), wsReconnectMax)
				continue
			}
			c.logger.Info().Msg("websocket reconnected successfully")
			c.fireCallbacks(&c.onReconnectMu, c.onReconnectCbs)
			continue
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if c.appCtx.Err() != nil {
				return
			}
			var netErr net.Error
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.logger.Info().Msg("websocket connection closed normally")
			} else if errors.As(err, &netErr) && netErr.Timeout() {
				c.logger.Warn().Err(err).Dur("pongWait", c.pongWait).
					Msg("websocket peer silent beyond liveness deadline (no pong/data), tearing down connection and reconnecting")
			} else {
				c.logger.Warn().Err(err).Msg("websocket read error, will reconnect")
			}
			// Only a connection that outlived a liveness window resets the
			// backoff; one dropped right after the handshake counts as a
			// failed dial.
			if time.Since(connectedAt) >= c.pongWait {
				backoff = wsReconnectMin
			} else {
				backoff = min(time.Duration(float64(backoff)*wsReconnectFactor), wsReconnectMax)
			}
			c.teardownConn(conn, common.NewErrEndpointTransportFailure(c.Url, fmt.Errorf("websocket connection lost: %w", err)))
			c.fireCallbacks(&c.onDisconnectMu, c.onDisconnectCbs)
			continue
		}

		// Any inbound frame proves the peer is alive — push the liveness
		// deadline forward.
		_ = conn.SetReadDeadline(time.Now().Add(c.pongWait))

		c.handleMessage(message, epoch)
	}
}

// fireCallbacks snapshots the callback map under rlock and invokes each
// callback synchronously, so disconnect callbacks always complete before
// the reconnect callbacks of the next connection run. Callbacks must
// therefore be fast and must not block on the WS client's request path.
func (c *WsJsonRpcClient) fireCallbacks(mu *sync.RWMutex, cbs map[string]func()) {
	mu.RLock()
	snapshot := make([]func(), 0, len(cbs))
	for _, cb := range cbs {
		snapshot = append(snapshot, cb)
	}
	mu.RUnlock()
	for _, cb := range snapshot {
		cb()
	}
}

// handleMessage dispatches one frame read from connection epoch.
func (c *WsJsonRpcClient) handleMessage(message []byte, epoch uint64) {
	var msg wsMessage
	if err := common.SonicCfg.Unmarshal(message, &msg); err != nil {
		c.logger.Warn().Err(err).Str("raw", string(message)).Msg("failed to parse websocket message")
		return
	}

	// Subscription notification: has "method" field (typically "eth_subscription")
	if msg.Method != "" && msg.ID == nil {
		c.handleNotification(msg.Method, msg.Params)
		return
	}

	// Response to a pending request: has "id" field
	if msg.ID != nil {
		idKey := normalizeIDKey(msg.ID)

		c.pendingMu.Lock()
		p, ok := c.pending[idKey]
		delete(c.pending, idKey)
		abandoned := ok && p.abandoned
		c.pendingMu.Unlock()

		if !ok {
			c.logger.Debug().Str("id", idKey).Msg("received response for unknown request ID")
			return
		}
		var subID string
		if p.sub != nil && common.SonicCfg.Unmarshal(msg.Result, &subID) == nil && subID != "" {
			if abandoned {
				go c.unsubscribeOrphan(subID, epoch)
				return
			}
			// Register before the next frame is read so no notification
			// is dropped as belonging to an unknown subscription.
			c.subHandlersMu.Lock()
			c.subHandlers[subID] = p.sub.Handler
			c.subHandlersMu.Unlock()
			p.sub.ID, p.sub.Epoch = subID, epoch
		}
		if !abandoned {
			p.ch <- &wsPendingResult{message: message}
		}
		return
	}

	c.logger.Debug().Str("raw", string(message)).Msg("received unhandled websocket message")
}

func (c *WsJsonRpcClient) handleNotification(method string, params []byte) {
	if method != "eth_subscription" {
		c.logger.Debug().Str("method", method).Msg("received non-subscription notification")
		return
	}

	var notifParams wsNotificationParams
	if err := common.SonicCfg.Unmarshal(params, &notifParams); err != nil {
		c.logger.Warn().Err(err).Msg("failed to parse subscription notification params")
		return
	}

	c.subHandlersMu.RLock()
	handler, ok := c.subHandlers[notifParams.Subscription]
	c.subHandlersMu.RUnlock()

	if !ok {
		c.logger.Debug().Str("subscriptionId", notifParams.Subscription).Msg("received notification for unknown subscription")
		return
	}

	handler(params)
}

// writeToConn writes to an explicit connection so callers that need to act
// on a write failure (e.g. pingLoop closing the broken conn) operate on the
// exact connection they wrote to, not whatever c.conn points at by then.
func (c *WsJsonRpcClient) writeToConn(conn *websocket.Conn, messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

// unsubscribeOrphan cancels a subscription the upstream created after its
// caller gave up. Fire-and-forget: the response is dropped as unknown.
func (c *WsJsonRpcClient) unsubscribeOrphan(subID string, epoch uint64) {
	body, err := common.SonicCfg.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      c.wireIDCounter.Add(1),
		"method":  "eth_unsubscribe",
		"params":  []string{subID},
	})
	if err != nil {
		return
	}
	c.connMu.Lock()
	conn := c.conn
	live := c.epoch.Load() == epoch
	c.connMu.Unlock()
	if conn != nil && live {
		_ = c.writeToConn(conn, websocket.TextMessage, body)
	}
}

func (c *WsJsonRpcClient) pingLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.setConnectedMetric()
			c.connMu.Lock()
			conn := c.conn
			c.connMu.Unlock()
			if conn == nil {
				continue
			}
			if err := c.writeToConn(conn, websocket.PingMessage, nil); err != nil {
				// Only close it: readLoop's ReadMessage then fails and it
				// tears down and re-dials. Clearing c.conn here instead would
				// leave nobody to re-dial if readLoop was busy in a handler.
				c.logger.Warn().Err(err).Msg("websocket ping write failed, closing connection to force reconnect")
				_ = conn.Close()
			}
		case <-c.appCtx.Done():
			return
		}
	}
}

// normalizeIDKey converts a JSON-RPC ID to a stable string key.
// JSON unmarshalling turns integer IDs into float64, which can produce
// scientific notation with fmt.Sprintf (e.g., "1.51e+09" vs "1510000000").
// This function normalizes to avoid mismatches.
func normalizeIDKey(id interface{}) string {
	switch v := id.(type) {
	case float64:
		// Format without scientific notation
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (c *WsJsonRpcClient) shutdown() {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn != nil {
		// Send close frame and close
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(wsWriteWait),
		)
	}
	c.teardownConn(conn, common.NewErrEndpointRequestCanceled(fmt.Errorf("websocket client shutting down")))
}
