package hranago

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn is a driver.Conn backed by a persistent Hrana WebSocket connection.
// One wsConn owns one server-side Hrana stream. If the stream is superseded by
// a newer database generation, the client transparently closes the old stream,
// opens a fresh one, and retries the original request.
//
// The Hrana WS protocol works as follows:
//  1. Client → hello  (with optional JWT)
//  2. Server → hello_ok
//  3. Client → request{open_stream, stream_id=N}
//  4. Server → response_ok
//  5. Client → request{execute, stream_id=N, stmt=...}  (repeated)
//  6. Client → request{close_stream, stream_id=N}  (on Close)
type wsConn struct {
	cfg      *config
	ws       *websocket.Conn
	writeMu  sync.Mutex // serialises ws.WriteMessage calls
	mu       sync.Mutex // guards pending, closed, closeErr
	reqIDSeq int32      // atomically incremented per request
	pending  map[int32]*wsPending
	closed   bool
	closeErr error

	streamID         int32
	recovering       chan struct{}
	maxStreamRetries int32
}

type wsResult struct {
	data json.RawMessage
	err  error
}

type wsPending struct {
	payload any
	ch      chan wsResult
	retries int32
}

// newWSConn dials the server, completes the Hrana hello handshake, opens
// stream 0, and starts the background read loop.
func newWSConn(cfg *config) (*wsConn, error) {
	// Map version ("v3") to WS subprotocol ("hrana3").
	subproto := "hrana" + cfg.apiVersion[1:]

	dialer := websocket.Dialer{
		Subprotocols: []string{subproto},
	}
	header := http.Header{}
	if cfg.authToken != "" {
		header.Set("Authorization", "Bearer "+cfg.authToken)
	}

	ws, _, err := dialer.DialContext(context.Background(), cfg.baseURL, header)
	if err != nil {
		return nil, fmt.Errorf("hrana: ws dial: %w", err)
	}

	c := &wsConn{
		cfg:              cfg,
		ws:               ws,
		pending:          make(map[int32]*wsPending),
		streamID:         0,
		maxStreamRetries: 3,
	}

	// Send hello.
	type helloMsg struct {
		Type string  `json:"type"`
		JWT  *string `json:"jwt,omitempty"`
	}
	hello := helloMsg{Type: "hello"}
	if cfg.authToken != "" {
		hello.JWT = &cfg.authToken
	}
	if err := c.writeJSON(hello); err != nil {
		ws.Close()
		return nil, fmt.Errorf("hrana: ws hello send: %w", err)
	}

	// Read hello_ok / hello_error synchronously before starting the read loop.
	_, data, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, fmt.Errorf("hrana: ws hello response: %w", err)
	}
	var helloResp struct {
		Type  string      `json:"type"`
		Error *hranaError `json:"error,omitempty"`
	}
	if err := json.Unmarshal(data, &helloResp); err != nil {
		ws.Close()
		return nil, fmt.Errorf("hrana: ws hello decode: %w", err)
	}
	switch helloResp.Type {
	case "hello_ok":
		// ready
	case "hello_error":
		ws.Close()
		if helloResp.Error != nil {
			return nil, fmt.Errorf("hrana: ws auth rejected: %s", helloResp.Error.Message)
		}
		return nil, fmt.Errorf("hrana: ws auth rejected")
	default:
		ws.Close()
		return nil, fmt.Errorf("hrana: ws unexpected message %q during hello", helloResp.Type)
	}

	// Start the read loop before sending requests.
	go c.readLoop()

	// Open stream 0.
	if err := c.openStream(context.Background()); err != nil {
		_ = ws.Close()
		return nil, err
	}

	return c, nil
}

// ─── driver.Conn ─────────────────────────────────────────────────────────────

func (c *wsConn) Prepare(query string) (driver.Stmt, error) {
	return &preparedStmt{conn: c, sql: query}, nil
}

func (c *wsConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Best-effort: close the server-side stream before dropping the connection.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.sendRequest(ctx, map[string]any{
		"type":      "close_stream",
		"stream_id": c.streamID,
	})

	_ = c.ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second),
	)
	return c.ws.Close()
}

func (c *wsConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ─── driver.ConnBeginTx ──────────────────────────────────────────────────────

func (c *wsConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	sql := "BEGIN"
	if opts.ReadOnly {
		sql = "BEGIN DEFERRED"
	}
	if _, err := c.execStatement(ctx, &stmt{SQL: sql, WantRows: false}); err != nil {
		return nil, err
	}
	return &tx{conn: c, ctx: ctx}, nil
}

// ─── driver.ExecerContext ────────────────────────────────────────────────────

func (c *wsConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	s, err := buildStmt(query, args, false)
	if err != nil {
		return nil, err
	}
	result, err := c.execStatement(ctx, s)
	if err != nil {
		return nil, err
	}
	return &execResult{result: result}, nil
}

// ─── driver.QueryerContext ───────────────────────────────────────────────────

func (c *wsConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s, err := buildStmt(query, args, true)
	if err != nil {
		return nil, err
	}
	result, err := c.execStatement(ctx, s)
	if err != nil {
		return nil, err
	}
	return newRows(result), nil
}

// ─── Internal helpers ──────────────────────────────────────────────────────────────

func (c *wsConn) openStream(ctx context.Context) error {
	_, err := c.sendRequest(ctx, map[string]any{
		"type":      "open_stream",
		"stream_id": c.streamID,
	})
	if err != nil {
		return fmt.Errorf("hrana: ws open_stream: %w", err)
	}
	return nil
}

// execStatement implements stmtExecutor for wsConn.
func (c *wsConn) execStatement(ctx context.Context, s *stmt) (*stmtResult, error) {
	resp, err := c.sendRequest(ctx, map[string]any{
		"type":      "execute",
		"stream_id": c.streamID,
		"stmt":      s,
	})
	if err != nil {
		return nil, err
	}

	var r struct {
		Type   string     `json:"type"`
		Result stmtResult `json:"result"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		return nil, fmt.Errorf("hrana: ws execute response: %w", err)
	}
	return &r.Result, nil
}

// sendRequest sends a Hrana request and blocks until the matching response
// arrives on the read loop, or until ctx is cancelled.
func (c *wsConn) sendRequest(ctx context.Context, payload any) (json.RawMessage, error) {
	return c.sendRequestWithRetries(ctx, payload, 0)
}

func (c *wsConn) sendRequestWithRetries(ctx context.Context, payload any, retries int32) (json.RawMessage, error) {
	id := atomic.AddInt32(&c.reqIDSeq, 1)

	ch := make(chan wsResult, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("hrana: ws connection is closed")
	}
	c.pending[id] = &wsPending{payload: payload, ch: ch, retries: retries}
	c.mu.Unlock()

	msg := struct {
		Type      string `json:"type"`
		RequestID int32  `json:"request_id"`
		Request   any    `json:"request"`
	}{
		Type:      "request",
		RequestID: id,
		Request:   payload,
	}

	if err := c.writeJSON(msg); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("hrana: ws send: %w", err)
	}

	select {
	case result := <-ch:
		return result.data, result.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("hrana: ws request cancelled: %w", ctx.Err())
	}
}

// writeJSON marshals v and writes it as a WebSocket text frame.
// A write mutex prevents concurrent writes to the underlying conn.
func (c *wsConn) writeJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("hrana: marshal: %w", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(websocket.TextMessage, data)
}

// readLoop runs in a goroutine and dispatches server responses to the
// corresponding pending channels registered by sendRequest.
func (c *wsConn) readLoop() {
	var readErr error
	defer func() {
		if readErr == nil {
			readErr = fmt.Errorf("hrana: ws connection closed")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.closed = true
		c.closeErr = readErr
		for id, p := range c.pending {
			p.ch <- wsResult{err: readErr}
			delete(c.pending, id)
		}
	}()

	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			readErr = fmt.Errorf("hrana: ws read: %w", err)
			return
		}

		var base struct {
			Type      string `json:"type"`
			RequestID int32  `json:"request_id"`
		}
		if err := json.Unmarshal(data, &base); err != nil {
			continue
		}

		switch base.Type {
		case "response_ok":
			var msg struct {
				RequestID int32           `json:"request_id"`
				Response  json.RawMessage `json:"response"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			c.dispatch(msg.RequestID, wsResult{data: msg.Response})

		case "response_error":
			var msg struct {
				RequestID int32      `json:"request_id"`
				Error     hranaError `json:"error"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			c.dispatch(msg.RequestID, wsResult{err: fmt.Errorf("hrana: %s", msg.Error.Message)})

		case "stream_superseded_error":
			var msg struct {
				RequestID int32      `json:"request_id"`
				Error     hranaError `json:"error"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			c.handleStreamSuperseded(msg.RequestID, msg.Error.Message)
		}
	}
}

func (c *wsConn) handleStreamSuperseded(requestID int32, message string) {
	c.mu.Lock()
	p, ok := c.pending[requestID]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.pending, requestID)
	c.mu.Unlock()

	go c.retrySupersededRequest(p, message)
}

func (c *wsConn) retrySupersededRequest(p *wsPending, message string) {
	payloadMap, ok := p.payload.(map[string]any)
	if !ok {
		p.ch <- wsResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}
	if _, hasStreamID := payloadMap["stream_id"]; !hasStreamID {
		p.ch <- wsResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}
	if p.retries >= c.maxStreamRetries {
		p.ch <- wsResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}

	if err := c.recoverStream(); err != nil {
		p.ch <- wsResult{err: err}
		return
	}

	payloadMap["stream_id"] = c.streamID
	result, err := c.sendRequestWithRetries(context.Background(), payloadMap, p.retries+1)
	if err != nil {
		p.ch <- wsResult{err: err}
		return
	}
	p.ch <- wsResult{data: result}
}

func (c *wsConn) recoverStream() error {
	c.mu.Lock()
	if c.recovering != nil {
		wait := c.recovering
		c.mu.Unlock()
		<-wait
		return nil
	}
	done := make(chan struct{})
	c.recovering = done
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.recovering = nil
		close(done)
		c.mu.Unlock()
	}()

	// Close the superseded stream (best effort).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.sendRequestWithRetries(ctx, map[string]any{
		"type":      "close_stream",
		"stream_id": c.streamID,
	}, 0)

	// Open a fresh stream with an assigned id.
	resp, err := c.sendRequestWithRetries(ctx, map[string]any{
		"type": "open_stream_assigned",
	}, 0)
	if err != nil {
		return fmt.Errorf("hrana: ws recover stream: %w", err)
	}

	var assigned struct {
		Type     string `json:"type"`
		StreamID int32  `json:"stream_id"`
	}
	if err := json.Unmarshal(resp, &assigned); err != nil {
		return fmt.Errorf("hrana: ws open_stream_assigned decode: %w", err)
	}
	c.streamID = assigned.StreamID
	return nil
}

func (c *wsConn) dispatch(id int32, result wsResult) {
	c.mu.Lock()
	p, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if ok {
		p.ch <- result
	}
}
