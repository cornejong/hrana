//go:build js && wasm

package hranago

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall/js"
	"time"
)

type jsWSConn struct {
	cfg      *config
	codec    Codec
	ws       js.Value
	cbs      []js.Func  // references to JS callbacks to prevent garbage collection
	writeMu  sync.Mutex // serialises ws.Call("send") calls
	mu       sync.Mutex // guards pending, closed, closeErr
	reqIDSeq int32      // atomically incremented per request
	pending  map[int32]*jsWSPending
	closed   bool
	closeErr error

	streamID         int32
	recovering       chan struct{}
	maxStreamRetries int32
}

type jsWSResult struct {
	data RawMessage
	err  error
}

type jsWSPending struct {
	payload any
	ch      chan jsWSResult
	retries int32
}

func newJSWSConn(cfg *config) (*jsWSConn, error) {
	subproto := "hrana" + cfg.apiVersion[1:]
	if cfg.codec.IsBinary() {
		subproto += "-bin"
	}

	ws := js.Global().Get("WebSocket").New(cfg.baseURL, []any{subproto})
	// Crucial for Msgpack: instructs the browser to return binary frames as ArrayBuffers.
	ws.Set("binaryType", "arraybuffer")

	c := &jsWSConn{
		cfg:              cfg,
		codec:            cfg.codec,
		ws:               ws,
		pending:          make(map[int32]*jsWSPending),
		streamID:         0,
		maxStreamRetries: 3,
	}

	openCh := make(chan error, 1)

	onOpen := js.FuncOf(func(this js.Value, args []js.Value) any {
		select {
		case openCh <- nil:
		default:
		}
		return nil
	})
	c.cbs = append(c.cbs, onOpen)
	ws.Set("onopen", onOpen)

	onError := js.FuncOf(func(this js.Value, args []js.Value) any {
		select {
		case openCh <- fmt.Errorf("hrana: websocket connection failed"):
		default:
		}
		c.handleClose(fmt.Errorf("websocket error"))
		return nil
	})
	c.cbs = append(c.cbs, onError)
	ws.Set("onerror", onError)

	select {
	case err := <-openCh:
		if err != nil {
			c.cleanupCallbacks()
			return nil, err
		}
	case <-time.After(10 * time.Second):
		c.cleanupCallbacks()
		return nil, fmt.Errorf("hrana: ws connect timeout")
	}

	helloCh := make(chan jsWSResult, 1)
	isHello := true

	onMessage := js.FuncOf(func(this js.Value, args []js.Value) any {
		dataVal := args[0].Get("data")
		var data []byte

		// Smartly handle both Text (JSON) and Binary (Msgpack) frames
		if dataVal.Type() == js.TypeString {
			data = []byte(dataVal.String())
		} else {
			// It's an ArrayBuffer
			uint8Array := js.Global().Get("Uint8Array").New(dataVal)
			data = make([]byte, uint8Array.Length())
			js.CopyBytesToGo(data, uint8Array)
		}

		if isHello {
			isHello = false
			helloCh <- jsWSResult{data: data}
			return nil
		}
		c.handleMessage(data)
		return nil
	})
	c.cbs = append(c.cbs, onMessage)
	ws.Set("onmessage", onMessage)

	onClose := js.FuncOf(func(this js.Value, args []js.Value) any {
		c.handleClose(fmt.Errorf("websocket closed"))
		return nil
	})
	c.cbs = append(c.cbs, onClose)
	ws.Set("onclose", onClose)

	type helloMsg struct {
		Type string  `json:"type" msgpack:"type"`
		JWT  *string `json:"jwt,omitempty" msgpack:"jwt,omitempty"`
	}
	hello := helloMsg{Type: "hello"}
	if cfg.authToken != "" {
		hello.JWT = &cfg.authToken
	}

	if err := c.writeMessage(hello); err != nil {
		c.Close()
		return nil, fmt.Errorf("hrana: ws hello send: %w", err)
	}

	select {
	case res := <-helloCh:
		var helloResp struct {
			Type  string      `json:"type" msgpack:"type"`
			Error *hranaError `json:"error,omitempty" msgpack:"error,omitempty"`
		}
		if err := c.codec.Unmarshal(res.data, &helloResp); err != nil {
			c.Close()
			return nil, fmt.Errorf("hrana: ws hello decode: %w", err)
		}
		switch helloResp.Type {
		case "hello_ok":
			// ready
		case "hello_error":
			c.Close()
			if helloResp.Error != nil {
				return nil, fmt.Errorf("hrana: ws auth rejected: %s", helloResp.Error.Message)
			}
			return nil, fmt.Errorf("hrana: ws auth rejected")
		default:
			c.Close()
			return nil, fmt.Errorf("hrana: ws unexpected message %q during hello", helloResp.Type)
		}
	case <-time.After(10 * time.Second):
		c.Close()
		return nil, fmt.Errorf("hrana: ws hello timeout")
	}

	if err := c.openStream(context.Background()); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

// ─── driver.Conn ─────────────────────────────────────────────────────────────

func (c *jsWSConn) Prepare(query string) (driver.Stmt, error) {
	return &preparedStmt{conn: c, sql: query}, nil
}

func (c *jsWSConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.sendRequest(ctx, map[string]any{
		"type":      "close_stream",
		"stream_id": c.streamID,
	})

	c.ws.Call("close")
	return nil
}

func (c *jsWSConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ─── driver.ConnBeginTx ──────────────────────────────────────────────────────

func (c *jsWSConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
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

func (c *jsWSConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
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

func (c *jsWSConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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

func (c *jsWSConn) openStream(ctx context.Context) error {
	_, err := c.sendRequest(ctx, map[string]any{
		"type":      "open_stream",
		"stream_id": c.streamID,
	})
	if err != nil {
		return fmt.Errorf("hrana: ws open_stream: %w", err)
	}
	return nil
}

func (c *jsWSConn) execStatement(ctx context.Context, s *stmt) (*stmtResult, error) {
	resp, err := c.sendRequest(ctx, map[string]any{
		"type":      "execute",
		"stream_id": c.streamID,
		"stmt":      s,
	})
	if err != nil {
		return nil, err
	}

	var r struct {
		Type   string     `json:"type" msgpack:"type"`
		Result stmtResult `json:"result" msgpack:"result"`
	}
	if err := c.codec.Unmarshal(resp, &r); err != nil {
		return nil, fmt.Errorf("hrana: ws execute response: %w", err)
	}
	return &r.Result, nil
}

func (c *jsWSConn) sendRequest(ctx context.Context, payload any) (RawMessage, error) {
	return c.sendRequestWithRetries(ctx, payload, 0)
}

func (c *jsWSConn) sendRequestWithRetries(ctx context.Context, payload any, retries int32) (RawMessage, error) {
	id := atomic.AddInt32(&c.reqIDSeq, 1)

	ch := make(chan jsWSResult, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("hrana: ws connection is closed")
	}
	c.pending[id] = &jsWSPending{payload: payload, ch: ch, retries: retries}
	c.mu.Unlock()

	msg := struct {
		Type      string `json:"type" msgpack:"type"`
		RequestID int32  `json:"request_id" msgpack:"request_id"`
		Request   any    `json:"request" msgpack:"request"`
	}{
		Type:      "request",
		RequestID: id,
		Request:   payload,
	}

	if err := c.writeMessage(msg); err != nil {
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

// writeMessage marshals v using the codec and sends it over the JS WebSocket.
func (c *jsWSConn) writeMessage(v any) error {
	data, err := c.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("hrana: marshal: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.codec.IsBinary() {
		// Msgpack: Convert []byte to Uint8Array for binary transmission
		uint8Array := js.Global().Get("Uint8Array").New(len(data))
		js.CopyBytesToJS(uint8Array, data)
		c.ws.Call("send", uint8Array)
	} else {
		// JSON: Send as standard string/text frame
		c.ws.Call("send", string(data))
	}

	return nil
}

func (c *jsWSConn) handleMessage(data []byte) {
	var base struct {
		Type      string `json:"type" msgpack:"type"`
		RequestID int32  `json:"request_id" msgpack:"request_id"`
	}
	if err := c.codec.Unmarshal(data, &base); err != nil {
		return
	}

	switch base.Type {
	case "response_ok":
		var msg struct {
			RequestID int32      `json:"request_id" msgpack:"request_id"`
			Response  RawMessage `json:"response" msgpack:"response"`
		}
		if err := c.codec.Unmarshal(data, &msg); err != nil {
			return
		}
		c.dispatch(msg.RequestID, jsWSResult{data: msg.Response})

	case "response_error":
		var msg struct {
			RequestID int32      `json:"request_id" msgpack:"request_id"`
			Error     hranaError `json:"error" msgpack:"error"`
		}
		if err := c.codec.Unmarshal(data, &msg); err != nil {
			return
		}
		c.dispatch(msg.RequestID, jsWSResult{err: fmt.Errorf("hrana: %s", msg.Error.Message)})

	case "stream_superseded_error":
		var msg struct {
			RequestID int32      `json:"request_id" msgpack:"request_id"`
			Error     hranaError `json:"error" msgpack:"error"`
		}
		if err := c.codec.Unmarshal(data, &msg); err != nil {
			return
		}
		c.handleStreamSuperseded(msg.RequestID, msg.Error.Message)
	}
}

func (c *jsWSConn) handleStreamSuperseded(requestID int32, message string) {
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

func (c *jsWSConn) retrySupersededRequest(p *jsWSPending, message string) {
	payloadMap, ok := p.payload.(map[string]any)
	if !ok {
		p.ch <- jsWSResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}
	if _, hasStreamID := payloadMap["stream_id"]; !hasStreamID {
		p.ch <- jsWSResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}
	if p.retries >= c.maxStreamRetries {
		p.ch <- jsWSResult{err: fmt.Errorf("hrana: %s", message)}
		return
	}

	if err := c.recoverStream(); err != nil {
		p.ch <- jsWSResult{err: err}
		return
	}

	payloadMap["stream_id"] = c.streamID
	result, err := c.sendRequestWithRetries(context.Background(), payloadMap, p.retries+1)
	if err != nil {
		p.ch <- jsWSResult{err: err}
		return
	}
	p.ch <- jsWSResult{data: result}
}

func (c *jsWSConn) recoverStream() error {
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
		Type     string `json:"type" msgpack:"type"`
		StreamID int32  `json:"stream_id" msgpack:"stream_id"`
	}
	if err := c.codec.Unmarshal(resp, &assigned); err != nil {
		return fmt.Errorf("hrana: ws open_stream_assigned decode: %w", err)
	}
	c.streamID = assigned.StreamID
	return nil
}

func (c *jsWSConn) handleClose(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeErr = err
	for id, p := range c.pending {
		p.ch <- jsWSResult{err: err}
		delete(c.pending, id)
	}
	c.mu.Unlock()

	go c.cleanupCallbacks()
}

func (c *jsWSConn) cleanupCallbacks() {
	for _, cb := range c.cbs {
		cb.Release()
	}
	c.cbs = nil
}

func (c *jsWSConn) dispatch(id int32, result jsWSResult) {
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
