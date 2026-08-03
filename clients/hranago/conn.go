package hranago

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// stmtExecutor is implemented by both conn (HTTP) and wsConn (WebSocket).
type stmtExecutor interface {
	execStatement(ctx context.Context, s *stmt) (*stmtResult, error)
}

// conn is a single Hrana connection backed by an HTTP pipeline stream.
// Each conn maintains a baton that identifies the server-side stream.
//
// database/sql guarantees a Conn is used by only one goroutine at a time,
// but we hold a mutex around baton access for safety during Close.
type conn struct {
	cfg              *config
	httpClient       *http.Client
	wsConnectHeaders http.Header
	mu               sync.Mutex
	baton            *string // nil until first request; rotates on every pipeline call
	baseURL          string  // may be updated by base_url in pipeline responses
	closed           bool
}

func newConn(cfg *config) *conn {
	return &conn{
		cfg:              cfg,
		httpClient:       &http.Client{},
		wsConnectHeaders: make(http.Header),
		baseURL:          cfg.baseURL,
	}
}

// ─── driver.Conn ─────────────────────────────────────────────────────────────

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &preparedStmt{conn: c, sql: query}, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	baton := c.baton
	c.mu.Unlock()

	if baton == nil {
		// Stream was never opened on the server; nothing to clean up.
		return nil
	}

	// Best-effort close: send a "close" request to release the server-side stream.
	_, err := c.sendPipeline(context.Background(), []streamRequest{{Type: "close"}})
	return err
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ─── driver.ConnBeginTx ──────────────────────────────────────────────────────

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
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

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
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

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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

// ─── Internal helpers ────────────────────────────────────────────────────────

// execStatement sends a single execute request through the pipeline.
func (c *conn) execStatement(ctx context.Context, s *stmt) (*stmtResult, error) {
	resp, err := c.sendPipeline(ctx, []streamRequest{{Type: "execute", Stmt: s}})
	if err != nil {
		return nil, err
	}

	if len(resp.Results) == 0 {
		return nil, fmt.Errorf("hrana: empty pipeline response")
	}

	r := resp.Results[0]
	if r.Type == "error" {
		if r.Error != nil {
			return nil, fmt.Errorf("hrana: %s", r.Error.Message)
		}
		return nil, fmt.Errorf("hrana: unknown error in pipeline response")
	}

	if r.Response == nil || r.Response.Result == nil {
		return nil, fmt.Errorf("hrana: missing result in pipeline response")
	}

	return r.Response.Result, nil
}

// sendPipeline posts a pipeline request to the server and returns the response.
// It reads and rotates the baton automatically.
func (c *conn) sendPipeline(ctx context.Context, requests []streamRequest) (*pipelineResp, error) {
	c.mu.Lock()
	baton := c.baton
	baseURL := c.baseURL
	c.mu.Unlock()

	body := pipelineReq{
		Baton:    baton,
		Requests: requests,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("hrana: marshal request: %w", err)
	}

	endpoint := baseURL + "/" + c.cfg.apiVersion + "/pipeline"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("hrana: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.cfg.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.authToken)
	}

	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hrana: http error: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(httpResp.Body).Decode(&errBody)
		if errBody.Error != "" {
			return nil, fmt.Errorf("hrana: server returned %d: %s", httpResp.StatusCode, errBody.Error)
		}
		return nil, fmt.Errorf("hrana: server returned %d", httpResp.StatusCode)
	}

	var pResp pipelineResp
	if err := json.NewDecoder(httpResp.Body).Decode(&pResp); err != nil {
		return nil, fmt.Errorf("hrana: decode response: %w", err)
	}

	// Rotate baton and optionally follow base_url redirect.
	c.mu.Lock()
	c.baton = pResp.Baton
	if pResp.BaseURL != nil && *pResp.BaseURL != "" {
		c.baseURL = *pResp.BaseURL
	}
	c.mu.Unlock()

	return &pResp, nil
}
