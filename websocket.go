package hrana

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"lowbit.dev/websockets"
)

// ServeConn performs the WebSocket handshake on a raw TCP connection and then
// runs the Hrana WebSocket message loop. The connection must not have had any
// data read from it yet; ServeConn reads the HTTP upgrade request itself.
func (s *Server) ServeConn(conn net.Conn) {
	defer conn.Close()
	if s.closing.Load() {
		writeRawHTTP(conn, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
		return
	}

	// bufio.Reader is used only to parse the HTTP upgrade headers. WebSocket
	// clients do not send frame data before receiving the 101 response, so the
	// read buffer will be empty before we hand the raw conn to the WS package.
	brdr := bufio.NewReader(conn)
	req, err := readHTTPRequest(brdr)
	if err != nil {
		return
	}

	proto := matchSubprotocol(s.enabledSubprotocols(), req.header("sec-websocket-protocol"))
	if proto == "" {
		writeRawHTTP(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
		return
	}

	mode, err := parseMode(req.queryParam("mode"))
	if err != nil {
		writeRawHTTP(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
		return
	}

	accept := wsAccept(req.header("sec-websocket-key"))
	writeRawHTTP(conn,
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: "+accept+"\r\n"+
			"Sec-WebSocket-Protocol: "+proto+"\r\n\r\n")

	wsConn := s.wsConnPool.Acquire(conn, websockets.ReadLimitStandard, websockets.FrameSizeBalanced)
	wsConn.AssumeServerRole()
	wsConn.SetSubprotocol(proto)
	defer s.wsConnPool.Release(wsConn)
	defer wsConn.Close()

	s.runWSSession(wsConn, proto, mode, conn.RemoteAddr().String())
}

// serveWSUpgrade handles a WebSocket upgrade that arrived via ServeHTTP.
// The Acceptor performs validation, subprotocol negotiation, hijacking, and
// the 101 handshake, returning a ready-to-use Connection.
func (s *Server) serveWSUpgrade(w http.ResponseWriter, r *http.Request) {
	if s.closing.Load() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wsConn, err := s.wsAcceptor.Accept(w, r)
	if err != nil {
		return // Acceptor has already written the HTTP error response.
	}
	defer s.wsAcceptor.Release(wsConn)
	defer wsConn.Close()

	s.runWSSession(wsConn, wsConn.Subprotocol(), mode, r.RemoteAddr)
}

// ─── Codec helpers ───────────────────────────────────────────────────────────

// codecForProto returns the Codec and WebSocket frame opcode for a given
// subprotocol. The "-bin" variants use MsgpackCodec over binary frames;
// all others use JSONCodec over text frames.
func codecForProto(proto string) (Codec, websockets.OpCode) {
	if strings.HasSuffix(proto, "-bin") {
		return MsgpackCodec{}, websockets.OpCodeBinary
	}
	return JSONCodec{}, websockets.OpCodeText
}

// extractWSRequest pulls the request_id and raw inner request bytes from the
// full WS message payload using the session's codec.
//
// For msgpack we decode Request into any and immediately re-marshal it. This
// is necessary because msgpack.RawMessage.DecodeMsgpack does not preserve the
// outer type-prefix byte, so the captured bytes are not a valid standalone
// msgpack document that can be Unmarshal-ed on their own.
func extractWSRequest(payload []byte, codec Codec) (int32, []byte, error) {
	switch codec.(type) {
	case JSONCodec:
		var env RequestMsg
		if err := json.Unmarshal(payload, &env); err != nil {
			return 0, nil, err
		}
		return env.RequestID, env.Request, nil
	case MsgpackCodec:
		var env struct {
			Type      string `msgpack:"type"`
			RequestID int32  `msgpack:"request_id"`
			Request   any    `msgpack:"request"`
		}
		if err := msgpack.Unmarshal(payload, &env); err != nil {
			return 0, nil, err
		}
		// Re-marshal the decoded inner value so dispatchWSRequest receives a
		// self-contained msgpack document it can Unmarshal independently.
		inner, err := msgpack.Marshal(env.Request)
		if err != nil {
			return 0, nil, err
		}
		return env.RequestID, inner, nil
	default:
		return 0, nil, fmt.Errorf("hrana: unsupported codec type")
	}
}

// runWSSession is the shared Hrana WebSocket message loop entered after the
// 101 handshake has been completed and a Connection is ready.
func (s *Server) runWSSession(wsConn websockets.Connection, proto string, mode ConnectionMode, remoteAddr string) {
	s.wsLog.Debug("ws session started", slog.String("remote", remoteAddr), slog.String("proto", proto))

	s.wsMu.Lock()
	s.wsConns[wsConn] = struct{}{}
	s.wsMu.Unlock()

	s.wg.Add(1)
	atomic.AddInt64(&s.wsConnCount, 1)
	defer func() {
		s.wsMu.Lock()
		delete(s.wsConns, wsConn)
		s.wsMu.Unlock()
		s.wg.Done()
		s.wsLog.Debug("ws session ended", slog.String("remote", remoteAddr), slog.String("proto", proto))
		if atomic.AddInt64(&s.wsConnCount, -1) == 0 && s.config.OnLastWSDisconnect != nil {
			s.config.OnLastWSDisconnect()
		}
	}()

	sess := newSession(mode)
	defer sess.closeAll()

	codec, opCode := codecForProto(proto)
	authed := false

	// Reuse a single read buffer for the lifetime of the session to avoid
	// per-message allocations. ReadMessage slices into this buffer directly.
	readBuf := make([]byte, 0, websockets.ReadLimitStandard)

	sendMsg := func(msg any) error {
		data, err := codec.Encode(msg)
		if err != nil {
			s.wsLog.Error("failed to encode payload", "error", err)
			return err
		}
		if err := wsConn.StreamMessage(opCode, websockets.FrameSizeBalanced-12, bytes.NewReader(data)); err != nil {
			s.wsLog.Error("failed to send message", "error", err)
		}
		return nil
	}

	for {
		readBuf = readBuf[:0]
		payload, op, err := wsConn.ReadMessage(readBuf)
		if err != nil {
			s.wsLog.Error("failed to read message", "error", err)
			return
		}
		if op != opCode {
			s.wsLog.Error("received unexpected frame opcode", "op", op, "expected", opCode)
			return
		}

		var base struct {
			Type string `json:"type" msgpack:"type"`
		}
		if err := codec.Decode(payload, &base); err != nil {
			s.wsLog.Error("failed to decode message type", "error", err)
			return
		}

		switch base.Type {
		case "hello":
			var msg HelloMsg
			if err := codec.Decode(payload, &msg); err != nil {
				s.wsLog.Error("Failed to decode payload for hello", "error", err)
				return
			}

			token := ""
			if msg.JWT != nil {
				token = *msg.JWT
			}

			expiry, authErr := s.config.AuthFunc(token)
			if authErr != nil {
				s.wsLog.Debug("ws auth rejected", slog.String("remote", remoteAddr), slog.String("error", authErr.Error()))
				_ = sendMsg(HelloErrorMsg{Type: "hello_error", Error: Error{Message: authErr.Error()}})
				return
			}

			if expiry != nil {
				_ = wsConn.SetReadDeadline(*expiry)
			} else {
				_ = wsConn.SetReadDeadline(time.Time{})
			}

			s.wsLog.Debug("ws auth accepted", slog.String("remote", remoteAddr))
			authed = true
			_ = sendMsg(HelloOkMsg{Type: "hello_ok"})

		case "request":
			if !authed {
				return
			}
			if s.closing.Load() {
				return
			}
			requestID, rawReq, err := extractWSRequest(payload, codec)
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func(id int32, raw []byte) {
				defer s.wg.Done()
				s.wsLog.Debug("ws request dispatched", slog.Int64("request_id", int64(id)), slog.String("remote", remoteAddr))
				resp, respErr := s.dispatchWSRequest(id, raw, codec, sess, proto)
				if respErr != nil {
					_ = sendMsg(ResponseErrorMsg{
						Type:      "response_error",
						RequestID: id,
						Error:     Error{Message: respErr.Error()},
					})
					return
				}
				_ = sendMsg(ResponseOkMsg{
					Type:      "response_ok",
					RequestID: id,
					Response:  resp,
				})
			}(requestID, rawReq)

		default:
			return
		}
	}
}

// dispatchWSRequest routes a decoded WS request to the appropriate handler.
// rawReq contains the raw bytes of the inner request object (not the full
// envelope), encoded with the session's codec.
func (s *Server) dispatchWSRequest(requestID int32, rawReq []byte, codec Codec, sess *Session, proto string) (any, error) {
	var base struct {
		Type string `json:"type" msgpack:"type"`
	}
	if err := codec.Decode(rawReq, &base); err != nil {
		return nil, fmt.Errorf("hrana: invalid request payload")
	}

	s.wsLog.Debug("ws request type", slog.Int64("request_id", int64(requestID)), slog.String("type", base.Type))

	switch base.Type {
	case "open_stream":
		var req OpenStreamReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsOpenStream(sess, req)

	case "close_stream":
		var req CloseStreamReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsCloseStream(sess, req)

	case "execute":
		var req ExecuteReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsExecute(sess, req)

	case "batch":
		var req BatchReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsBatch(sess, req)

	case "store_sql":
		var req StoreSqlReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsStoreSQL(sess, req)

	case "close_sql":
		var req CloseSqlReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		sess.closeSQL(req.SQLID)
		return CloseSqlResp{Type: "close_sql"}, nil

	case "sequence":
		var req SequenceReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsSequence(sess, req)

	case "describe":
		var req DescribeReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsDescribe(sess, req)

	case "get_autocommit":
		var req GetAutocommitReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsGetAutocommit(sess, req)

	case "open_cursor":
		if proto != "hrana3" && proto != "hrana3-bin" {
			return nil, fmt.Errorf("hrana: open_cursor requires hrana3")
		}
		var req OpenCursorReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsOpenCursor(sess, req)

	case "close_cursor":
		var req CloseCursorReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsCloseCursor(sess, req)

	case "fetch_cursor":
		var req FetchCursorReq
		if err := codec.Decode(rawReq, &req); err != nil {
			return nil, err
		}
		return s.wsFetchCursor(sess, req)

	default:
		return nil, fmt.Errorf("hrana: unknown request type %q", base.Type)
	}
}

// ─── WS request handlers ──────────────────────────────────────────────────────

func (s *Server) wsOpenStream(sess *Session, req OpenStreamReq) (any, error) {
	if _, exists := sess.getStream(req.StreamID); exists {
		return nil, fmt.Errorf("hrana: stream_id %d already open", req.StreamID)
	}
	stream, err := s.OpenStream(s.ctx)
	if err != nil {
		return nil, err
	}
	stream.Mode = sess.Mode
	sess.addStream(req.StreamID, stream)
	s.wsLog.Debug("ws stream opened", slog.Int("stream_id", int(req.StreamID)))
	return OpenStreamResp{Type: "open_stream"}, nil
}

func (s *Server) wsCloseStream(sess *Session, req CloseStreamReq) (any, error) {
	sess.removeStream(req.StreamID)
	s.wsLog.Debug("ws stream closed", slog.Int("stream_id", int(req.StreamID)))
	return CloseStreamResp{Type: "close_stream"}, nil
}

func (s *Server) wsExecute(sess *Session, req ExecuteReq) (any, error) {
	stream, ok := sess.getStream(req.StreamID)
	if !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	stream.Lock()
	defer stream.Unlock()
	result, err := executeStmt(s.ctx, stream, &req.Stmt, sess.storedSQL)
	if err != nil {
		s.wsLog.Debug("ws execute failed", slog.Int("stream_id", int(req.StreamID)), slog.String("error", err.Error()))
		return nil, err
	}
	s.wsLog.Debug("ws execute ok", slog.Int("stream_id", int(req.StreamID)), slog.Uint64("rows_affected", result.AffectedRowCount), slog.Float64("duration_ms", result.QueryDurationMs))
	return ExecuteResp{Type: "execute", Result: *result}, nil
}

func (s *Server) wsBatch(sess *Session, req BatchReq) (any, error) {
	stream, ok := sess.getStream(req.StreamID)
	if !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	stream.Lock()
	defer stream.Unlock()
	result, err := executeBatch(s.ctx, stream, &req.Batch, sess.storedSQL)
	if err != nil {
		s.wsLog.Debug("ws batch failed", slog.Int("stream_id", int(req.StreamID)), slog.String("error", err.Error()))
		return nil, err
	}
	s.wsLog.Debug("ws batch ok", slog.Int("stream_id", int(req.StreamID)), slog.Int("steps", len(result.StepResults)))
	return BatchResp{Type: "batch", Result: *result}, nil
}

func (s *Server) wsStoreSQL(sess *Session, req StoreSqlReq) (any, error) {
	if err := sess.storeSQL(req.SQLID, req.SQL); err != nil {
		return nil, err
	}
	s.wsLog.Debug("ws sql stored", slog.Int("sql_id", int(req.SQLID)))
	return StoreSqlResp{Type: "store_sql"}, nil
}

func (s *Server) wsSequence(sess *Session, req SequenceReq) (any, error) {
	stream, ok := sess.getStream(req.StreamID)
	if !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	sqlText, err := sess.resolveSQL(req.SQL, req.SQLId)
	if err != nil {
		return nil, err
	}
	stream.Lock()
	defer stream.Unlock()
	if err := executeSequence(s.ctx, stream, sqlText); err != nil {
		return nil, err
	}
	return SequenceResp{Type: "sequence"}, nil
}

func (s *Server) wsDescribe(sess *Session, req DescribeReq) (any, error) {
	stream, ok := sess.getStream(req.StreamID)
	if !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	sqlText, err := sess.resolveSQL(req.SQL, req.SQLId)
	if err != nil {
		return nil, err
	}
	stream.Lock()
	defer stream.Unlock()
	result, err := describeStmt(s.ctx, stream, sqlText)
	if err != nil {
		return nil, err
	}
	return DescribeResp{Type: "describe", Result: *result}, nil
}

func (s *Server) wsGetAutocommit(sess *Session, req GetAutocommitReq) (any, error) {
	if _, ok := sess.getStream(req.StreamID); !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	return GetAutocommitResp{Type: "get_autocommit", IsAutocommit: true}, nil
}

func (s *Server) wsOpenCursor(sess *Session, req OpenCursorReq) (any, error) {
	stream, ok := sess.getStream(req.StreamID)
	if !ok {
		return nil, fmt.Errorf("hrana: stream_id %d not found", req.StreamID)
	}
	stream.Lock()
	entries, err := executeBatchCursor(s.ctx, stream, &req.Batch, sess.storedSQL)
	stream.Unlock()
	if err != nil {
		return nil, err
	}
	cursor := newCursor(req.CursorID, entries)
	stream.Lock()
	stream.Cursors[req.CursorID] = cursor
	stream.Unlock()
	s.wsLog.Debug("ws cursor opened", slog.Int("cursor_id", int(req.CursorID)), slog.Int("stream_id", int(req.StreamID)))
	return OpenCursorResp{Type: "open_cursor"}, nil
}

func (s *Server) wsCloseCursor(sess *Session, req CloseCursorReq) (any, error) {
	sess.mu.Lock()
	for _, st := range sess.streams {
		st.Lock()
		delete(st.Cursors, req.CursorID)
		st.Unlock()
	}
	sess.mu.Unlock()
	s.wsLog.Debug("ws cursor closed", slog.Int("cursor_id", int(req.CursorID)))
	return CloseCursorResp{Type: "close_cursor"}, nil
}

func (s *Server) wsFetchCursor(sess *Session, req FetchCursorReq) (any, error) {
	sess.mu.Lock()
	var cursor *Cursor
	for _, st := range sess.streams {
		st.Lock()
		c, ok := st.Cursors[req.CursorID]
		st.Unlock()
		if ok {
			cursor = c
			break
		}
	}
	sess.mu.Unlock()

	if cursor == nil {
		return nil, fmt.Errorf("hrana: cursor_id %d not found", req.CursorID)
	}

	entries, done := cursor.Fetch(req.MaxCount)
	return FetchCursorResp{Type: "fetch_cursor", Entries: entries, Done: done}, nil
}

// closeAllWSConns sends a WebSocket 1001 Going Away close frame to every active
// connection. CloseWithCode closes the underlying TCP socket, which also
// unblocks any pending ReadMessage calls in the session goroutines.
func (s *Server) closeAllWSConns(_ string) {
	s.wsMu.Lock()
	conns := make([]websockets.Connection, 0, len(s.wsConns))
	for c := range s.wsConns {
		conns = append(conns, c)
	}
	s.wsMu.Unlock()

	for _, c := range conns {
		_ = c.(*websockets.Conn).CloseWithCode(websockets.CloseGoingAway)
	}
}

// matchSubprotocol returns the first entry in serverProtos that the client
// offered, preserving server-side priority order.
func matchSubprotocol(serverProtos []string, clientHeader string) string {
	for _, server := range serverProtos {
		for _, offered := range strings.Split(clientHeader, ",") {
			if strings.TrimSpace(offered) == server {
				return server
			}
		}
	}
	return ""
}
