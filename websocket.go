package hrana

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
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
	brw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	req, err := readHTTPRequest(brw.Reader)
	if err != nil {
		return
	}

	proto := s.negotiateSubprotocol(req.header("Sec-Websocket-Protocol"))
	if proto == "" {
		writeRawHTTP(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
		return
	}

	mode, err := parseMode(req.queryParam("mode"))
	if err != nil {
		writeRawHTTP(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
		return
	}

	key := req.header("Sec-Websocket-Key")
	if err := writeWSHandshake(conn, key, proto); err != nil {
		return
	}

	s.runWSSession(conn, brw.Reader, proto, mode)
}

// serveWSUpgrade handles a WebSocket upgrade that arrived via ServeHTTP.
// The http.Request has already been parsed by net/http, so we hijack the
// connection, write the 101 handshake, and enter the session loop.
func (s *Server) serveWSUpgrade(w http.ResponseWriter, r *http.Request) {
	if s.closing.Load() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket upgrade not supported", http.StatusInternalServerError)
		return
	}

	proto := s.negotiateSubprotocol(r.Header.Get("Sec-WebSocket-Protocol"))
	if proto == "" {
		http.Error(w, "no supported Hrana subprotocol", http.StatusBadRequest)
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, brw, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	key := r.Header.Get("Sec-WebSocket-Key")
	if err := writeWSHandshake(conn, key, proto); err != nil {
		return
	}

	s.runWSSession(conn, brw.Reader, proto, mode)
}

// runWSSession is the shared Hrana WebSocket message loop. It is entered after
// the HTTP 101 handshake has already been written to conn.
func (s *Server) runWSSession(conn net.Conn, r *bufio.Reader, proto string, mode ConnectionMode) {
	remoteAddr := conn.RemoteAddr().String()
	s.wsLog.Debug("ws session started", slog.String("remote", remoteAddr), slog.String("proto", proto))

	s.wsMu.Lock()
	s.wsConns[conn] = struct{}{}
	s.wsMu.Unlock()

	s.wg.Add(1)
	atomic.AddInt64(&s.wsConnCount, 1)
	defer func() {
		s.wsMu.Lock()
		delete(s.wsConns, conn)
		s.wsMu.Unlock()
		s.wg.Done()
		s.wsLog.Debug("ws session ended", slog.String("remote", remoteAddr), slog.String("proto", proto))
		if atomic.AddInt64(&s.wsConnCount, -1) == 0 && s.config.OnLastWSDisconnect != nil {
			s.config.OnLastWSDisconnect()
		}
	}()

	sess := newSession(mode)
	defer sess.closeAll()

	authed := false

	var writeMu sync.Mutex
	sendMsg := func(msg any) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeWSTextFrame(conn, data)
	}

	for {
		data, err := readWSTextFrame(r)
		if err != nil {
			return
		}

		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &base); err != nil {
			return
		}

		switch base.Type {
		case "hello":
			var msg HelloMsg
			if err := json.Unmarshal(data, &msg); err != nil {
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
				_ = conn.SetReadDeadline(*expiry)
			} else {
				_ = conn.SetReadDeadline(time.Time{})
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
			var reqMsg RequestMsg
			if err := json.Unmarshal(data, &reqMsg); err != nil {
				return
			}
			s.wg.Add(1)
			go func(rm RequestMsg) {
				defer s.wg.Done()
				s.wsLog.Debug("ws request dispatched", slog.Int64("request_id", int64(rm.RequestID)), slog.String("remote", remoteAddr))
				resp, respErr := s.dispatchWSRequest(rm, sess, proto)
				if respErr != nil {
					_ = sendMsg(ResponseErrorMsg{
						Type:      "response_error",
						RequestID: rm.RequestID,
						Error:     Error{Message: respErr.Error()},
					})
					return
				}
				_ = sendMsg(ResponseOkMsg{
					Type:      "response_ok",
					RequestID: rm.RequestID,
					Response:  resp,
				})
			}(reqMsg)

		default:
			return
		}
	}
}

// dispatchWSRequest routes a decoded WS request to the appropriate handler.
func (s *Server) dispatchWSRequest(rm RequestMsg, sess *Session, proto string) (any, error) {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rm.Request, &base); err != nil {
		return nil, fmt.Errorf("hrana: invalid request payload")
	}

	s.wsLog.Debug("ws request type", slog.Int64("request_id", int64(rm.RequestID)), slog.String("type", base.Type))

	switch base.Type {
	case "open_stream":
		var req OpenStreamReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsOpenStream(sess, req)

	case "close_stream":
		var req CloseStreamReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsCloseStream(sess, req)

	case "execute":
		var req ExecuteReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsExecute(sess, req)

	case "batch":
		var req BatchReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsBatch(sess, req)

	case "store_sql":
		var req StoreSqlReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsStoreSQL(sess, req)

	case "close_sql":
		var req CloseSqlReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		sess.closeSQL(req.SQLID)
		return CloseSqlResp{Type: "close_sql"}, nil

	case "sequence":
		var req SequenceReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsSequence(sess, req)

	case "describe":
		var req DescribeReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsDescribe(sess, req)

	case "get_autocommit":
		var req GetAutocommitReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsGetAutocommit(sess, req)

	case "open_cursor":
		if proto != "hrana3" {
			return nil, fmt.Errorf("hrana: open_cursor requires hrana3")
		}
		var req OpenCursorReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsOpenCursor(sess, req)

	case "close_cursor":
		var req CloseCursorReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
			return nil, err
		}
		return s.wsCloseCursor(sess, req)

	case "fetch_cursor":
		var req FetchCursorReq
		if err := json.Unmarshal(rm.Request, &req); err != nil {
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

// closeAllWSConns sends a WebSocket close frame (1001 Going Away) to every
// active WS connection and sets their read deadline to now so that blocked
// readWSTextFrame calls return immediately.
func (s *Server) closeAllWSConns(reason string) {
	s.wsMu.Lock()
	conns := make([]net.Conn, 0, len(s.wsConns))
	for c := range s.wsConns {
		conns = append(conns, c)
	}
	s.wsMu.Unlock()

	for _, c := range conns {
		_ = writeWSCloseFrame(c, 1001, reason)
		_ = c.SetReadDeadline(time.Now())
	}
}

// writeWSCloseFrame sends a WebSocket close frame with the given status code
// and reason string per RFC 6455 §5.5.1.
func writeWSCloseFrame(conn net.Conn, code uint16, reason string) error {
	payload := make([]byte, 2+len(reason))
	payload[0] = byte(code >> 8)
	payload[1] = byte(code)
	copy(payload[2:], reason)

	length := len(payload)
	var header []byte
	header = append(header, 0x88) // FIN=1, opcode=8 (close)
	if length <= 125 {
		header = append(header, byte(length))
	} else {
		header = append(header, 126, byte(length>>8), byte(length))
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

// ─── Minimal WebSocket framing ────────────────────────────────────────────────

func writeWSHandshake(conn net.Conn, key, proto string) error {
	accept := wsAccept(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n" +
		"Sec-WebSocket-Protocol: " + proto + "\r\n\r\n"
	_, err := fmt.Fprint(conn, resp)
	return err
}

func writeWSTextFrame(conn net.Conn, payload []byte) error {
	length := len(payload)
	var header []byte
	header = append(header, 0x81) // FIN=1, opcode=1 (text)
	if length <= 125 {
		header = append(header, byte(length))
	} else if length <= 65535 {
		header = append(header, 126, byte(length>>8), byte(length))
	} else {
		header = append(header, 127,
			0, 0, 0, 0,
			byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func readWSTextFrame(r *bufio.Reader) ([]byte, error) {
	var payload []byte
	for {
		b0, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		b1, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		fin := b0&0x80 != 0
		opcode := b0 & 0x0f
		masked := b1&0x80 != 0
		length := int(b1 & 0x7f)

		if length == 126 {
			buf := make([]byte, 2)
			if _, err := readFull(r, buf); err != nil {
				return nil, err
			}
			length = int(buf[0])<<8 | int(buf[1])
		} else if length == 127 {
			buf := make([]byte, 8)
			if _, err := readFull(r, buf); err != nil {
				return nil, err
			}
			length = int(buf[4])<<24 | int(buf[5])<<16 | int(buf[6])<<8 | int(buf[7])
		}

		var mask [4]byte
		if masked {
			if _, err := readFull(r, mask[:]); err != nil {
				return nil, err
			}
		}

		data := make([]byte, length)
		if _, err := readFull(r, data); err != nil {
			return nil, err
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}

		if opcode == 8 {
			return nil, fmt.Errorf("hrana: websocket closed by client")
		}

		payload = append(payload, data...)
		if fin {
			return payload, nil
		}
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// negotiateSubprotocol picks the highest enabled Hrana version that the client offered.
func (s *Server) negotiateSubprotocol(offered string) string {
	for _, preferred := range []string{"hrana3", "hrana2", "hrana1"} {
		// Map "hranaX" to "vX" for the version-enabled check.
		if !s.isVersionEnabled("v" + preferred[len("hrana"):]) {
			continue
		}
		for _, o := range splitAndTrim(offered, ",") {
			if o == preferred {
				return preferred
			}
		}
	}
	return ""
}

func splitAndTrim(s, sep string) []string {
	var out []string
	for _, part := range splitString(s, sep) {
		t := trimSpace(part)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func splitString(s, sep string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			start = i + len(sep)
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
