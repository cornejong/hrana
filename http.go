package hrana

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// ServeHTTP routes incoming HTTP requests to the appropriate Hrana handler.
// If the request is a WebSocket upgrade it is handled as a Hrana WebSocket
// connection on the same path, following the sqld convention of accepting WS
// on any path (typically "/").
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpLog.Debug("http request received", slog.String("method", r.Method), slog.String("path", r.URL.Path))

	if s.closing.Load() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}

	if s.setCORSHeaders(w, r) {
		return
	}

	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.serveWSUpgrade(w, r)
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/execute" && s.isVersionEnabled("v1"):
		s.handleV1Execute(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/batch" && s.isVersionEnabled("v1"):
		s.handleV1Batch(w, r)
	case r.Method == http.MethodGet && (r.URL.Path == "/v2" || r.URL.Path == "/v2/") && s.isVersionEnabled("v2"):
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Path == "/v2/pipeline" && s.isVersionEnabled("v2"):
		s.handlePipeline(w, r)
	case r.Method == http.MethodGet && (r.URL.Path == "/v3" || r.URL.Path == "/v3/") && s.isVersionEnabled("v3"):
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Path == "/v3/pipeline" && s.isVersionEnabled("v3"):
		s.handlePipeline(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v3/cursor" && s.isVersionEnabled("v3"):
		s.handleV3Cursor(w, r)
	default:
		http.NotFound(w, r)
	}
}

// ─── V1 handlers ─────────────────────────────────────────────────────────────

func (s *Server) handleV1Execute(w http.ResponseWriter, r *http.Request) {
	if err := s.checkAuth(r); err != nil {
		writeHTTPError(w, http.StatusUnauthorized, err.Error())
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}

	var body V1ExecuteReqBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	stream, err := s.OpenStream(r.Context())
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer stream.Close()
	stream.Mode = mode

	result, err := executeStmt(r.Context(), stream, &body.Stmt, stream.StoredSQL)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.httpLog.Debug("http v1 execute ok", slog.Uint64("rows_affected", result.AffectedRowCount), slog.Float64("duration_ms", result.QueryDurationMs))
	writeJSON(w, http.StatusOK, V1ExecuteRespBody{Result: *result})
}

func (s *Server) handleV1Batch(w http.ResponseWriter, r *http.Request) {
	if err := s.checkAuth(r); err != nil {
		writeHTTPError(w, http.StatusUnauthorized, err.Error())
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}

	var body V1BatchReqBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	stream, err := s.OpenStream(r.Context())
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer stream.Close()
	stream.Mode = mode

	result, err := executeBatch(r.Context(), stream, &body.Batch, stream.StoredSQL)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.httpLog.Debug("http v1 batch ok", slog.Int("steps", len(result.StepResults)))
	writeJSON(w, http.StatusOK, V1BatchRespBody{Result: *result})
}

// ─── V2/V3 pipeline handler ──────────────────────────────────────────────────

func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	if err := s.checkAuth(r); err != nil {
		writeHTTPError(w, http.StatusUnauthorized, err.Error())
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}

	var body PipelineReqBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Resolve or create the stream.
	var stream *Stream
	var oldBaton string

	if body.Baton != nil && *body.Baton != "" {
		oldBaton = *body.Baton
		st, err := s.batons.Get(oldBaton)
		if err != nil || st == nil {
			writeHTTPError(w, http.StatusBadRequest, "baton not found or expired")
			return
		}
		s.httpLog.Debug("http pipeline resolved baton", slog.String("path", r.URL.Path))
		stream = st
	} else {
		st, err := s.OpenStream(r.Context())
		if err != nil {
			writeHTTPError(w, http.StatusInternalServerError, err.Error())
			return
		}
		st.Mode = mode
		s.httpLog.Debug("http pipeline opened new stream", slog.String("path", r.URL.Path))
		stream = st
	}

	results, closed := s.executePipelineRequests(r, stream, body.Requests)

	// Rotate baton unless the stream was closed by a "close" request.
	var newBaton *string
	if !closed {
		baton, err := s.batons.Put(stream, oldBaton)
		if err != nil {
			stream.Close()
			writeHTTPError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.httpLog.Debug("http pipeline baton rotated", slog.String("path", r.URL.Path), slog.Int("results", len(results)))
		newBaton = &baton
	} else {
		s.httpLog.Debug("http pipeline stream closed", slog.String("path", r.URL.Path))
		stream.Close()
		if oldBaton != "" {
			s.batons.Delete(oldBaton)
		}
	}

	writeJSON(w, http.StatusOK, PipelineRespBody{
		Baton:   newBaton,
		BaseURL: nil,
		Results: results,
	})
}

func (s *Server) executePipelineRequests(r *http.Request, stream *Stream, reqs []StreamRequest) ([]StreamResult, bool) {
	results := make([]StreamResult, len(reqs))
	closed := false

	for i, req := range reqs {
		if closed {
			results[i] = errorStreamResult("stream is closed")
			continue
		}

		resp, err := s.dispatchStreamRequest(r, stream, req)
		if err != nil {
			results[i] = errorStreamResult(err.Error())
		} else {
			results[i] = StreamResult{Type: "ok", Response: resp}
		}

		if req.Type == "close" {
			closed = true
		}
	}

	return results, closed
}

func (s *Server) dispatchStreamRequest(r *http.Request, stream *Stream, req StreamRequest) (*StreamResponse, error) {
	ctx := r.Context()
	s.httpLog.Debug("http stream request", slog.String("type", req.Type), slog.String("path", r.URL.Path))
	stream.Lock()
	defer stream.Unlock()

	switch req.Type {
	case "close":
		return &StreamResponse{Type: "close"}, nil

	case "execute":
		if req.Execute == nil {
			return nil, fmt.Errorf("hrana: missing execute payload")
		}
		result, err := executeStmt(ctx, stream, &req.Execute.Stmt, stream.StoredSQL)
		if err != nil {
			return nil, err
		}
		return &StreamResponse{Type: "execute", ExecuteResult: result}, nil

	case "batch":
		if req.Batch == nil {
			return nil, fmt.Errorf("hrana: missing batch payload")
		}
		result, err := executeBatch(ctx, stream, &req.Batch.Batch, stream.StoredSQL)
		if err != nil {
			return nil, err
		}
		return &StreamResponse{Type: "batch", BatchResult: result}, nil

	case "sequence":
		if req.Sequence == nil {
			return nil, fmt.Errorf("hrana: missing sequence payload")
		}
		sqlText, err := resolveStreamSQL(req.Sequence.SQL, req.Sequence.SQLId, stream.StoredSQL)
		if err != nil {
			return nil, err
		}
		if err := executeSequence(ctx, stream, sqlText); err != nil {
			return nil, err
		}
		return &StreamResponse{Type: "sequence"}, nil

	case "describe":
		if req.Describe == nil {
			return nil, fmt.Errorf("hrana: missing describe payload")
		}
		sqlText, err := resolveStreamSQL(req.Describe.SQL, req.Describe.SQLId, stream.StoredSQL)
		if err != nil {
			return nil, err
		}
		result, err := describeStmt(ctx, stream, sqlText)
		if err != nil {
			return nil, err
		}
		return &StreamResponse{Type: "describe", DescribeResult: result}, nil

	case "store_sql":
		if req.StoreSql == nil {
			return nil, fmt.Errorf("hrana: missing store_sql payload")
		}
		if _, exists := stream.StoredSQL[req.StoreSql.SQLID]; exists {
			return nil, fmt.Errorf("hrana: sql_id %d is already in use", req.StoreSql.SQLID)
		}
		stream.StoredSQL[req.StoreSql.SQLID] = req.StoreSql.SQL
		return &StreamResponse{Type: "store_sql"}, nil

	case "close_sql":
		if req.CloseSql == nil {
			return nil, fmt.Errorf("hrana: missing close_sql payload")
		}
		delete(stream.StoredSQL, req.CloseSql.SQLID)
		return &StreamResponse{Type: "close_sql"}, nil

	case "get_autocommit":
		// Default to true; a driver-specific check would go here.
		isAC := true
		return &StreamResponse{Type: "get_autocommit", IsAutocommit: &isAC}, nil

	default:
		return nil, fmt.Errorf("hrana: unknown stream request type %q", req.Type)
	}
}

func resolveStreamSQL(sqlText *string, sqlID *int32, storedSQL map[int32]string) (string, error) {
	if sqlText != nil && sqlID != nil {
		return "", fmt.Errorf("hrana: exactly one of sql and sql_id must be specified")
	}
	if sqlText != nil {
		return *sqlText, nil
	}
	if sqlID != nil {
		s, ok := storedSQL[*sqlID]
		if !ok {
			return "", fmt.Errorf("hrana: sql_id %d not found", *sqlID)
		}
		return s, nil
	}
	return "", fmt.Errorf("hrana: one of sql or sql_id must be specified")
}

// ─── V3 cursor handler ───────────────────────────────────────────────────────

func (s *Server) handleV3Cursor(w http.ResponseWriter, r *http.Request) {
	if err := s.checkAuth(r); err != nil {
		writeHTTPError(w, http.StatusUnauthorized, err.Error())
		return
	}

	mode, err := parseMode(r.URL.Query().Get("mode"))
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}

	var body CursorReqBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	var stream *Stream
	var oldBaton string

	if body.Baton != nil && *body.Baton != "" {
		oldBaton = *body.Baton
		st, err := s.batons.Get(oldBaton)
		if err != nil || st == nil {
			writeHTTPError(w, http.StatusBadRequest, "baton not found or expired")
			return
		}
		stream = st
	} else {
		st, err := s.OpenStream(r.Context())
		if err != nil {
			writeHTTPError(w, http.StatusInternalServerError, err.Error())
			return
		}
		st.Mode = mode
		stream = st
	}

	// Rotate the baton up front so we can include it in the first JSON line.
	baton, err := s.batons.Put(stream, oldBaton)
	if err != nil {
		stream.Close()
		writeHTTPError(w, http.StatusInternalServerError, err.Error())
		return
	}
	newBaton := &baton

	// Set streaming headers.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)

	// First line: CursorRespBody
	if err := enc.Encode(CursorRespBody{Baton: newBaton, BaseURL: nil}); err != nil {
		return
	}
	flushIfPossible(w)

	stream.Lock()
	entries, execErr := executeBatchCursor(r.Context(), stream, &body.Batch, stream.StoredSQL)
	stream.Unlock()

	if execErr != nil {
		_ = enc.Encode(CursorEntry{Type: "error", Error: &Error{Message: execErr.Error()}})
		flushIfPossible(w)
		return
	}

	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return
		}
		flushIfPossible(w)
	}
}

func flushIfPossible(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// ─── CORS helper ────────────────────────────────────────────────────────────

// setCORSHeaders writes CORS response headers when AllowOrigins is configured.
// Returns true if the request was a preflight OPTIONS request that has been
// fully handled (caller should return immediately).
func (s *Server) setCORSHeaders(w http.ResponseWriter, r *http.Request) bool {
	allowed := s.config.AllowOrigins
	if len(allowed) == 0 {
		return false
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}

	matched := false
	for _, o := range allowed {
		if o == "*" || o == origin {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
	w.Header().Set("Vary", "Origin")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// ─── Auth helper ─────────────────────────────────────────────────────────────

func (s *Server) checkAuth(r *http.Request) error {
	if s.config.AuthFunc == nil {
		return nil
	}
	token := ""
	if auth := r.Header.Get("Authorization"); len(auth) > 7 && auth[:7] == "Bearer " {
		token = auth[7:]
	}
	_, err := s.config.AuthFunc(token)
	return err
}

// ─── JSON helpers ─────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeHTTPError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, Error{Message: msg})
}

func errorStreamResult(msg string) StreamResult {
	e := &Error{Message: msg}
	return StreamResult{Type: "error", Error: e}
}
