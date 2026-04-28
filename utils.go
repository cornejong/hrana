package hrana

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
)

// RandomInt32 generates a cryptographically random int32.
func RandomInt32() (int32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b[:])), nil
}

// wsAccept computes the Sec-WebSocket-Accept header value for a given client key.
func wsAccept(key string) string {
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + magic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func writeRawHTTP(conn net.Conn, s string) {
	_, _ = fmt.Fprint(conn, s)
}

// minimalHTTPRequest is a minimal parsed HTTP/1.x request line + headers.
type minimalHTTPRequest struct {
	method  string
	path    string
	headers map[string]string
}

func (r *minimalHTTPRequest) header(key string) string {
	return r.headers[strings.ToLower(key)]
}

// readHTTPRequest reads the request line and headers from a bufio.Reader.
func readHTTPRequest(r *bufio.Reader) (*minimalHTTPRequest, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("hrana: invalid HTTP request line")
	}

	req := &minimalHTTPRequest{
		method:  parts[0],
		path:    parts[1],
		headers: make(map[string]string),
	}

	for {
		hline, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hline = strings.TrimSpace(hline)
		if hline == "" {
			break
		}
		idx := strings.IndexByte(hline, ':')
		if idx < 0 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(hline[:idx]))
		v := strings.TrimSpace(hline[idx+1:])
		req.headers[k] = v
	}

	return req, nil
}

// queryParam returns the first value of the named query parameter from the
// request path (e.g. "/v3/pipeline?mode=readonly" → "readonly").
func (r *minimalHTTPRequest) queryParam(key string) string {
	idx := strings.IndexByte(r.path, '?')
	if idx < 0 {
		return ""
	}
	vals, err := url.ParseQuery(r.path[idx+1:])
	if err != nil {
		return ""
	}
	return vals.Get(key)
}

// isWriteStatement reports whether sql is a write (mutating) SQL statement.
// The check is keyword-based on the first meaningful token after stripping
// leading line comments (--) and block comments (/* ... */). It is not a full
// SQL parser and is intentionally conservative: only verbs that directly mutate
// schema or data are rejected; transaction control (BEGIN/COMMIT/ROLLBACK) is
// not blocked so that read-only transactions can still be opened explicitly.
func isWriteStatement(sql string) bool {
	s := strings.TrimSpace(sql)

	// Skip leading line comments.
	for strings.HasPrefix(s, "--") {
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			return false // comment-only
		}
		s = strings.TrimSpace(s[nl+1:])
	}

	// Skip leading block comments.
	for strings.HasPrefix(s, "/*") {
		end := strings.Index(s, "*/")
		if end < 0 {
			return false
		}
		s = strings.TrimSpace(s[end+2:])
	}

	upper := strings.ToUpper(s)
	for _, kw := range writeKeywords {
		if strings.HasPrefix(upper, kw) {
			rest := upper[len(kw):]
			if rest == "" || !isIdentChar(rest[0]) {
				return true
			}
		}
	}
	return false
}

// writeKeywords is the set of SQL verb prefixes considered write operations.
var writeKeywords = []string{
	"INSERT", "UPDATE", "DELETE",
	"CREATE", "DROP", "ALTER",
	"REPLACE", "REINDEX", "VACUUM", "ANALYZE",
}

// isIdentChar reports whether b can be part of a SQL identifier (A-Z, 0-9, _).
func isIdentChar(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// prefixHandler wraps a slog.Handler and prepends "[hrana.{part}] " to every
// log message, producing output like: [hrana.http] request received path=/v1/execute
type prefixHandler struct {
	handler slog.Handler
	prefix  string
}

func (h *prefixHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *prefixHandler) Handle(ctx context.Context, r slog.Record) error {
	r2 := slog.NewRecord(r.Time, r.Level, h.prefix+" "+r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		r2.AddAttrs(a)
		return true
	})
	return h.handler.Handle(ctx, r2)
}

func (h *prefixHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &prefixHandler{handler: h.handler.WithAttrs(attrs), prefix: h.prefix}
}

func (h *prefixHandler) WithGroup(name string) slog.Handler {
	return &prefixHandler{handler: h.handler.WithGroup(name), prefix: h.prefix}
}

// subLogger returns a *slog.Logger that prepends "[hrana.{part}]" to every message.
func subLogger(base *slog.Logger, part string) *slog.Logger {
	return slog.New(&prefixHandler{
		handler: base.Handler(),
		prefix:  "[hrana." + part + "]",
	})
}
