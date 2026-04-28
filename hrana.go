package hrana

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionMode controls which SQL operations a stream allows at the
// protocol level, independent of the underlying database permissions.
type ConnectionMode string

const (
	// ModeReadWrite is the default: all SQL operations are permitted.
	ModeReadWrite ConnectionMode = "readwrite"
	// ModeReadOnly rejects any write SQL statements before they reach the
	// database. The check is keyword-based (INSERT/UPDATE/DELETE/CREATE/
	// DROP/ALTER/REPLACE/REINDEX/VACUUM/ANALYZE).
	ModeReadOnly ConnectionMode = "readonly"
)

// parseMode converts a raw ?mode= query parameter value to a ConnectionMode.
// An empty string is treated as the default (ModeReadWrite).
func parseMode(s string) (ConnectionMode, error) {
	switch s {
	case "readonly":
		return ModeReadOnly, nil
	case "readwrite", "":
		return ModeReadWrite, nil
	default:
		return ModeReadWrite, fmt.Errorf("hrana: unknown mode %q, use 'readonly' or 'readwrite'", s)
	}
}

// Config configures the Hrana server.
type Config struct {
	ctx context.Context

	// AuthFunc is invoked on HelloMsg or HTTP requests. It returns the token's
	// expiry time and any auth error. A nil expiry means the token never expires.
	// If AuthFunc itself is nil, auth is bypassed and tokens never expire.
	AuthFunc func(token string) (*time.Time, error)

	// EnableProtobuf is a placeholder for future Protobuf support.
	EnableProtobuf bool

	// BatonTTL defines how long HTTP streams stay alive without activity (default: 10s).
	BatonTTL time.Duration

	// StatementTimeout defines when a statement will timeout in execution. If nill no timeout will be enforced
	StatementTimeout *time.Duration

	// OnLastWSDisconnect is called when the last active WebSocket connection
	// closes. It is invoked on the goroutine that handled the final connection.
	// It is not called if no WebSocket connections were ever established.
	OnLastWSDisconnect func()

	// Logger is the structured logger used by the server. If nil, slog.Default() is used.
	// Sub-loggers are derived per component with the prefix [hrana.{part}].
	Logger *slog.Logger

	// Versions is the list of Hrana versions the server will accept ("v1", "v2", "v3").
	// If nil or empty, all versions are supported.
	Versions []string

	// AllowOrigins is the list of origins that are permitted to make cross-origin
	// requests (CORS). Use ["*"] to allow all origins. If nil or empty, no CORS
	// headers are sent (same-origin only).
	AllowOrigins []string
}

// Server holds the Hrana server state.
type Server struct {
	db          *sql.DB
	config      *Config
	httpLog     *slog.Logger
	wsLog       *slog.Logger
	batons      *batonStore
	ctx         context.Context
	cancel      context.CancelFunc
	wsConnCount int64 // accessed atomically
	closing     atomic.Bool
	wsMu        sync.Mutex
	wsConns     map[net.Conn]struct{}
	wg          sync.WaitGroup
}

// New creates a new Hrana Server wrapping the given *sql.DB.
func New(db *sql.DB, conf *Config) *Server {
	if conf == nil {
		conf = &Config{}
	}

	if conf.BatonTTL == 0 {
		conf.BatonTTL = 10 * time.Second
	}

	if conf.AuthFunc == nil {
		conf.AuthFunc = func(token string) (*time.Time, error) { return nil, nil }
	}

	base := conf.Logger
	if base == nil {
		base = slog.Default()
	}

	if conf.ctx == nil {
		conf.ctx = context.Background()
	}

	ctx, cancel := context.WithCancel(conf.ctx)

	return &Server{
		db:      db,
		config:  conf,
		httpLog: subLogger(base, "http"),
		wsLog:   subLogger(base, "ws"),
		batons:  newBatonStore(conf.BatonTTL),
		ctx:     ctx,
		cancel:  cancel,
		wsConns: make(map[net.Conn]struct{}),
	}
}

// ActiveWSConnections returns the number of currently active WebSocket connections.
func (s *Server) ActiveWSConnections() int64 {
	return atomic.LoadInt64(&s.wsConnCount)
}

// Done returns a channel that is closed when the server is shut down via Close.
// It follows the same semantics as context.Context.Done.
func (s *Server) Done() <-chan struct{} {
	return s.ctx.Done()
}

// IsClosed reports whether the server has been shut down via Close.
func (s *Server) IsClosed() bool {
	return s.ctx.Err() != nil
}

// Close shuts down the server gracefully. It stops accepting new connections
// and requests, sends a WebSocket close frame (1001 Going Away) to all active
// WebSocket connections, waits for all in-flight queries to complete, then
// cancels the server context and releases all HTTP baton streams.
// An optional reason string is sent as the close-frame reason; it defaults to
// "server is shutting down".
func (s *Server) Close(reason ...string) {
	msg := "server is shutting down"
	if len(reason) > 0 && reason[0] != "" {
		msg = reason[0]
	}
	s.closing.Store(true)
	s.closeAllWSConns(msg)
	s.wg.Wait()
	s.cancel()
	s.batons.Close()
}

// isVersionEnabled reports whether the given Hrana version (e.g. "v1", "v2", "v3")
// is enabled. When no Versions are configured, all versions are considered enabled.
func (s *Server) isVersionEnabled(v string) bool {
	if len(s.config.Versions) == 0 {
		return true
	}
	for _, ver := range s.config.Versions {
		if ver == v {
			return true
		}
	}
	return false
}
