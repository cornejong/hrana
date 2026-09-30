package hrana

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

var ErrStreamIsSuperseded = errors.New("stream is superseded by a newer one. Create a new stream")

type GenerationalDB struct {
	*sql.DB
	generation int64
}

func (g *GenerationalDB) Generation() int64 {
	return g.generation
}

type GenerationalConn struct {
	*sql.Conn
	generation int64
}

func (g *GenerationalConn) Generation() int64 {
	return g.generation
}

// Stream represents a single Hrana stream – a pinned *sql.Conn plus
// per-stream state (stored SQL texts, open cursors).
type Stream struct {
	ID           int32
	Conn         *GenerationalConn
	IsSuperseded bool

	StoredSQL map[int32]string  // sql_id -> SQL text (V2+)
	Cursors   map[int32]*Cursor // cursor_id -> Cursor (V3 WS only)
	Mode      ConnectionMode    // readonly or readwrite (default)

	// mu serialises all operations on this stream.
	mu     sync.Mutex
	closed bool
}

// NewStream constructs a Stream that owns conn.
func NewStream(id int32, conn *GenerationalConn) *Stream {
	return &Stream{
		ID:        id,
		Conn:      conn,
		StoredSQL: make(map[int32]string),
		Cursors:   make(map[int32]*Cursor),
	}
}

// Lock acquires the stream mutex. The caller must call Unlock when done.
func (s *Stream) Lock()   { s.mu.Lock() }
func (s *Stream) Unlock() { s.mu.Unlock() }

// Close closes the underlying *sql.Conn, returning it to the pool.
// Safe to call multiple times.
func (s *Stream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	_ = s.Conn.Close()
}

// IsClosed reports whether the stream has been closed.
func (s *Stream) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// OpenStream creates a new Stream by acquiring a dedicated *sql.Conn from the
// pool. The caller is responsible for calling Stream.Close when done.
func (s *Server) OpenStream(ctx context.Context) (*Stream, error) {
	db := s.db.Load()
	dbGen := db.Generation() // Extract it here already to avoid race conditions.. Probably should make this mutexed or a lane

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("hrana: failed to obtain sql.Conn: %w", err)
	}

	id, err := RandomInt32()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("hrana: failed to generate stream id: %w", err)
	}

	return NewStream(id, &GenerationalConn{Conn: conn, generation: dbGen}), nil
}

// ─── Cursor (V3) ─────────────────────────────────────────────────────────────

// Cursor holds pre-computed entries for WS open_cursor / fetch_cursor requests.
type Cursor struct {
	ID      int32
	entries []CursorEntry
	pos     int
	done    bool
}

func newCursor(id int32, entries []CursorEntry) *Cursor {
	return &Cursor{ID: id, entries: entries}
}

// Fetch returns up to maxCount entries and whether the cursor is exhausted.
func (c *Cursor) Fetch(maxCount uint32) ([]CursorEntry, bool) {
	end := c.pos + int(maxCount)
	if end > len(c.entries) {
		end = len(c.entries)
	}
	out := c.entries[c.pos:end]
	c.pos = end
	c.done = c.pos >= len(c.entries)
	return out, c.done
}

// ─── Session (per WS connection) ────────────────────────────────────────────

// Session holds per-WebSocket-connection state: open streams and
// connection-scoped stored SQL texts (V2+).
type Session struct {
	mu        sync.Mutex
	streams   map[int32]*Stream
	storedSQL map[int32]string
	Mode      ConnectionMode // inherited by all streams opened on this session
}

func newSession(mode ConnectionMode) *Session {
	return &Session{
		streams:   make(map[int32]*Stream),
		storedSQL: make(map[int32]string),
		Mode:      mode,
	}
}

func (sess *Session) addStream(id int32, st *Stream) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st.ID = id
	sess.streams[id] = st
}

// AddStreamAssinged assigns a unique random id to st and registers it in the
// session. It returns the assigned id and true on success, or 0 and false if
// no id could be generated.
func (sess *Session) AddStreamAssinged(st *Stream) (int32, bool) {
	if st == nil {
		return 0, false
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	id, err := RandomInt32()
	if err != nil {
		return 0, false
	}

	for _, exists := sess.streams[id]; exists; {
		id, err = RandomInt32()
		if err != nil {
			return 0, false
		}
		_, exists = sess.streams[id]
	}

	st.ID = id
	sess.streams[id] = st
	return id, true
}

func (sess *Session) getStream(id int32) (*Stream, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st, ok := sess.streams[id]
	return st, ok
}

func (sess *Session) removeStream(id int32) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if st, ok := sess.streams[id]; ok {
		st.Close()
		delete(sess.streams, id)
	}
}

func (sess *Session) closeAll() {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, st := range sess.streams {
		st.Close()
	}
	sess.streams = make(map[int32]*Stream)
}

func (sess *Session) storeSQL(id int32, sqlText string) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if _, exists := sess.storedSQL[id]; exists {
		return fmt.Errorf("hrana: sql_id %d is already in use", id)
	}
	sess.storedSQL[id] = sqlText
	return nil
}

func (sess *Session) closeSQL(id int32) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	delete(sess.storedSQL, id)
}

func (sess *Session) resolveSQL(sqlText *string, sqlID *int32) (string, error) {
	if sqlText != nil && sqlID != nil {
		return "", fmt.Errorf("hrana: exactly one of sql and sql_id must be specified")
	}
	if sqlText != nil {
		return *sqlText, nil
	}
	if sqlID != nil {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		s, ok := sess.storedSQL[*sqlID]
		if !ok {
			return "", fmt.Errorf("hrana: sql_id %d not found", *sqlID)
		}
		return s, nil
	}
	return "", fmt.Errorf("hrana: one of sql or sql_id must be specified")
}
