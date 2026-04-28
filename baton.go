package hrana

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// batonEntry holds a stream together with its last-access timestamp.
type batonEntry struct {
	stream    *Stream
	lastSeen  time.Time
}

// batonStore is a thread-safe map from baton string → stream. A background
// goroutine evicts entries that have exceeded the configured TTL.
type batonStore struct {
	mu      sync.Mutex
	entries map[string]*batonEntry
	ttl     time.Duration
	stop    chan struct{}
}

func newBatonStore(ttl time.Duration) *batonStore {
	bs := &batonStore{
		entries: make(map[string]*batonEntry),
		ttl:     ttl,
		stop:    make(chan struct{}),
	}
	go bs.sweepLoop()
	return bs
}

// Put stores a stream under a newly generated baton key and returns that key.
// If an old baton is provided it is deleted from the store beforehand (the spec
// requires a fresh baton on every request).
func (bs *batonStore) Put(stream *Stream, oldBaton string) (string, error) {
	newBaton, err := generateBaton()
	if err != nil {
		return "", fmt.Errorf("hrana: generating baton: %w", err)
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	if oldBaton != "" {
		delete(bs.entries, oldBaton)
	}

	bs.entries[newBaton] = &batonEntry{stream: stream, lastSeen: time.Now()}
	return newBaton, nil
}

// Get retrieves the stream for the given baton and refreshes its last-seen
// timestamp. Returns (nil, nil) when the baton is not found.
func (bs *batonStore) Get(baton string) (*Stream, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	entry, ok := bs.entries[baton]
	if !ok {
		return nil, nil
	}
	entry.lastSeen = time.Now()
	return entry.stream, nil
}

// Delete removes the baton from the store without closing the stream.
func (bs *batonStore) Delete(baton string) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	delete(bs.entries, baton)
}

// Close stops the sweep goroutine and closes all remaining streams.
func (bs *batonStore) Close() {
	close(bs.stop)
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for _, e := range bs.entries {
		e.stream.Close()
	}
	bs.entries = make(map[string]*batonEntry)
}

func (bs *batonStore) sweepLoop() {
	ticker := time.NewTicker(bs.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bs.sweep()
		case <-bs.stop:
			return
		}
	}
}

func (bs *batonStore) sweep() {
	deadline := time.Now().Add(-bs.ttl)
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for baton, entry := range bs.entries {
		if entry.lastSeen.Before(deadline) {
			entry.stream.Close()
			delete(bs.entries, baton)
		}
	}
}

// generateBaton produces a cryptographically random 32-byte hex baton token.
func generateBaton() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
