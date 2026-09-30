package hrana

import (
	"container/list"
	"math"
	"net"
	"sync"
	"time"
)

// BlacklistChecker is implemented by anything that can track per-address
// "instant error" strikes and answer whether an address is currently
// blocked. The default implementation (constructed automatically when
// Config.Blacklist.Enabled is true) is an in-memory, LRU-bounded store
// scoped to a single process. Config.Blacklist.Store can inject a
// different implementation (e.g. Redis-backed) so blacklist state can be
// shared across multiple Server instances or processes, such as the
// per-subdomain multi-tenant deployment model.
type BlacklistChecker interface {
	// IsBlocked reports whether addr is currently blocked from opening new
	// connections, and if so, how much longer the block lasts.
	IsBlocked(addr string) (blocked bool, retryAfter time.Duration)

	// Strike records an "instant error" for addr. It returns the block
	// duration just applied (zero if addr is still within its free
	// strikes and was not blocked) along with the address's current
	// strike count.
	Strike(addr string) (blockedFor time.Duration, strikes int)
}

// BlacklistConfig controls the exponential-backoff blacklist applied to
// remote addresses that repeatedly produce "instant" WebSocket failures:
// sessions that end (read error, protocol violation, or early close)
// before completing `hello` and issuing at least one request, within
// InstantErrorThreshold of being opened.
//
// Starting-point configuration: the zero value of every field except
// Enabled is filled in with a sane default by New (see the field comments
// below), so enabling the feature with no further tuning is a reasonable
// starting point for most deployments:
//
//	conf := &hrana.Config{
//		Blacklist: hrana.BlacklistConfig{
//			Enabled: true, // everything else left as defaults
//		},
//	}
//
// which is equivalent to spelling out the defaults explicitly:
//
//	conf := &hrana.Config{
//		Blacklist: hrana.BlacklistConfig{
//			Enabled:               true,
//			FreeStrikes:           2,                // 2 instant failures tolerated before any block
//			InstantErrorThreshold: 2 * time.Second,   // session must survive 2s (and auth+request) to not strike
//			InitialBlock:          time.Second,       // first real block is just 1s
//			Multiplier:            4,                 // ...then 4s, 16s, 1m4s, 4m16s, ...
//			MaxBlock:              15 * time.Minute,  // ...capped at 15m regardless of further strikes
//			StrikeResetAfter:      10 * time.Minute,  // 10m of good behavior fully forgives past strikes
//			MaxTrackedAddrs:       10000,             // hard cap on distinct addresses tracked at once
//		},
//	}
//
// Tighten InitialBlock/Multiplier/MaxBlock for more aggressive throttling,
// or raise FreeStrikes/InstantErrorThreshold if legitimate clients are
// being caught (e.g. slow TLS handshakes on a loaded network). Set Store
// to share blacklist state across multiple Server instances/processes
// instead of using the built-in in-memory, per-process store.
type BlacklistConfig struct {
	// Enabled turns on blacklist tracking and enforcement. Default false;
	// existing integrators see no behavior change unless they opt in.
	Enabled bool

	// FreeStrikes is the number of instant-error strikes an address may
	// accumulate before the first block is issued. Default 2.
	FreeStrikes int

	// InstantErrorThreshold is the session duration below which an ending
	// session is considered a candidate strike. Default 2s.
	InstantErrorThreshold time.Duration

	// InitialBlock is the block duration applied on the first strike past
	// FreeStrikes. Default 1s.
	InitialBlock time.Duration

	// Multiplier is the growth factor applied to the block duration for
	// each additional strike beyond the first. Default 4.
	Multiplier float64

	// MaxBlock caps the block duration regardless of strike count.
	// Default 15m.
	MaxBlock time.Duration

	// StrikeResetAfter is the quiet period (no new strikes) after which an
	// address's strike count decays back to zero. Default 10m.
	StrikeResetAfter time.Duration

	// MaxTrackedAddrs bounds the number of addresses tracked at once by the
	// default in-memory store; least-recently-used addresses are evicted
	// once the limit is reached. This guarantees bounded memory usage even
	// under an active flood of distinct addresses. Default 10000.
	MaxTrackedAddrs int

	// OnBlock, if set, is called whenever an address transitions into a
	// blocked state, for metrics/alerting hooks.
	OnBlock func(addr string, strikes int, blockedFor time.Duration)

	// Store optionally overrides the default in-memory store with a custom
	// BlacklistChecker implementation. Leave nil to use the built-in,
	// process-local, LRU-bounded store.
	Store BlacklistChecker
}

// blacklistEntry tracks strike state for a single remote address.
type blacklistEntry struct {
	addr         string
	strikes      int
	blockedUntil time.Time
	lastStrike   time.Time
}

// memoryBlacklist is the default in-process BlacklistChecker: a
// mutex-guarded LRU of blacklistEntry, hard-bounded by MaxTrackedAddrs,
// with a background sweep that evicts fully-decayed entries.
type memoryBlacklist struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List
	cfg     BlacklistConfig
	sweep   *Interval
}

func newMemoryBlacklist(cfg BlacklistConfig) *memoryBlacklist {
	bl := &memoryBlacklist{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		cfg:     cfg,
	}

	interval := cfg.StrikeResetAfter / 2
	if interval < 60*time.Second {
		interval = 60 * time.Second
	}

	bl.sweep = IntervalFunc(interval, func(*Interval) {
		bl.sweepExpired()
	})

	return bl
}

// IsBlocked reports whether addr is currently blocked. Looking up an
// address also marks it as recently used in the LRU.
func (bl *memoryBlacklist) IsBlocked(addr string) (bool, time.Duration) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	el, ok := bl.entries[addr]
	if !ok {
		return false, 0
	}
	bl.order.MoveToFront(el)

	entry := el.Value.(*blacklistEntry)
	now := time.Now()
	if entry.blockedUntil.After(now) {
		return true, entry.blockedUntil.Sub(now)
	}
	return false, 0
}

// Strike records an instant-error strike for addr, applying strike-count
// decay first if the address has been quiet for longer than
// StrikeResetAfter. Returns the newly applied block duration (zero if still
// within FreeStrikes) and the resulting strike count.
func (bl *memoryBlacklist) Strike(addr string) (time.Duration, int) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	now := time.Now()
	entry := bl.getOrCreateLocked(addr)

	if entry.strikes > 0 && now.Sub(entry.lastStrike) > bl.cfg.StrikeResetAfter {
		entry.strikes = 0
		entry.blockedUntil = time.Time{}
	}

	entry.strikes++
	entry.lastStrike = now

	var blockedFor time.Duration
	if entry.strikes > bl.cfg.FreeStrikes {
		exp := entry.strikes - bl.cfg.FreeStrikes - 1
		blockedFor = scaledDuration(bl.cfg.InitialBlock, bl.cfg.Multiplier, exp, bl.cfg.MaxBlock)
		entry.blockedUntil = now.Add(blockedFor)
	}

	return blockedFor, entry.strikes
}

// getOrCreateLocked returns the entry for addr, creating it (and evicting
// the least-recently-used entry if MaxTrackedAddrs would be exceeded) if
// necessary. Callers must hold bl.mu.
func (bl *memoryBlacklist) getOrCreateLocked(addr string) *blacklistEntry {
	if el, ok := bl.entries[addr]; ok {
		bl.order.MoveToFront(el)
		return el.Value.(*blacklistEntry)
	}

	entry := &blacklistEntry{addr: addr}
	el := bl.order.PushFront(entry)
	bl.entries[addr] = el

	if max := bl.cfg.MaxTrackedAddrs; max > 0 && bl.order.Len() > max {
		if oldest := bl.order.Back(); oldest != nil {
			bl.order.Remove(oldest)
			delete(bl.entries, oldest.Value.(*blacklistEntry).addr)
		}
	}

	return entry
}

// sweepExpired removes entries that are both unblocked and have been quiet
// for longer than StrikeResetAfter, keeping the map small between LRU
// evictions.
func (bl *memoryBlacklist) sweepExpired() {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	now := time.Now()
	deadline := now.Add(-bl.cfg.StrikeResetAfter)
	for el := bl.order.Back(); el != nil; {
		prev := el.Prev()
		entry := el.Value.(*blacklistEntry)
		if entry.lastStrike.Before(deadline) && entry.blockedUntil.Before(now) {
			bl.order.Remove(el)
			delete(bl.entries, entry.addr)
		}
		el = prev
	}
}

// Close stops the background sweep goroutine.
func (bl *memoryBlacklist) Close() {
	bl.sweep.Stop()
}

// scaledDuration computes min(base * multiplier^exp, max), guarding against
// float overflow for large exponents.
func scaledDuration(base time.Duration, multiplier float64, exp int, max time.Duration) time.Duration {
	if exp <= 0 {
		if base > max {
			return max
		}
		return base
	}
	scaled := float64(base) * math.Pow(multiplier, float64(exp))
	if scaled > float64(max) || math.IsInf(scaled, 1) {
		return max
	}
	return time.Duration(scaled)
}

// remoteIP extracts just the IP portion of a "host:port" remote address
// string, so the blacklist keys on IP rather than the ephemeral port that
// changes on every connection. If addr has no port (or SplitHostPort
// otherwise fails), addr is returned unchanged.
func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
