package hrana

import (
	"testing"
	"time"
)

// var _ BlacklistChecker ensures memoryBlacklist keeps satisfying the public
// interface at compile time.
var _ BlacklistChecker = (*memoryBlacklist)(nil)

// testBlacklistConfig mirrors the "starting point" defaults documented on
// BlacklistConfig, with a large MaxTrackedAddrs so eviction tests can opt
// into a smaller value explicitly.
func testBlacklistConfig() BlacklistConfig {
	return BlacklistConfig{
		FreeStrikes:           2,
		InstantErrorThreshold: 2 * time.Second,
		InitialBlock:          time.Second,
		Multiplier:            4,
		MaxBlock:              15 * time.Minute,
		StrikeResetAfter:      10 * time.Minute,
		MaxTrackedAddrs:       10000,
	}
}

func newTestBlacklist(t *testing.T, cfg BlacklistConfig) *memoryBlacklist {
	t.Helper()
	bl := NewMemoryBlacklist(cfg)
	t.Cleanup(bl.Close)
	return bl
}

// entryFor reaches into the store's internal map for white-box assertions
// and test setup (e.g. simulating the passage of time without sleeping).
func entryFor(bl *memoryBlacklist, addr string) *blacklistEntry {
	el, ok := bl.entries[addr]
	if !ok {
		return nil
	}
	return el.Value.(*blacklistEntry)
}

// ─── scaledDuration ──────────────────────────────────────────────────────────

func TestScaledDuration(t *testing.T) {
	const max = 15 * time.Minute

	cases := []struct {
		name string
		exp  int
		want time.Duration
	}{
		{"exp0", 0, time.Second},
		{"negativeExpTreatedAsBase", -1, time.Second},
		{"exp1", 1, 4 * time.Second},
		{"exp2", 2, 16 * time.Second},
		{"exp3", 3, 64 * time.Second},
		{"expBeyondCap", 5, max}, // 4^5 * 1s = 1024s > 15m cap
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := scaledDuration(time.Second, 4, c.exp, max)
			if got != c.want {
				t.Errorf("scaledDuration(1s, 4, %d, 15m) = %v, want %v", c.exp, got, c.want)
			}
		})
	}
}

func TestScaledDuration_BaseAboveMaxWithoutExponent(t *testing.T) {
	got := scaledDuration(20*time.Minute, 4, 0, 15*time.Minute)
	if got != 15*time.Minute {
		t.Errorf("expected base to be clamped to max, got %v", got)
	}
}

// ─── remoteIP ────────────────────────────────────────────────────────────────

func TestRemoteIP(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		{"192.0.2.1:1234", "192.0.2.1"},
		{"[2001:db8::1]:1234", "2001:db8::1"},
		{"192.0.2.1", "192.0.2.1"}, // no port: SplitHostPort fails, returned unchanged
		{"", ""},
	}

	for _, c := range cases {
		if got := remoteIP(c.addr); got != c.want {
			t.Errorf("remoteIP(%q) = %q, want %q", c.addr, got, c.want)
		}
	}
}

// ─── memoryBlacklist.Strike / IsBlocked ─────────────────────────────────────

func TestMemoryBlacklist_FreeStrikesDoNotBlock(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())

	for i := 1; i <= 2; i++ {
		blockedFor, strikes := bl.Strike("10.0.0.1")
		if blockedFor != 0 {
			t.Fatalf("strike %d: expected no block within FreeStrikes, got %v", i, blockedFor)
		}
		if strikes != i {
			t.Fatalf("strike %d: expected strikes=%d, got %d", i, i, strikes)
		}
	}

	if blocked, _ := bl.IsBlocked("10.0.0.1"); blocked {
		t.Fatal("address should not be blocked while within FreeStrikes")
	}
}

func TestMemoryBlacklist_ExponentialBackoffProgression(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addr := "10.0.0.2"

	// Burn the two free strikes first.
	bl.Strike(addr)
	bl.Strike(addr)

	want := []time.Duration{
		time.Second,      // strike 3 (1st over threshold): 1s * 4^0
		4 * time.Second,  // strike 4: 1s * 4^1
		16 * time.Second, // strike 5: 1s * 4^2
		64 * time.Second, // strike 6: 1s * 4^3
	}

	for i, want := range want {
		got, strikes := bl.Strike(addr)
		if got != want {
			t.Fatalf("strike %d: blockedFor = %v, want %v", i+3, got, want)
		}
		if strikes != i+3 {
			t.Fatalf("strike %d: strikes = %d, want %d", i+3, strikes, i+3)
		}
	}
}

func TestMemoryBlacklist_BlockDurationCapped(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addr := "10.0.0.3"

	bl.Strike(addr)
	bl.Strike(addr)

	var last time.Duration
	for i := 0; i < 10; i++ {
		last, _ = bl.Strike(addr)
	}

	if last != 15*time.Minute {
		t.Fatalf("expected block duration to cap at MaxBlock (15m), got %v", last)
	}
}

func TestMemoryBlacklist_IsBlockedReflectsStrike(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addr := "10.0.0.4"

	bl.Strike(addr)
	bl.Strike(addr)
	blockedFor, _ := bl.Strike(addr) // 3rd strike -> first real block

	blocked, retryAfter := bl.IsBlocked(addr)
	if !blocked {
		t.Fatal("expected address to be blocked")
	}
	if retryAfter <= 0 || retryAfter > blockedFor {
		t.Fatalf("retryAfter = %v, expected in (0, %v]", retryAfter, blockedFor)
	}
}

func TestMemoryBlacklist_UnknownAddrNotBlocked(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())

	if blocked, retryAfter := bl.IsBlocked("10.0.0.99"); blocked || retryAfter != 0 {
		t.Fatalf("expected unknown address to be unblocked, got blocked=%v retryAfter=%v", blocked, retryAfter)
	}
}

func TestMemoryBlacklist_BlockExpires(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addr := "10.0.0.5"

	bl.Strike(addr)
	bl.Strike(addr)
	bl.Strike(addr)

	if blocked, _ := bl.IsBlocked(addr); !blocked {
		t.Fatal("expected address to be blocked immediately after strike")
	}

	// Simulate the block window having elapsed without sleeping in the test.
	entryFor(bl, addr).blockedUntil = time.Now().Add(-time.Second)

	if blocked, retryAfter := bl.IsBlocked(addr); blocked || retryAfter != 0 {
		t.Fatalf("expected block to have expired, got blocked=%v retryAfter=%v", blocked, retryAfter)
	}
}

func TestMemoryBlacklist_StrikeCountDecaysAfterQuietPeriod(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addr := "10.0.0.6"

	bl.Strike(addr)
	bl.Strike(addr)
	bl.Strike(addr) // strikes=3, now blocked

	// Simulate a quiet period longer than StrikeResetAfter.
	entryFor(bl, addr).lastStrike = time.Now().Add(-bl.cfg.StrikeResetAfter - time.Second)

	blockedFor, strikes := bl.Strike(addr)
	if strikes != 1 {
		t.Fatalf("expected strike count to reset to 1 after quiet period, got %d", strikes)
	}
	if blockedFor != 0 {
		t.Fatalf("expected no block immediately after decay, got %v", blockedFor)
	}
	if blocked, _ := bl.IsBlocked(addr); blocked {
		t.Fatal("expected address to be unblocked after strike-count decay")
	}
}

// ─── LRU eviction (hard memory bound) ───────────────────────────────────────

func TestMemoryBlacklist_LRUEvictsOldestWhenFull(t *testing.T) {
	cfg := testBlacklistConfig()
	cfg.MaxTrackedAddrs = 2
	bl := newTestBlacklist(t, cfg)

	bl.Strike("addr-a")
	bl.Strike("addr-b")
	bl.Strike("addr-c") // should evict addr-a (least recently used)

	if entryFor(bl, "addr-a") != nil {
		t.Fatal("expected addr-a to have been evicted")
	}
	if entryFor(bl, "addr-b") == nil {
		t.Fatal("expected addr-b to still be tracked")
	}
	if entryFor(bl, "addr-c") == nil {
		t.Fatal("expected addr-c to still be tracked")
	}
	if got := len(bl.entries); got != 2 {
		t.Fatalf("expected exactly 2 tracked addresses, got %d", got)
	}

	// A fresh strike against the evicted address starts its count over.
	_, strikes := bl.Strike("addr-a")
	if strikes != 1 {
		t.Fatalf("expected evicted address to start over at strikes=1, got %d", strikes)
	}
}

func TestMemoryBlacklist_TouchingEntryProtectsFromEviction(t *testing.T) {
	cfg := testBlacklistConfig()
	cfg.MaxTrackedAddrs = 2
	bl := newTestBlacklist(t, cfg)

	bl.Strike("addr-a")
	bl.Strike("addr-b")

	// Touch addr-a via IsBlocked so it becomes most-recently-used, meaning
	// addr-b should be evicted instead when addr-c arrives.
	bl.IsBlocked("addr-a")
	bl.Strike("addr-c")

	if entryFor(bl, "addr-b") != nil {
		t.Fatal("expected addr-b to have been evicted after addr-a was touched")
	}
	if entryFor(bl, "addr-a") == nil {
		t.Fatal("expected addr-a to remain tracked after being touched")
	}
}

func TestMemoryBlacklist_ZeroMaxTrackedAddrsDisablesEviction(t *testing.T) {
	cfg := testBlacklistConfig()
	cfg.MaxTrackedAddrs = 0
	bl := newTestBlacklist(t, cfg)

	for i := 0; i < 50; i++ {
		bl.Strike(string(rune('a' + i%26)))
	}
	// No assertion on a specific count beyond "didn't panic and grew";
	// just sanity check the map actually grew past a tiny bound.
	if got := len(bl.entries); got == 0 {
		t.Fatal("expected entries to be tracked when eviction is disabled")
	}
}

// ─── sweepExpired ────────────────────────────────────────────────────────────

func TestMemoryBlacklist_SweepRemovesDecayedEntries(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())

	bl.Strike("stale-addr")
	bl.Strike("fresh-addr")

	// Make stale-addr look long-quiet and unblocked; fresh-addr stays recent.
	stale := entryFor(bl, "stale-addr")
	stale.lastStrike = time.Now().Add(-bl.cfg.StrikeResetAfter - time.Minute)
	stale.blockedUntil = time.Time{}

	bl.sweepExpired()

	if entryFor(bl, "stale-addr") != nil {
		t.Fatal("expected stale-addr to be swept")
	}
	if entryFor(bl, "fresh-addr") == nil {
		t.Fatal("expected fresh-addr to survive the sweep")
	}
}

func TestMemoryBlacklist_SweepKeepsStillBlockedEntries(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())

	bl.Strike("addr")
	bl.Strike("addr")
	bl.Strike("addr") // now blocked

	entry := entryFor(bl, "addr")
	entry.lastStrike = time.Now().Add(-bl.cfg.StrikeResetAfter - time.Minute)
	// blockedUntil intentionally left in the future.

	bl.sweepExpired()

	if entryFor(bl, "addr") == nil {
		t.Fatal("expected still-blocked entry to survive the sweep despite being quiet")
	}
}

// ─── Close ───────────────────────────────────────────────────────────────────

func TestMemoryBlacklist_CloseStopsSweep(t *testing.T) {
	cfg := testBlacklistConfig()
	bl := NewMemoryBlacklist(cfg)
	bl.Strike("addr")

	bl.Close() // should not panic, and stops the background Interval

	if entryFor(bl, "addr") == nil {
		t.Fatal("closing the store should not clear existing entries")
	}
}

// ─── concurrency ─────────────────────────────────────────────────────────────

// TestMemoryBlacklist_ConcurrentAccess exercises Strike/IsBlocked from many
// goroutines across a handful of addresses. It doesn't assert on specific
// values (those are covered above) - the goal is to catch data races when
// run with `go test -race`.
func TestMemoryBlacklist_ConcurrentAccess(t *testing.T) {
	bl := newTestBlacklist(t, testBlacklistConfig())
	addrs := []string{"c1", "c2", "c3", "c4"}

	done := make(chan struct{})
	for _, addr := range addrs {
		addr := addr
		go func() {
			for i := 0; i < 200; i++ {
				bl.Strike(addr)
				bl.IsBlocked(addr)
			}
			done <- struct{}{}
		}()
	}

	for range addrs {
		<-done
	}
}
