package hrana

import (
	"sync"
	"time"
)

// Interval represents an active recurring function call.
type Interval struct {
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
}

// IntervalFunc waits for the duration to elapse and then calls f sequentially
// with that interval. It returns an *Interval that can be used to cancel
// the recurring calls using its Stop method.
func IntervalFunc(d time.Duration, f func(interval *Interval)) *Interval {
	i := &Interval{}

	var run func()
	run = func() {
		i.mu.Lock()
		if i.stopped {
			i.mu.Unlock()
			return
		}
		i.mu.Unlock()

		// Execute f() outside the lock so long-running functions
		// do not block concurrent calls to Stop().
		f(i)

		i.mu.Lock()
		if !i.stopped {
			// Reuse the existing timer rather than allocating a new one.
			i.timer.Reset(d)
		}
		i.mu.Unlock()
	}

	i.mu.Lock()
	i.timer = time.AfterFunc(d, run)
	i.mu.Unlock()

	return i
}

// Stop prevents further calls to the function f.
// It returns true if the call stops the interval, false if the interval has
// already been stopped.
func (i *Interval) Stop() bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.stopped {
		return false
	}

	i.stopped = true
	return i.timer.Stop()
}
