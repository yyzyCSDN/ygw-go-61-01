package version

import "sync"

// Allocator hands out monotonically increasing session versions.  Versions
// are the only ordering signal used by mirror and reconnect logic, so the
// allocator never reuses a number.
type Allocator struct {
	mu   sync.Mutex
	next uint64
}

// NewAllocator creates an allocator whose first issued version is start.
func NewAllocator(start uint64) *Allocator {
	return &Allocator{next: start}
}

// Next returns the next version and advances the allocator.
func (a *Allocator) Next() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	value := a.next
	a.next++
	return value
}
