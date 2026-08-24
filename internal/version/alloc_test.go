package version

import "testing"

func TestNextVersionsMonotonic(t *testing.T) {
	alloc := NewAllocator(1)
	previous := uint64(0)
	for i := 0; i < 50; i++ {
		next := alloc.Next()
		if next <= previous {
			t.Fatalf("version must be monotonic: %d after %d", next, previous)
		}
		previous = next
	}
}
