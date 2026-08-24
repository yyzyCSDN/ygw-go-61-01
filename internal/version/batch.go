package version

// AllocBatch reserves n consecutive versions and returns them in order.  The
// returned slice has exactly n entries and never overlaps with a previous or
// following batch.
func (a *Allocator) AllocBatch(n int) []uint64 {
	if n <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	start := a.next
	a.next += uint64(n - 1)
	return Range(start, a.next)
}
