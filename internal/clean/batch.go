package clean

// Take returns the next batch of ids starting at cursor.  The final partial
// batch is clamped to the slice length instead of being dropped.
func Take(ids []string, cursor, size int) []string {
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(ids) || size <= 0 {
		return nil
	}
	end := cursor + size
	if end > len(ids) {
		return nil
	}
	return ids[cursor:end]
}
