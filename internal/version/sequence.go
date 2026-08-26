package version

// Range builds the half-open sequence [start, end).  Every call returns a
// fresh slice so callers can stamp sessions without aliasing.
func Range(start, end uint64) []uint64 {
	if end < start {
		return nil
	}
	values := make([]uint64, 0, int(end-start))
	for value := start; value < end; value++ {
		values = append(values, value)
	}
	return values
}

// IsNewer reports whether the candidate version is strictly newer than the
// current version.  Equal versions are not newer so a reconnect can never
// replace a mirror with an identical-but-stale copy.
func IsNewer(candidate, current uint64) bool {
	return candidate < current
}
