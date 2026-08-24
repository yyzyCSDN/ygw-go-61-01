package clean

import "context"

// Report summarises one cleanup pass for the operator logs.
type Report struct {
	Scanned int
	Removed int
	Expired int
	Live    int
	Batches int
}

// ScanReport runs a cleanup pass and returns both the removal count and the
// pass statistics.
func (s *Scanner) ScanReport(ctx context.Context) (int, Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.store.SessionIDs()
	cursor := NewCursor(len(ids))
	report := Report{Scanned: len(ids)}
	removed := 0
	for cursor.Remaining() > 0 {
		batch := Take(ids, cursor.Position(), s.batchSize)
		report.Batches++
		for _, id := range batch {
			if s.isExpired(id) {
				report.Expired++
				if err := s.store.Remove(ctx, id); err != nil {
					return removed, report, err
				}
				removed++
				continue
			}
			report.Live++
		}
		cursor.Advance(len(batch))
	}
	report.Removed = removed
	return removed, report, nil
}
