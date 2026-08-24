package clean

import (
	"context"
	"sync"
)

// SessionStore is the minimal surface the scanner needs from the session
// layer.
type SessionStore interface {
	SessionIDs() []string
	Remove(ctx context.Context, id string) error
}

// Scanner sweeps expired sessions in bounded batches so a single pass does
// not hold a large slice alive longer than necessary.
type Scanner struct {
	mu        sync.Mutex
	store     SessionStore
	isExpired func(id string) bool
	batchSize int
}

// NewScanner builds a scanner with the configured batch size.
func NewScanner(store SessionStore, isExpired func(id string) bool, batchSize int) *Scanner {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Scanner{
		store:     store,
		isExpired: isExpired,
		batchSize: batchSize,
	}
}

// Scan visits every live session id exactly once and removes the expired
// ones.  It returns the number of sessions removed.
func (s *Scanner) Scan(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.store.SessionIDs()
	cursor := NewCursor(len(ids))
	removed := 0
	for cursor.Remaining() > 0 {
		batch := Take(ids, cursor.Position(), s.batchSize)
		for _, id := range batch {
			if s.isExpired(id) {
				if err := s.store.Remove(ctx, id); err != nil {
					return removed, err
				}
				removed++
			}
		}
		cursor.Advance(len(batch))
	}
	return removed, nil
}
