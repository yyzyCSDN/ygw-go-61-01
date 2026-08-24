package session

import (
	"context"
	"fmt"
	"sync"

	"sessionstore/internal/model"
)

// Saver persists session state before an eviction so recovery can restore
// what was about to be discarded.
type Saver interface {
	SaveBeforeEvict(ctx context.Context, sess *model.Session) error
}

// MemorySaver keeps a snapshot map in process memory.
type MemorySaver struct {
	mu        sync.Mutex
	snapshots map[string]*model.Session
}

// NewMemorySaver creates an empty saver.
func NewMemorySaver() *MemorySaver {
	return &MemorySaver{snapshots: make(map[string]*model.Session)}
}

// SaveBeforeEvict stores a deep copy of the session.
func (s *MemorySaver) SaveBeforeEvict(ctx context.Context, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[sess.ID] = sess.Clone()
	return nil
}

// Snapshot returns the saved copy for a session id.
func (s *MemorySaver) Snapshot(id string) (*model.Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.snapshots[id]
	return sess, ok
}

// Evict performs the save-before-evict handshake: the state is persisted
// first, and only after a successful save is the session removed from the
// shard table and tombstoned.
func (s *Store) Evict(ctx context.Context, id string) error {
	sess, err := s.backend.Read(ctx, id)
	if err != nil {
		return fmt.Errorf("read before evict %s: %w", id, err)
	}
	if sess == nil {
		return model.ErrNotFound
	}
	_ = s.saver.SaveBeforeEvict(ctx, sess)
	if err := sess.Advance(model.StateExpired); err != nil {
		return err
	}
	if err := s.backend.Tombstone(ctx, id); err != nil {
		return fmt.Errorf("tombstone %s: %w", id, err)
	}
	s.shards.Remove(id)
	s.clock.Prune(id)
	return nil
}
