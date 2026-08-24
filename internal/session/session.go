package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

// MirrorSink is implemented by the sync manager so the store can fan out
// session changes without importing the sync package.
type MirrorSink interface {
	Mirror(ctx context.Context, sess *model.Session) error
}

// StoreConfig carries every dependency of the session store.
type StoreConfig struct {
	Shards   *shard.Manager
	Clock    *ttl.Clock
	Versions *version.Allocator
	Mirror   MirrorSink
	Saver    Saver
	Backend  Backend
	Now      func() time.Time
}

// Store is the session lifecycle facade: creation, renewal, reads, eviction
// and batch renewal all flow through it.
type Store struct {
	mu       sync.RWMutex
	shards   *shard.Manager
	clock    *ttl.Clock
	versions *version.Allocator
	mirror   MirrorSink
	saver    Saver
	backend  Backend
	now      func() time.Time
}

// NewStore validates the configuration and returns a ready store.
func NewStore(cfg StoreConfig) (*Store, error) {
	if cfg.Shards == nil || cfg.Clock == nil || cfg.Versions == nil || cfg.Backend == nil {
		return nil, ErrMissingConfig
	}
	if cfg.Saver == nil {
		cfg.Saver = NewMemorySaver()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Store{
		shards:   cfg.Shards,
		clock:    cfg.Clock,
		versions: cfg.Versions,
		mirror:   cfg.Mirror,
		saver:    cfg.Saver,
		backend:  cfg.Backend,
		now:      cfg.Now,
	}, nil
}

// Create registers a new session, assigns its owner shard and mirrors the
// initial snapshot.
func (s *Store) Create(ctx context.Context, id string, ttl time.Duration) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.backend.Read(ctx, id)
	if err != nil && !errors.Is(err, model.ErrNotFound) {
		return nil, err
	}
	if existing != nil {
		return nil, model.ErrExists
	}
	owner := s.shards.NodeFor(id)
	sess := model.NewSession(id, owner, ttl, s.now())
	s.clock.Register(id, ttl)
	s.shards.Put(sess)
	if err := s.backend.Write(ctx, id, sess); err != nil {
		return nil, fmt.Errorf("persist %s: %w", id, err)
	}
	if err := s.Activate(ctx, sess); err != nil {
		return nil, err
	}
	if s.mirror != nil {
		if err := s.mirror.Mirror(ctx, sess); err != nil {
			return nil, err
		}
	}
	return sess.Clone(), nil
}

// Renew refreshes the activity watermark, bumps the version and moves the TTL
// window forward for the full session lifetime.
func (s *Store) Renew(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.backend.Read(ctx, id)
	if err != nil {
		return err
	}
	if sess == nil {
		return model.ErrNotFound
	}
	sess.Touch(s.now())
	sess.State = model.StateActive
	sess.Version = s.versions.Next()
	sess.ExpireAt = s.now().Add(sess.TTL)
	s.clock.Refresh(id, sess.TTL)
	if err := s.backend.Write(ctx, id, sess); err != nil {
		return fmt.Errorf("persist after renew %s: %w", id, err)
	}
	if s.mirror != nil {
		if err := s.mirror.Mirror(ctx, sess); err != nil {
			return fmt.Errorf("mirror after renew %s: %w", id, err)
		}
	}
	return nil
}

// Get returns the active session for an id.  Expired and tombstoned sessions
// surface as ErrExpired instead of a nil dereference.
func (s *Store) Get(ctx context.Context, id string) (*model.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, err := s.backend.Read(ctx, id)
	if err != nil {
		return nil, err
	}
	if sess == nil || !s.clock.IsActive(sess) {
		return nil, model.ErrExpired
	}
	_ = s.MarkStale(ctx, sess, s.now())
	return sess.Clone(), nil
}

// BatchRenew stamps a group of sessions with a contiguous version batch and
// refreshes each TTL window.
func (s *Store) BatchRenew(ctx context.Context, ids []string, ttl time.Duration) ([]uint64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.versions.AllocBatch(len(ids))
	if len(versions) != len(ids) {
		return nil, fmt.Errorf("allocated %d versions for %d sessions", len(versions), len(ids))
	}
	for index, id := range ids {
		sess, err := s.backend.Read(ctx, id)
		if err != nil {
			return nil, err
		}
		if sess == nil {
			return nil, model.ErrNotFound
		}
		sess.Version = versions[index]
		sess.Touch(s.now())
		sess.State = model.StateActive
		sess.ExpireAt = s.now().Add(ttl)
		s.clock.Refresh(id, ttl)
		if err := s.backend.Write(ctx, id, sess); err != nil {
			return nil, err
		}
		if s.mirror != nil {
			if err := s.mirror.Mirror(ctx, sess); err != nil {
				return nil, err
			}
		}
	}
	return versions, nil
}

// SessionIDs returns every live session id for the cleanup scanner.
func (s *Store) SessionIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.shards.All()
	ids := make([]string, 0, len(all))
	for _, sess := range all {
		ids = append(ids, sess.ID)
	}
	return ids
}

// Remove deletes a session from the shard table and TTL clock without the
// save-before-evict handshake; it is used by the cleanup scanner.
func (s *Store) Remove(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shards.Remove(id)
	s.clock.Prune(id)
	return nil
}

// Stats counts sessions by lifecycle state for the monitor page.
func (s *Store) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := map[string]int{"created": 0, "active": 0, "stale": 0, "expired": 0}
	for _, sess := range s.shards.All() {
		stats[sess.State.String()]++
	}
	return stats
}
