package session

import (
	"context"
	"sync"
	"time"

	"sessionstore/internal/model"
)

// Backend is the durable read/write layer underneath the session store.  A
// tombstoned session id yields a nil session with a nil error so callers can
// distinguish "never existed" from "was evicted".
type Backend interface {
	Read(ctx context.Context, id string) (*model.Session, error)
	Write(ctx context.Context, id string, sess *model.Session) error
	Tombstone(ctx context.Context, id string) error
}

// MemoryBackend keeps sessions in process memory and records tombstones for
// evicted sessions.
type MemoryBackend struct {
	mu         sync.RWMutex
	items      map[string]*model.Session
	tombstones map[string]time.Time
}

// NewMemoryBackend creates an empty in-memory backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		items:      make(map[string]*model.Session),
		tombstones: make(map[string]time.Time),
	}
}

// Read returns the stored session, nil for a tombstoned id or ErrNotFound
// when the id was never seen.
func (b *MemoryBackend) Read(ctx context.Context, id string) (*model.Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, gone := b.tombstones[id]; gone {
		return nil, nil
	}
	sess, ok := b.items[id]
	if !ok {
		return nil, model.ErrNotFound
	}
	return sess.Clone(), nil
}

// Write stores a snapshot of the session under its id.
func (b *MemoryBackend) Write(ctx context.Context, id string, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.tombstones, id)
	b.items[id] = sess.Clone()
	return nil
}

// Tombstone marks a session id as evicted so reads yield nil instead of stale
// state.
func (b *MemoryBackend) Tombstone(ctx context.Context, id string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.items, id)
	b.tombstones[id] = time.Now()
	return nil
}

// Tombstoned reports whether an id was evicted.
func (b *MemoryBackend) Tombstoned(id string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.tombstones[id]
	return ok
}
