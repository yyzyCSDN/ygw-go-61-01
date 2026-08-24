package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"sessionstore/internal/model"
)

// ErrMirrorWrite is the sentinel wrapped into every error produced by a failed
// mirror write.  It lets upper layers distinguish a mirror failure from a
// not-found or lifecycle error with errors.Is so the failure is reported
// honestly instead of being mistaken for a benign miss.
var ErrMirrorWrite = errors.New("mirror write failed")

// MirrorBackend is the durable sink that receives session snapshots.
type MirrorBackend interface {
	Read(ctx context.Context, id string) (*model.Session, error)
	Write(ctx context.Context, sess *model.Session) error
}

// MemoryMirror is an in-process mirror used by the demo server and tests.
type MemoryMirror struct {
	mu    sync.RWMutex
	items map[string]*model.Session
}

// NewMemoryMirror creates an empty in-memory mirror.
func NewMemoryMirror() *MemoryMirror {
	return &MemoryMirror{items: make(map[string]*model.Session)}
}

// Read returns the latest mirrored snapshot for a session id.
func (m *MemoryMirror) Read(ctx context.Context, id string) (*model.Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.items[id]
	if !ok {
		return nil, model.ErrNotFound
	}
	return sess.Clone(), nil
}

// Write stores a session snapshot in the mirror.
func (m *MemoryMirror) Write(ctx context.Context, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[sess.ID] = sess.Clone()
	return nil
}

// Count returns the number of snapshots held by the mirror.
func (m *MemoryMirror) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

// MirrorError wraps a failed mirror write with the session id for observability.
// The sentinel ErrMirrorWrite is always wrapped so callers can detect mirror
// failures with errors.Is even after the id and cause are layered on top.
func MirrorError(id string, err error) error {
	return fmt.Errorf("mirror write %s: %w: %w", id, err, ErrMirrorWrite)
}

// IsMirrorError reports whether err originated from a failed mirror write.
func IsMirrorError(err error) bool {
	return errors.Is(err, ErrMirrorWrite)
}
