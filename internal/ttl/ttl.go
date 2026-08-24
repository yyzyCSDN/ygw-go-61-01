package ttl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sessionstore/internal/model"
)

// Entry tracks the expiry window of one session.
type Entry struct {
	SessionID string
	ExpireAt  time.Time
	TTL       time.Duration
}

// Clock owns the expiration table shared by the session lifecycle and the
// cleanup scanner.  The clock uses an injectable time source so tests can
// advance time deterministically.
type Clock struct {
	mu         sync.Mutex
	entries    map[string]*Entry
	now        func() time.Time
	defaultTTL time.Duration
}

// NewClock creates a clock with the given default session TTL.
func NewClock(defaultTTL time.Duration) *Clock {
	return &Clock{
		entries:    make(map[string]*Entry),
		now:        time.Now,
		defaultTTL: defaultTTL,
	}
}

// Register installs the expiry entry for a freshly created session.
func (c *Clock) Register(id string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	c.entries[id] = &Entry{
		SessionID: id,
		ExpireAt:  c.now().Add(ttl),
		TTL:       ttl,
	}
}

// Prune forgets an entry after the session was evicted.
func (c *Clock) Prune(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, id)
}

// IsExpired reports whether a session id is past its expiry or no longer
// tracked at all.
func (c *Clock) IsExpired(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[id]
	if !ok {
		return true
	}
	return c.now().After(entry.ExpireAt)
}

// Remaining reports how much time is left before a session expires.
func (c *Clock) Remaining(id string) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[id]
	if !ok {
		return 0, false
	}
	return entry.ExpireAt.Sub(c.now()), true
}

// Count returns the number of sessions still tracked by the clock.
func (c *Clock) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// IsActive answers whether a session object still falls inside its TTL
// window.  A nil session is treated as inactive instead of being dereferenced.
func (c *Clock) IsActive(sess *model.Session) bool {
	if sess == nil || sess.ExpireAt.IsZero() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now().Before(sess.ExpireAt)
}

// Evictor is implemented by the session store so the clock can drive the
// eviction of expired sessions.
type Evictor interface {
	Evict(ctx context.Context, id string) error
}

// EvictExpired collects every expired session id and evicts them through the
// supplied evictor.  A failing eviction aborts the sweep so the caller can
// retry later instead of losing state.
func (c *Clock) EvictExpired(ctx context.Context, evictor Evictor) error {
	var expired []string
	c.mu.Lock()
	now := c.now()
	for id, entry := range c.entries {
		if now.After(entry.ExpireAt) {
			expired = append(expired, id)
		}
	}
	c.mu.Unlock()
	for _, id := range expired {
		if err := evictor.Evict(ctx, id); err != nil {
			return fmt.Errorf("evict %s: %w", id, err)
		}
	}
	return nil
}
