package ttl

import (
	"context"
	"errors"
	"testing"
	"time"
)

// errEvict is the sentinel returned by failingEvictor.
var errEvict = errors.New("evict failed")

// failingEvictor reports an eviction failure for a given id so the sweep is
// forced to surface the error instead of swallowing it.
type failingEvictor struct {
	calls int
}

func (f *failingEvictor) Evict(_ context.Context, _ string) error {
	f.calls++
	return errEvict
}

// TestEvictExpiredSurfacesEvictionError verifies that a failing eviction
// aborts the sweep and returns the error so the caller can retry the next
// tick rather than losing the session state.
func TestEvictExpiredSurfacesEvictionError(t *testing.T) {
	clock := NewClock(time.Hour)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock.now = func() time.Time { return now }
	clock.Register("expired-1", time.Hour)

	// Advance time past the expiry window so the session is swept.
	clock.now = func() time.Time { return now.Add(2 * time.Hour) }

	evictor := &failingEvictor{}
	err := clock.EvictExpired(context.Background(), evictor)
	if err == nil {
		t.Fatal("EvictExpired must surface the eviction failure instead of returning nil")
	}
	if !errors.Is(err, errEvict) {
		t.Fatalf("expected error to wrap errEvict, got %v", err)
	}
	if evictor.calls != 1 {
		t.Fatalf("expected the failing session to be evicted once, got %d calls", evictor.calls)
	}
}

// noopEvictor records evictions without failing.
type noopEvictor struct {
	evicted []string
}

func (n *noopEvictor) Evict(_ context.Context, id string) error {
	n.evicted = append(n.evicted, id)
	return nil
}

// TestEvictExpiredSweepsAllExpiredOnSuccess confirms the happy path still
// evicts every expired session when nothing fails.
func TestEvictExpiredSweepsAllExpiredOnSuccess(t *testing.T) {
	clock := NewClock(time.Hour)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock.now = func() time.Time { return now }
	clock.Register("e1", time.Hour)
	clock.Register("e2", time.Hour)
	clock.now = func() time.Time { return now.Add(2 * time.Hour) }

	evictor := &noopEvictor{}
	if err := clock.EvictExpired(context.Background(), evictor); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(evictor.evicted) != 2 {
		t.Fatalf("expected 2 evictions, got %d", len(evictor.evicted))
	}
}
