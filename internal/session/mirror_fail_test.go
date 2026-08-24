package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

// failingMirror is a MirrorSink that always returns a recorded error from
// Mirror, so the store layer can be exercised for honest failure reporting.
type failingMirror struct {
	mu  sync.Mutex
	err error
}

func newFailingMirror(err error) *failingMirror {
	return &failingMirror{err: err}
}

func (f *failingMirror) Mirror(ctx context.Context, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return f.err
}

func newStoreWithMirror(t *testing.T, mirror MirrorSink) (*Store, *shard.Manager) {
	t.Helper()
	shards := shard.NewManager([]string{"node-a", "node-b"})
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Mirror:   mirror,
		Saver:    NewMemorySaver(),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store, shards
}

// TestCreatePropagatesMirrorError confirms that a mirror failure during Create
// is surfaced.  This path already reported the error before the fix and must
// keep doing so; it anchors the honest-reporting contract the fix extends to
// Renew.
func TestCreatePropagatesMirrorError(t *testing.T) {
	boom := errors.New("mirror down")
	store, _ := newStoreWithMirror(t, newFailingMirror(boom))
	if _, err := store.Create(context.Background(), "create-err", time.Hour); err == nil {
		t.Fatal("Create with failing mirror must surface the error")
	} else if !errors.Is(err, boom) {
		t.Fatalf("Create error must wrap the mirror cause, got %v", err)
	}
}

// TestRenewPropagatesMirrorError is the regression for the second swallowed
// error: Renew used to discard a mirror failure ("_ = s.mirror.Mirror(...)"),
// so a renewal that did not reach the mirror silently lost the version bump.
// On failover, Reconnect would then adopt the stale mirror and roll the
// session back.  Now Renew must report the mirror error like Create and
// BatchRenew do.
func TestRenewPropagatesMirrorError(t *testing.T) {
	boom := errors.New("mirror down")

	// Seed a session against a store with no mirror so Renew has a live record.
	seedShards := shard.NewManager([]string{"node-a", "node-b"})
	seedStore, err := NewStore(StoreConfig{
		Shards:   seedShards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Saver:    NewMemorySaver(),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if _, err := seedStore.Create(context.Background(), "renew-err", time.Hour); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	// Build the store under test sharing the same backend/clock so it sees the
	// seeded session, but wire in the failing mirror.
	store, err := NewStore(StoreConfig{
		Shards:   shard.NewManager([]string{"node-a", "node-b"}),
		Clock:    seedStore.clock,
		Versions: version.NewAllocator(100),
		Saver:    NewMemorySaver(),
		Backend:  seedStore.backend,
		Mirror:   newFailingMirror(boom),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// The seeded session lives in the original shard table; make the new shard
	// table aware of it too so Renew finds the owner.
	if sess, err := store.backend.Read(context.Background(), "renew-err"); err == nil {
		store.shards.Put(sess)
	}

	err = store.Renew(context.Background(), "renew-err")
	if err == nil {
		t.Fatal("Renew must return the mirror error instead of swallowing it")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Renew error must wrap the mirror cause, got %v", err)
	}
}
