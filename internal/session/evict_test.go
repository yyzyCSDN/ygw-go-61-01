package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

// failingSaver reports a save failure once, then succeeds, so a test can
// exercise the retry path of the save-before-evict handshake.
type failingSaver struct {
	saved    map[string]*model.Session
	failNext bool
}

func (f *failingSaver) SaveBeforeEvict(_ context.Context, sess *model.Session) error {
	if f.failNext {
		f.failNext = false
		return errSaveFailed
	}
	if f.saved == nil {
		f.saved = make(map[string]*model.Session)
	}
	f.saved[sess.ID] = sess.Clone()
	return nil
}

func (f *failingSaver) Snapshot(id string) (*model.Session, bool) {
	sess, ok := f.saved[id]
	return sess, ok
}

// errSaveFailed is the sentinel returned by failingSaver.
var errSaveFailed = errors.New("save failed")

// TestEvictSaveFailureReportsErrorAndRetainsState verifies that when the
// save-before-evict handshake fails, Evict surfaces the error and leaves the
// session intact in the backend and shard table so the next sweep can retry
// instead of losing state.
func TestEvictSaveFailureReportsErrorAndRetainsState(t *testing.T) {
	saver := &failingSaver{failNext: true}
	shards := shard.NewManager([]string{"node-a", "node-b"})
	backend := NewMemoryBackend()
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Saver:    saver,
		Backend:  backend,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, "evict-fail", time.Hour); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// First eviction: save fails. The error must surface and the session
	// must be retained for retry.
	err = store.Evict(ctx, "evict-fail")
	if err == nil {
		t.Fatal("evict must surface the save failure instead of swallowing it")
	}
	if !errors.Is(err, errSaveFailed) {
		t.Fatalf("expected error to wrap errSaveFailed, got %v", err)
	}
	if backend.Tombstoned("evict-fail") {
		t.Fatal("session must not be tombstoned when the save failed")
	}
	if _, ok := shards.Get("evict-fail"); !ok {
		t.Fatal("session must stay in the shard table when the save failed")
	}
	if _, ok := saver.Snapshot("evict-fail"); ok {
		t.Fatal("no snapshot must exist when the save failed")
	}

	// Second eviction: save succeeds. The handshake now completes and the
	// snapshot is present for recovery.
	if err := store.Evict(ctx, "evict-fail"); err != nil {
		t.Fatalf("retry evict failed: %v", err)
	}
	saved, ok := saver.Snapshot("evict-fail")
	if !ok || saved.ID != "evict-fail" {
		t.Fatalf("expected saved snapshot after retry, got ok=%v", ok)
	}
}
