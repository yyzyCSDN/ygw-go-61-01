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

func newTestStore(t *testing.T) (*Store, *shard.Manager, *MemorySaver) {
	t.Helper()
	shards := shard.NewManager([]string{"node-a", "node-b"})
	saver := NewMemorySaver()
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Saver:    saver,
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store, shards, saver
}

func TestCreateGetRoundtrip(t *testing.T) {
	store, shards, _ := newTestStore(t)
	ctx := context.Background()
	sess, err := store.Create(ctx, "sess-1", time.Hour)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if sess.State != model.StateActive {
		t.Fatalf("expected active state, got %s", sess.State)
	}
	if _, ok := shards.Get("sess-1"); !ok {
		t.Fatal("session must be stored in the shard table")
	}
	got, err := store.Get(ctx, "sess-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.ID != "sess-1" {
		t.Fatalf("unexpected session: %+v", got)
	}
}

func TestGetMissingSession(t *testing.T) {
	store, _, _ := newTestStore(t)
	_, err := store.Get(context.Background(), "missing-1")
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestEvictSavesSnapshot(t *testing.T) {
	store, _, saver := newTestStore(t)
	ctx := context.Background()
	if _, err := store.Create(ctx, "evict-1", time.Hour); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if err := store.Evict(ctx, "evict-1"); err != nil {
		t.Fatalf("evict failed: %v", err)
	}
	saved, ok := saver.Snapshot("evict-1")
	if !ok || saved.ID != "evict-1" {
		t.Fatalf("expected saved snapshot, got ok=%v", ok)
	}
}
