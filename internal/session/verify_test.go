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

type failSnapshotSaver struct {
	err error
}

func (f failSnapshotSaver) SaveBeforeEvict(ctx context.Context, sess *model.Session) error {
	return f.err
}

func TestEvictionSaveErrorNotSwallowed(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Saver:    failSnapshotSaver{err: errors.New("durable store is full")},
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, "evict-fail-1", time.Hour); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	err = store.Evict(ctx, "evict-fail-1")
	if err == nil {
		t.Fatal("eviction must report the save failure instead of swallowing it")
	}
	if _, err := store.Get(ctx, "evict-fail-1"); err != nil {
		t.Fatalf("session must survive a failed eviction save, got error: %v", err)
	}
}
