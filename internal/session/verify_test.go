package session

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

func TestExpiredSessionNoNilPanic(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	clock := ttl.NewClock(time.Hour)
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    clock,
		Versions: version.NewAllocator(1),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, "expired-1", 20*time.Millisecond); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := store.clock.EvictExpired(ctx, store); err != nil {
		t.Fatalf("evict expired failed: %v", err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("reading an expired session panicked: %v", recovered)
		}
	}()
	sess, err := store.Get(ctx, "expired-1")
	if err == nil {
		t.Fatalf("expected an error for the expired session, got %+v", sess)
	}
	if sess != nil {
		t.Fatalf("expired session must not be returned, got %+v", sess)
	}
}
