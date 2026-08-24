package session

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

func TestSessionLockReleased(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, "lock-1", time.Hour); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- store.Renew(ctx, "lock-1")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("renew failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("renew did not return")
	}
	acquired := make(chan struct{})
	go func() {
		store.mu.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("store lock was not released after the renew operation")
	}
}
