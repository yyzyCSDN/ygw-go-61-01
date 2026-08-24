package session

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

type slowBackend struct{}

func (slowBackend) Read(ctx context.Context, id string) (*model.Session, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (slowBackend) Write(ctx context.Context, id string, sess *model.Session) error {
	return nil
}

func (slowBackend) Tombstone(ctx context.Context, id string) error {
	return nil
}

func TestSessionReadHonorsTimeout(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Backend:  slowBackend{},
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := store.Get(ctx, "slow-1")
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected a timeout error from the slow read")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session read did not honor the context timeout")
	}
}
