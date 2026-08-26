package sync

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
)

func TestReconnectSyncUsesLatestVersion(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	mirror := NewMemoryMirror()
	manager := NewManager(mirror, route.NewRoute(shards), shards)
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	local := model.NewSession("reconnect-1", "node-a", time.Hour, now)
	local.Version = 5
	if err := mirror.Write(ctx, local); err != nil {
		t.Fatalf("write local failed: %v", err)
	}
	stale := model.NewSession("reconnect-1", "node-a", time.Hour, now)
	stale.Version = 3
	if err := manager.Reconnect(ctx, "reconnect-1", stale); err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	got, err := mirror.Read(ctx, "reconnect-1")
	if err != nil {
		t.Fatalf("read after reconnect failed: %v", err)
	}
	if got.Version != 5 {
		t.Fatalf("reconnect rolled the session back to version %d, expected 5", got.Version)
	}
}
