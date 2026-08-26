package route

import (
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
)

func TestStickyRouteUpdatedAfterMigration(t *testing.T) {
	shards := shard.NewManager([]string{"node-a", "node-b"})
	router := NewRoute(shards)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession("migrate-1", "node-a", time.Hour, now)
	shards.Put(sess)
	router.BindSticky("migrate-1", "node-a")
	if err := shards.Move("migrate-1", "node-b"); err != nil {
		t.Fatalf("move failed: %v", err)
	}
	node, err := router.Route("migrate-1")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if node != "node-b" {
		t.Fatalf("route still uses the stale node %q after migration, expected node-b", node)
	}
	snapshot := router.Snapshot()
	if pinned, ok := snapshot["migrate-1"]; !ok || pinned != "node-b" {
		t.Fatalf("sticky binding was not refreshed: got %q (present=%v)", pinned, ok)
	}
}
