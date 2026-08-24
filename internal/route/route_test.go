package route

import (
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
)

func TestRouteHashFallback(t *testing.T) {
	shards := shard.NewManager([]string{"node-a", "node-b"})
	router := NewRoute(shards)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession("fallback-1", "node-a", time.Hour, now)
	shards.Put(sess)
	node, err := router.Route("fallback-1")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if node != "node-a" {
		t.Fatalf("expected node-a, got %q", node)
	}
}

func TestRouteHashFallbackForUnknown(t *testing.T) {
	shards := shard.NewManager([]string{"node-a", "node-b"})
	router := NewRoute(shards)
	node, err := router.Route("unknown-1")
	if err != nil {
		t.Fatalf("hash fallback must still resolve: %v", err)
	}
	if node != "node-a" && node != "node-b" {
		t.Fatalf("unexpected node %q", node)
	}
}
