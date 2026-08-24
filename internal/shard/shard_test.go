package shard

import (
	"testing"
	"time"

	"sessionstore/internal/model"
)

func TestPutGetRemoveRoundtrip(t *testing.T) {
	manager := NewManager([]string{"node-a", "node-b"})
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession("roundtrip-1", "node-a", time.Hour, now)
	manager.Put(sess)
	got, ok := manager.Get("roundtrip-1")
	if !ok || got.ID != sess.ID {
		t.Fatalf("expected stored session, got ok=%v session=%+v", ok, got)
	}
	if owner, ok := manager.OwnerOf("roundtrip-1"); !ok || owner != "node-a" {
		t.Fatalf("expected owner node-a, got %q (ok=%v)", owner, ok)
	}
	manager.Remove("roundtrip-1")
	if _, ok := manager.Get("roundtrip-1"); ok {
		t.Fatal("session must disappear after remove")
	}
}
