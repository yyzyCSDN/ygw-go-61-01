package sync

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
)

func TestStateMachineTransitions(t *testing.T) {
	machine := &StateMachine{}
	if err := machine.Transition(SyncLocal, SyncSyncing); err != nil {
		t.Fatalf("local -> syncing must be legal: %v", err)
	}
	if err := machine.Transition(SyncSyncing, SyncConsistent); err != nil {
		t.Fatalf("syncing -> consistent must be legal: %v", err)
	}
	if err := machine.Transition(SyncLocal, SyncConsistent); err == nil {
		t.Fatal("local -> consistent must be rejected")
	}
}

func TestMirrorRoundtrip(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	mirror := NewMemoryMirror()
	manager := NewManager(mirror, route.NewRoute(shards), shards)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession("mirror-1", "node-a", time.Hour, now)
	if err := manager.Mirror(context.Background(), sess); err != nil {
		t.Fatalf("mirror failed: %v", err)
	}
	got, err := mirror.Read(context.Background(), "mirror-1")
	if err != nil {
		t.Fatalf("mirror read failed: %v", err)
	}
	if got.ID != "mirror-1" {
		t.Fatalf("unexpected mirror content: %+v", got)
	}
	if manager.StateOf("mirror-1") != SyncConsistent {
		t.Fatalf("expected consistent state, got %s", manager.StateOf("mirror-1"))
	}
}
