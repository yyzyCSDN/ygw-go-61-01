package model

import (
	"testing"
	"time"
)

func TestCloneDeepCopiesData(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := NewSession("s1", "node-a", time.Hour, now)
	sess.Data["theme"] = "dark"
	copied := sess.Clone()
	copied.Data["theme"] = "light"
	if sess.Data["theme"] != "dark" {
		t.Fatalf("clone shares the data map: got %q", sess.Data["theme"])
	}
	if copied.Version != sess.Version {
		t.Fatalf("clone changed identity fields")
	}
}

func TestSessionStateString(t *testing.T) {
	expected := map[SessionState]string{
		StateCreated: "created",
		StateActive:  "active",
		StateStale:   "stale",
		StateExpired: "expired",
	}
	for state, name := range expected {
		if state.String() != name {
			t.Fatalf("state %d: expected %q, got %q", int(state), name, state.String())
		}
	}
}

func TestIsExpiredAt(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := NewSession("s1", "node-a", time.Hour, now)
	if sess.IsExpiredAt(now.Add(30 * time.Minute)) {
		t.Fatal("session must still be alive inside its TTL window")
	}
	if !sess.IsExpiredAt(now.Add(2 * time.Hour)) {
		t.Fatal("session must be expired after its TTL window")
	}
}
