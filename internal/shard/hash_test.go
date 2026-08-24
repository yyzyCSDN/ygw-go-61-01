package shard

import "testing"

func TestHashKeyDeterministic(t *testing.T) {
	if HashKey("session-42") != HashKey("session-42") {
		t.Fatal("hash must be deterministic for the same key")
	}
	if HashKey("session-42") == HashKey("session-43") {
		t.Fatal("distinct keys must not collide for this assertion")
	}
}

func TestShardIndexInRange(t *testing.T) {
	for _, key := range []string{"a", "b", "c", "session-1", "session-1000"} {
		index := ShardIndex(key, 3)
		if index < 0 || index >= 3 {
			t.Fatalf("shard index %d out of range for key %q", index, key)
		}
	}
}
