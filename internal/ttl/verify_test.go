package ttl

import (
	"testing"
	"time"
)

func TestRenewExtendsSessionTTL(t *testing.T) {
	clock := NewClock(30 * time.Minute)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock.now = func() time.Time { return now }
	clock.Register("renew-1", time.Hour)
	before, ok := clock.Remaining("renew-1")
	if !ok {
		t.Fatal("session must be tracked by the clock")
	}
	if before < 50*time.Minute || before > time.Hour {
		t.Fatalf("unexpected initial remaining TTL: %v", before)
	}
	clock.Refresh("renew-1", 2*time.Hour)
	after, ok := clock.Remaining("renew-1")
	if !ok {
		t.Fatal("session disappeared from the clock after renewal")
	}
	if after <= before {
		t.Fatalf("renewal did not extend the TTL window: before=%v after=%v", before, after)
	}
	if after < 110*time.Minute {
		t.Fatalf("renewed session must keep a full fresh window, got %v", after)
	}
}
