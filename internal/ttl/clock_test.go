package ttl

import (
	"testing"
	"time"

	"sessionstore/internal/model"
)

func TestRegisterActiveWithinWindow(t *testing.T) {
	clock := NewClock(30 * time.Minute)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock.now = func() time.Time { return now }
	clock.Register("s1", time.Hour)
	sess := model.NewSession("s1", "node-a", time.Hour, now)
	if !clock.IsActive(sess) {
		t.Fatal("freshly registered session must be active")
	}
	if clock.IsExpired("s1") {
		t.Fatal("freshly registered session must not be expired")
	}
}

func TestIsExpiredForUnknownSession(t *testing.T) {
	clock := NewClock(30 * time.Minute)
	if !clock.IsExpired("missing") {
		t.Fatal("unknown session must be considered expired")
	}
}
