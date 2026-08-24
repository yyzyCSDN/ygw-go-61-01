package model

import (
	"fmt"
	"time"
)

// SessionState describes the lifecycle position of a session inside the
// distributed session store.  Every session starts as created, becomes active
// on its first successful read or renewal and is promoted to stale once its
// TTL window passes the warning threshold.  Expired sessions are evicted by
// the TTL clock and disappear from the shard table.
type SessionState int

const (
	StateCreated SessionState = iota
	StateActive
	StateStale
	StateExpired
)

// String returns the stable name of a session state.
func (s SessionState) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateActive:
		return "active"
	case StateStale:
		return "stale"
	case StateExpired:
		return "expired"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Session is the unit of state kept by the store.  It carries the owning node,
// a monotonically increasing version used for conflict detection during
// mirroring and reconnect recovery, plus the TTL policy applied by the clock.
type Session struct {
	ID           string
	State        SessionState
	OwnerNode    string
	Data         map[string]string
	Version      uint64
	CreatedAt    time.Time
	LastActiveAt time.Time
	TTL          time.Duration
	ExpireAt     time.Time
}

// NewSession builds a session in the created state with an empty payload.
func NewSession(id, ownerNode string, ttl time.Duration, now time.Time) *Session {
	return &Session{
		ID:           id,
		State:        StateCreated,
		OwnerNode:    ownerNode,
		Data:         make(map[string]string),
		CreatedAt:    now,
		LastActiveAt: now,
		TTL:          ttl,
		ExpireAt:     now.Add(ttl),
	}
}

// Touch refreshes the last-active watermark and keeps the session alive.
func (s *Session) Touch(now time.Time) {
	s.LastActiveAt = now
}

// Advance moves the session through the lifecycle state machine and rejects
// jumps that skip a stage.
func (s *Session) Advance(to SessionState) error {
	switch s.State {
	case StateCreated:
		if to != StateActive {
			return fmt.Errorf("cannot move created session to %s", to)
		}
	case StateActive:
		if to != StateStale && to != StateExpired {
			return fmt.Errorf("cannot move active session to %s", to)
		}
	case StateStale:
		if to != StateExpired && to != StateActive {
			return fmt.Errorf("cannot move stale session to %s", to)
		}
	case StateExpired:
		return fmt.Errorf("expired session is terminal")
	default:
		return fmt.Errorf("unknown session state %d", int(s.State))
	}
	s.State = to
	return nil
}

// IsExpiredAt reports whether the session is already past its expiration.
func (s *Session) IsExpiredAt(now time.Time) bool {
	return !s.ExpireAt.IsZero() && now.After(s.ExpireAt)
}

// Clone returns a deep copy so callers cannot mutate the stored session
// through a returned reference.
func (s *Session) Clone() *Session {
	data := make(map[string]string, len(s.Data))
	for key, value := range s.Data {
		data[key] = value
	}
	copied := *s
	copied.Data = data
	return &copied
}
