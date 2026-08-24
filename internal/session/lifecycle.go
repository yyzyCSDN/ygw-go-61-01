package session

import (
	"context"
	"fmt"
	"time"

	"sessionstore/internal/model"
)

// lifecycle constants drive the created/active/stale/expired transitions.
const staleThreshold = 0.8

// Activate promotes a session from created to active and stamps its activity
// watermark.
func (s *Store) Activate(ctx context.Context, sess *model.Session) error {
	switch sess.State {
	case model.StateCreated, model.StateStale:
		if err := sess.Advance(model.StateActive); err != nil {
			return err
		}
		sess.Touch(s.now())
		sess.Version = s.versions.Next()
		return s.backend.Write(ctx, sess.ID, sess)
	case model.StateActive:
		return nil
	default:
		return fmt.Errorf("cannot activate session in state %s", sess.State)
	}
}

// MarkStale flags a session whose TTL window is close to closing.
func (s *Store) MarkStale(ctx context.Context, sess *model.Session, now time.Time) error {
	if sess.State != model.StateActive {
		return nil
	}
	remaining := now.Sub(sess.ExpireAt)
	if remaining < 0 {
		remaining = -remaining
	}
	window := sess.TTL
	if window <= 0 {
		window = time.Hour
	}
	if remaining <= time.Duration(float64(window)*staleThreshold) {
		if err := sess.Advance(model.StateStale); err != nil {
			return err
		}
		return s.backend.Write(ctx, sess.ID, sess)
	}
	return nil
}
