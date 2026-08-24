package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
	"sessionstore/internal/version"
)

// retryAttempts bounds how many times a mirror write is retried before the
// failure is surfaced.  A mirror write is the line of defence that keeps a
// session recoverable after a node failover, so transient backend hiccups
// must not be allowed to lose state.  The attempt count is conservative so a
// genuinely broken backend does not stall callers indefinitely.
const retryAttempts = 3

// retryDelay is the brief back-off applied between mirror write attempts.  It
// is small because the failures we are defending against are short lived
// (a slow disk flush, a momentary lock contention); a permanent outage is
// reported after retryAttempts instead of being hidden by long waits.
const retryDelay = 20 * time.Millisecond

// Manager coordinates the mirror state of every session and drives migration
// and reconnect recovery.
type Manager struct {
	mu      sync.Mutex
	states  map[string]State
	backend MirrorBackend
	route   *route.Route
	shards  *shard.Manager
	machine *StateMachine
}

// NewManager wires the mirror backend, router and shard table together.
func NewManager(backend MirrorBackend, router *route.Route, shards *shard.Manager) *Manager {
	return &Manager{
		states:  make(map[string]State),
		backend: backend,
		route:   router,
		shards:  shards,
		machine: &StateMachine{},
	}
}

// MarkSyncing moves a session into the syncing state for mirroring.
func (m *Manager) MarkSyncing(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.states[id]
	if err := m.machine.Transition(current, SyncSyncing); err != nil {
		return err
	}
	m.states[id] = SyncSyncing
	return nil
}

// MarkConsistent records that a mirror write completed for a session.
func (m *Manager) MarkConsistent(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.states[id]
	if err := m.machine.Transition(current, SyncConsistent); err != nil {
		return err
	}
	m.states[id] = SyncConsistent
	return nil
}

// StateOf returns the current mirror state of a session.
func (m *Manager) StateOf(id string) State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[id]
}

// Mirror writes a session snapshot to the mirror backend and advances the
// state machine.  Any backend failure is propagated so the caller can decide
// to retry instead of silently losing the mirror.
//
// A failed write is retried a bounded number of times before the error is
// surfaced; if every attempt fails the session is rolled back to the local
// state so a later failover can tell the mirror is stale instead of being
// lied to that it is consistent.
func (m *Manager) Mirror(ctx context.Context, sess *model.Session) error {
	if err := m.MarkSyncing(sess.ID); err != nil {
		return err
	}
	if err := m.writeMirrored(ctx, sess); err != nil {
		// The mirror never acknowledged the snapshot.  Roll the state machine
		// back to local so the table does not claim a consistency that the
		// mirror does not have, then report the failure honestly.
		_ = m.markLocal(sess.ID)
		return MirrorError(sess.ID, err)
	}
	return m.MarkConsistent(sess.ID)
}

// writeMirrored persists a session snapshot to the mirror backend with a
// bounded retry loop.  It is shared by Mirror, MigrateSession and Reconnect
// so every mirror write benefits from the same resilience and reporting.
// A cancelled context short-circuits the loop without consuming the remaining
// attempts.
func (m *Manager) writeMirrored(ctx context.Context, sess *model.Session) error {
	var last error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := m.backend.Write(ctx, sess)
		if err == nil {
			return nil
		}
		last = err
		// A cancelled context means the caller is no longer waiting; do not
		// burn the remaining attempts sleeping.
		if ctx.Err() != nil {
			return err
		}
		if attempt+1 < retryAttempts {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return last
}

// markLocal moves a session back to the local state after a mirror write
// failed, so the state table reflects that the mirror is stale.  It is best
// effort: a transition that the state machine rejects (for example because the
// session was already consistent) is ignored rather than masking the original
// write error.
func (m *Manager) markLocal(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.states[id]
	if err := m.machine.Transition(current, SyncLocal); err != nil {
		return nil
	}
	m.states[id] = SyncLocal
	return nil
}

// MigrateSession relocates a session to a new owner node, updates the mirror
// and re-pins the sticky route so subsequent requests converge immediately.
func (m *Manager) MigrateSession(ctx context.Context, id, newNode string) error {
	sess, err := m.backend.Read(ctx, id)
	if err != nil {
		return fmt.Errorf("read before migrate %s: %w", id, err)
	}
	if err := m.shards.Move(id, newNode); err != nil {
		return fmt.Errorf("move session %s: %w", id, err)
	}
	sess.OwnerNode = newNode
	m.route.Rebind(id, newNode)
	if err := m.writeMirrored(ctx, sess); err != nil {
		// The relocation already happened, but the mirror did not confirm the
		// new owner.  Roll the state back to local so the table does not claim
		// consistency and the next reconcile/failover knows the mirror is
		// stale, then surface the failure.
		_ = m.markLocal(id)
		return MirrorError(id, err)
	}
	m.mu.Lock()
	m.states[id] = SyncConsistent
	m.mu.Unlock()
	return nil
}

// Reconnect reconciles a session after the node lost contact with the mirror.
// The remote snapshot is applied only when its version is newer than the
// local copy, so a stale peer can never roll the session backwards.
//
// A nil remote snapshot means the peer has nothing to offer; the local state
// is preserved and the session stays consistent rather than being wiped.  The
// reconcile write goes through the shared retry loop so a transient mirror
// hiccup does not lose the recovered state to a failover.
func (m *Manager) Reconnect(ctx context.Context, id string, remote *model.Session) error {
	if remote == nil {
		return fmt.Errorf("reconnect %s: remote snapshot is nil", id)
	}
	local, err := m.backend.Read(ctx, id)
	if err != nil && err != model.ErrNotFound {
		return fmt.Errorf("read local %s: %w", id, err)
	}
	if local == nil || version.IsNewer(remote.Version, local.Version) {
		if err := m.writeMirrored(ctx, remote); err != nil {
			_ = m.markLocal(id)
			return MirrorError(id, err)
		}
	}
	m.mu.Lock()
	m.states[id] = SyncConsistent
	m.mu.Unlock()
	return nil
}

// Snapshot returns the mirror state table for the monitor page.
func (m *Manager) Snapshot() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]string, len(m.states))
	for id, state := range m.states {
		result[id] = state.String()
	}
	return result
}

// PendingCount returns how many sessions are currently in the syncing state.
func (m *Manager) PendingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := 0
	for _, state := range m.states {
		if state == SyncSyncing {
			pending++
		}
	}
	return pending
}
