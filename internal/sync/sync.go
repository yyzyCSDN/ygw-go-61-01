package sync

import (
	"context"
	"fmt"
	"sync"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
	"sessionstore/internal/version"
)

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
func (m *Manager) Mirror(ctx context.Context, sess *model.Session) error {
	if err := m.MarkSyncing(sess.ID); err != nil {
		return err
	}
	if err := m.backend.Write(ctx, sess); err != nil {
		m.mu.Lock()
		m.states[sess.ID] = SyncLocal
		m.mu.Unlock()
		return MirrorError(sess.ID, err)
	}
	return m.MarkConsistent(sess.ID)
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
	if err := m.backend.Write(ctx, sess); err != nil {
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
func (m *Manager) Reconnect(ctx context.Context, id string, remote *model.Session) error {
	local, err := m.backend.Read(ctx, id)
	if err != nil && err != model.ErrNotFound {
		return fmt.Errorf("read local %s: %w", id, err)
	}
	if local == nil || version.IsNewer(remote.Version, local.Version) {
		if err := m.backend.Write(ctx, remote); err != nil {
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
