package sync

import "fmt"

// State is the mirror state machine of a session: local, syncing while a
// mirror write is in flight, and consistent once the mirror acknowledged it.
type State int

const (
	SyncLocal State = iota
	SyncSyncing
	SyncConsistent
)

// String returns the stable name of a sync state.
func (s State) String() string {
	switch s {
	case SyncLocal:
		return "local"
	case SyncSyncing:
		return "syncing"
	case SyncConsistent:
		return "consistent"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// StateMachine is a tiny helper that enforces the legal transitions of the
// mirror state machine and records the last invalid attempt.
type StateMachine struct {
	LastInvalid string
}

// Transition moves from one state to another, returning an error when the
// requested jump is not part of the local -> syncing -> consistent graph.
func (m *StateMachine) Transition(from, to State) error {
	switch from {
	case SyncLocal:
		if to != SyncSyncing {
			m.LastInvalid = fmt.Sprintf("local -> %s", to)
			return fmt.Errorf("invalid transition local -> %s", to)
		}
	case SyncSyncing:
		if to != SyncConsistent && to != SyncLocal {
			m.LastInvalid = fmt.Sprintf("syncing -> %s", to)
			return fmt.Errorf("invalid transition syncing -> %s", to)
		}
	case SyncConsistent:
		if to != SyncSyncing {
			m.LastInvalid = fmt.Sprintf("consistent -> %s", to)
			return fmt.Errorf("invalid transition consistent -> %s", to)
		}
	default:
		return fmt.Errorf("unknown sync state %d", int(from))
	}
	return nil
}
