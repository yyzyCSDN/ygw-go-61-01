package shard

import (
	"sync"

	"sessionstore/internal/model"
)

// Shard owns the sessions assigned to one storage node.
type Shard struct {
	ID       int
	NodeID   string
	Sessions map[string]*model.Session
}

// Manager keeps the shard table and answers placement questions used by the
// routing and cleanup layers.
type Manager struct {
	mu     sync.RWMutex
	shards []*Shard
}

// NewManager builds one shard per cluster node, in node order.
func NewManager(nodeIDs []string) *Manager {
	shards := make([]*Shard, 0, len(nodeIDs))
	for index, id := range nodeIDs {
		shards = append(shards, &Shard{
			ID:       index,
			NodeID:   id,
			Sessions: make(map[string]*model.Session),
		})
	}
	return &Manager{shards: shards}
}

// NodeFor returns the node responsible for a session id under the current
// shard layout.
func (m *Manager) NodeFor(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	index := ShardIndex(id, len(m.shards))
	if index >= len(m.shards) {
		return ""
	}
	return m.shards[index].NodeID
}

// Put stores a session inside the shard that matches its owner node.
func (m *Manager) Put(sess *model.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sh := m.shardByNodeLocked(sess.OwnerNode); sh != nil {
		sh.Sessions[sess.ID] = sess
	}
}

// Get returns the live session reference stored in the shard table.
func (m *Manager) Get(id string) (*model.Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sh := range m.shards {
		if sess, ok := sh.Sessions[id]; ok {
			return sess, true
		}
	}
	return nil, false
}

// Remove deletes a session from whichever shard currently holds it.
func (m *Manager) Remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sh := range m.shards {
		delete(sh.Sessions, id)
	}
}

// OwnerOf returns the node that currently holds a session id.
func (m *Manager) OwnerOf(id string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sh := range m.shards {
		if _, ok := sh.Sessions[id]; ok {
			return sh.NodeID, true
		}
	}
	return "", false
}

// All returns a consistent snapshot of every stored session.
func (m *Manager) All() []*model.Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var sessions []*model.Session
	for _, sh := range m.shards {
		for _, sess := range sh.Sessions {
			sessions = append(sessions, sess.Clone())
		}
	}
	return sessions
}

// shardByNodeLocked resolves the shard assigned to a node id.
func (m *Manager) shardByNodeLocked(nodeID string) *Shard {
	for _, sh := range m.shards {
		if sh.NodeID == nodeID {
			return sh
		}
	}
	return nil
}
