package shard

import (
	"fmt"

	"sessionstore/internal/model"
)

// Move relocates a session from its current shard to the shard of the given
// node and updates the owner field on the session itself.
func (m *Manager) Move(id, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	target := m.shardByNodeLocked(nodeID)
	if target == nil {
		return fmt.Errorf("no shard for target node %q", nodeID)
	}
	for _, sh := range m.shards {
		sess, ok := sh.Sessions[id]
		if !ok {
			continue
		}
		if sh.NodeID == nodeID {
			return nil
		}
		sess.OwnerNode = nodeID
		delete(sh.Sessions, id)
		target.Sessions[id] = sess
		return nil
	}
	return model.ErrNotFound
}

// Rebalance walks the sessions of a source shard and moves every entry whose
// current owner differs from the node computed by the hash layout.
func (m *Manager) Rebalance() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	moved := 0
	for _, sh := range m.shards {
		for id, sess := range sh.Sessions {
			wanted := ShardIndex(id, len(m.shards))
			if wanted >= len(m.shards) || m.shards[wanted].NodeID == sh.NodeID {
				continue
			}
			sess.OwnerNode = m.shards[wanted].NodeID
			delete(sh.Sessions, id)
			m.shards[wanted].Sessions[id] = sess
			moved++
		}
	}
	return moved
}
