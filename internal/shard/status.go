package shard

// ShardStatus describes one shard for the monitoring page.
type ShardStatus struct {
	ShardID      int
	NodeID       string
	SessionCount int
}

// Status returns a consistent snapshot of every shard's load.
func (m *Manager) Status() []ShardStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	status := make([]ShardStatus, 0, len(m.shards))
	for _, shard := range m.shards {
		status = append(status, ShardStatus{
			ShardID:      shard.ID,
			NodeID:       shard.NodeID,
			SessionCount: len(shard.Sessions),
		})
	}
	return status
}
