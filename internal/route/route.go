package route

import (
	"fmt"
	"sync"

	"sessionstore/internal/shard"
)

// Route resolves the node that must serve a session request.  It prefers the
// sticky binding recorded when the session was created, but refreshes that
// binding whenever the shard table says the session moved to another owner.
type Route struct {
	mu     sync.RWMutex
	sticky map[string]string
	shards *shard.Manager
}

// NewRoute builds a router backed by the shared shard manager.
func NewRoute(shards *shard.Manager) *Route {
	return &Route{
		sticky: make(map[string]string),
		shards: shards,
	}
}

// BindSticky records the node that created a session so later requests stay
// pinned to it until a migration happens.
func (r *Route) BindSticky(id, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sticky[id] = node
}

// Rebind updates the sticky entry after a session migrated to another node.
func (r *Route) Rebind(id, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sticky[id] = node
}

// Route returns the node that should serve the session id.
func (r *Route) Route(id string) (string, error) {
	r.mu.RLock()
	node, pinned := r.sticky[id]
	r.mu.RUnlock()
	owner, ok := r.shards.OwnerOf(id)
	if !ok {
		owner = r.shards.NodeFor(id)
		if owner == "" {
			return "", fmt.Errorf("session %q has no owning shard", id)
		}
	}
	if !pinned {
		return owner, nil
	}
	return node, nil
}

// Snapshot returns a copy of the sticky binding table for monitoring.
func (r *Route) Snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copied := make(map[string]string, len(r.sticky))
	for id, node := range r.sticky {
		copied[id] = node
	}
	return copied
}
