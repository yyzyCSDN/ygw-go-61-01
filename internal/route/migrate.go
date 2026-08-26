package route

// Migrate re-pins every sticky binding whose session now belongs to another
// node.  It is invoked after a bulk shard relocation so routing converges
// without individual requests paying for the refresh.
func (r *Route) Migrate(bindings map[string]string) int {
	updated := 0
	for id, node := range bindings {
		owner, ok := r.shards.OwnerOf(id)
		if ok && owner != node {
			r.Rebind(id, owner)
			updated++
		}
	}
	return updated
}
