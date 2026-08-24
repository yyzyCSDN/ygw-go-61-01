package model

import "fmt"

// NodeState describes whether a storage node currently accepts sessions.
type NodeState int

const (
	NodeUp NodeState = iota
	NodeDown
	NodeMigrating
)

// String returns the stable name of a node state.
func (s NodeState) String() string {
	switch s {
	case NodeUp:
		return "up"
	case NodeDown:
		return "down"
	case NodeMigrating:
		return "migrating"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Node is a participant of the session cluster.  The address is used by the
// HTTP layer when a request has to be forwarded to the owner of a session.
type Node struct {
	ID     string
	Addr   string
	Region string
	Weight int
	State  NodeState
}

// NewNode creates an up node with the given identity.
func NewNode(id, addr, region string, weight int) *Node {
	return &Node{
		ID:     id,
		Addr:   addr,
		Region: region,
		Weight: weight,
		State:  NodeUp,
	}
}
