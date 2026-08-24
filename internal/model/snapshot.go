package model

import "fmt"

// ErrNotFound is returned whenever a session id has no entry at all.
var ErrNotFound = fmt.Errorf("session not found")

// ErrExpired is returned when a session exists but its TTL window has closed.
var ErrExpired = fmt.Errorf("session expired")

// ErrExists is returned when a create operation collides with a live session.
var ErrExists = fmt.Errorf("session already exists")

// Snapshot is a point-in-time view of the cluster used by the monitor page.
type Snapshot struct {
	TotalSessions int
	Active        int
	Stale         int
	Expired       int
	Nodes         []NodeInfo
	SyncStates    map[string]string
}

// NodeInfo carries the per-node counters shown on the monitoring page.
type NodeInfo struct {
	ID       string
	Addr     string
	State    string
	Sessions int
}
