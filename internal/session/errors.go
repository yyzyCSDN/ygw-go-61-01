package session

import "errors"

// ErrMissingConfig is returned when the store configuration is incomplete.
var ErrMissingConfig = errors.New("session store requires shards, clock, versions and backend")
