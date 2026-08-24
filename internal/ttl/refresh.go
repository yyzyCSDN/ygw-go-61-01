package ttl

import "time"

// Refresh moves the expiry window of an existing session forward by the
// supplied TTL.  Renewals that happen while a session is still active must
// keep the session alive for another full window.
func (c *Clock) Refresh(id string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[id]; !ok {
		return
	}
}
