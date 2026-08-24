package model

import "time"

// TTLPolicy clamps requested session lifetimes into a sane operational range
// so a misconfigured client cannot create sessions that live forever or die
// before the first request round trip.
type TTLPolicy struct {
	Min     time.Duration
	Max     time.Duration
	Default time.Duration
}

// DefaultTTLPolicy returns the policy used by the demo server.
func DefaultTTLPolicy() TTLPolicy {
	return TTLPolicy{
		Min:     30 * time.Second,
		Max:     24 * time.Hour,
		Default: 30 * time.Minute,
	}
}

// Clamp bounds a requested TTL to the policy range.
func (p TTLPolicy) Clamp(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return p.Default
	}
	if ttl < p.Min {
		return p.Min
	}
	if ttl > p.Max {
		return p.Max
	}
	return ttl
}

// TTLFromSeconds converts a client-supplied second count into a duration
// using the policy defaults when the value is absent.
func TTLFromSeconds(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}
