package shard

import "github.com/cespare/xxhash/v2"

// HashKey computes the 64-bit hash used for session placement.  The hash is
// stable across restarts so a session always lands on the same shard index
// unless the cluster size changes.
func HashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}

// ShardIndex maps a session id onto one of the configured shards.
func ShardIndex(key string, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	return int(HashKey(key) % uint64(shardCount))
}
