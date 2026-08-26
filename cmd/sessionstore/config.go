package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config carries every tunable of the session store server.
type Config struct {
	Addr          string
	DefaultTTL    time.Duration
	ReadTimeout   time.Duration
	EvictInterval time.Duration
	CleanInterval time.Duration
	CleanBatch    int
	NodeIDs       []string
	NodeAddrs     []string
	WebRoot       string
}

// DefaultConfig returns the configuration used when no environment override
// is present.
func DefaultConfig() Config {
	return Config{
		Addr:          ":8080",
		DefaultTTL:    30 * time.Minute,
		ReadTimeout:   2 * time.Second,
		EvictInterval: 10 * time.Second,
		CleanInterval: 30 * time.Second,
		CleanBatch:    100,
		NodeIDs:       []string{"node-a", "node-b", "node-c"},
		NodeAddrs:     []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
		WebRoot:       "web",
	}
}

// LoadConfig starts from defaults and applies environment overrides.
func LoadConfig() Config {
	cfg := DefaultConfig()
	if value := os.Getenv("SESSIONSTORE_ADDR"); value != "" {
		cfg.Addr = value
	}
	if value := durationEnv("SESSIONSTORE_TTL", 0); value > 0 {
		cfg.DefaultTTL = value
	}
	if value := durationEnv("SESSIONSTORE_READ_TIMEOUT", 0); value > 0 {
		cfg.ReadTimeout = value
	}
	if value := durationEnv("SESSIONSTORE_EVICT_INTERVAL", 0); value > 0 {
		cfg.EvictInterval = value
	}
	if value := durationEnv("SESSIONSTORE_CLEAN_INTERVAL", 0); value > 0 {
		cfg.CleanInterval = value
	}
	if value := os.Getenv("SESSIONSTORE_CLEAN_BATCH"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.CleanBatch = parsed
		}
	}
	if value := os.Getenv("SESSIONSTORE_NODES"); value != "" {
		cfg.NodeIDs = splitTrim(value)
	}
	if value := os.Getenv("SESSIONSTORE_NODE_ADDRS"); value != "" {
		cfg.NodeAddrs = splitTrim(value)
	}
	if value := os.Getenv("SESSIONSTORE_WEB_ROOT"); value != "" {
		cfg.WebRoot = value
	}
	return cfg
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// Validate checks that the configuration can start a sane server.
func (c Config) Validate() error {
	var problems []string
	if c.Addr == "" {
		problems = append(problems, "addr must not be empty")
	}
	if len(c.NodeIDs) == 0 {
		problems = append(problems, "at least one node id is required")
	}
	if len(c.NodeAddrs) != 0 && len(c.NodeAddrs) != len(c.NodeIDs) {
		problems = append(problems, "node addresses must match node ids")
	}
	if c.DefaultTTL <= 0 {
		problems = append(problems, "default ttl must be positive")
	}
	if c.ReadTimeout <= 0 {
		problems = append(problems, "read timeout must be positive")
	}
	if c.CleanBatch <= 0 {
		problems = append(problems, "clean batch must be positive")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %w", errors.New(strings.Join(problems, "; ")))
	}
	return nil
}
