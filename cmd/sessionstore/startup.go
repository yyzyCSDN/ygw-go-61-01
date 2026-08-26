package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"sessionstore/internal/clean"
	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/session"
	"sessionstore/internal/shard"
	"sessionstore/internal/sync"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

// Dependencies bundles every component built during startup.
type Dependencies struct {
	Config  Config
	Nodes   []*model.Node
	Shards  *shard.Manager
	Clock   *ttl.Clock
	Router  *route.Route
	Mirror  *sync.MemoryMirror
	Sync    *sync.Manager
	Store   *session.Store
	Scanner *clean.Scanner
	Backend *session.MemoryBackend
	Saver   *session.MemorySaver
}

// Summary renders a human readable description of the running dependency
// graph for the startup log.
func (d *Dependencies) Summary() string {
	return "addr=" + d.Config.Addr +
		" nodes=" + strings.Join(d.Config.NodeIDs, ",") +
		" ttl=" + d.Config.DefaultTTL.String() +
		" batch=" + strconv.Itoa(d.Config.CleanBatch)
}

// BuildDependencies wires the full component graph from the configuration.
func BuildDependencies(cfg Config) (*Dependencies, error) {
	nodes := make([]*model.Node, 0, len(cfg.NodeIDs))
	for index, id := range cfg.NodeIDs {
		addr := ""
		if index < len(cfg.NodeAddrs) {
			addr = cfg.NodeAddrs[index]
		}
		nodes = append(nodes, model.NewNode(id, addr, "default", 1))
	}
	shards := shard.NewManager(cfg.NodeIDs)
	clock := ttl.NewClock(cfg.DefaultTTL)
	router := route.NewRoute(shards)
	mirror := sync.NewMemoryMirror()
	syncMgr := sync.NewManager(mirror, router, shards)
	backend := session.NewMemoryBackend()
	saver := session.NewMemorySaver()
	store, err := session.NewStore(session.StoreConfig{
		Shards:   shards,
		Clock:    clock,
		Versions: version.NewAllocator(1),
		Mirror:   syncMgr,
		Saver:    saver,
		Backend:  backend,
	})
	if err != nil {
		return nil, err
	}
	scanner := clean.NewScanner(store, clock.IsExpired, cfg.CleanBatch)
	return &Dependencies{
		Config:  cfg,
		Nodes:   nodes,
		Shards:  shards,
		Clock:   clock,
		Router:  router,
		Mirror:  mirror,
		Sync:    syncMgr,
		Store:   store,
		Scanner: scanner,
		Backend: backend,
		Saver:   saver,
	}, nil
}

// RunBackground starts the eviction and cleanup loops until the context is
// cancelled.
func (d *Dependencies) RunBackground(ctx context.Context) {
	go d.evictionLoop(ctx)
	go d.cleanupLoop(ctx)
}

func (d *Dependencies) evictionLoop(ctx context.Context) {
	ticker := time.NewTicker(d.Config.EvictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.Clock.EvictExpired(ctx, d.Store); err != nil {
				log.Printf("eviction sweep failed: %v", err)
			}
		}
	}
}

func (d *Dependencies) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(d.Config.CleanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, report, err := d.Scanner.ScanReport(ctx)
			if err != nil {
				log.Printf("cleanup scan failed: %v", err)
				continue
			}
			if removed > 0 {
				log.Printf("cleanup removed %d/%d expired sessions in %d batches", removed, report.Expired, report.Batches)
			}
		}
	}
}
