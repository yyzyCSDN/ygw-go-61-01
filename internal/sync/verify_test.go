package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
)

type failingMirror struct{}

func (failingMirror) Read(ctx context.Context, id string) (*model.Session, error) {
	return nil, model.ErrNotFound
}

func (failingMirror) Write(ctx context.Context, sess *model.Session) error {
	return errors.New("mirror disk is full")
}

func TestMirrorWriteErrorNotSwallowed(t *testing.T) {
	shards := shard.NewManager([]string{"node-a"})
	manager := NewManager(failingMirror{}, route.NewRoute(shards), shards)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession("mirror-fail-1", "node-a", time.Hour, now)
	if err := manager.Mirror(context.Background(), sess); err == nil {
		t.Fatal("mirror write failure must be reported instead of swallowed")
	}
}
