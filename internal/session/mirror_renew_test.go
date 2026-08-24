package session

import (
	"context"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/shard"
	"sessionstore/internal/ttl"
	"sessionstore/internal/version"
)

// recordingMirror records the sessions handed to Mirror so a test can assert
// that a renewal actually reached the mirror.
type recordingMirror struct {
	sessions map[string]*model.Session
}

func newRecordingMirror() *recordingMirror {
	return &recordingMirror{sessions: make(map[string]*model.Session)}
}

func (r *recordingMirror) Mirror(ctx context.Context, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	r.sessions[sess.ID] = sess.Clone()
	return nil
}

func (r *recordingMirror) latest(id string) (*model.Session, bool) {
	sess, ok := r.sessions[id]
	return sess, ok
}

// TestRenewMirrorsLatestVersion confirms that a successful renewal now reaches
// the mirror with the bumped version.  This is the positive counterpart to the
// swallowed-error regression: once the mirror error is propagated, the renewal
// flows to the mirror so a failover can recover the up-to-date state instead of
// an older snapshot.
func TestRenewMirrorsLatestVersion(t *testing.T) {
	mirror := newRecordingMirror()
	shards := shard.NewManager([]string{"node-a", "node-b"})
	store, err := NewStore(StoreConfig{
		Shards:   shards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Mirror:   mirror,
		Saver:    NewMemorySaver(),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()
	sess, err := store.Create(ctx, "renew-ok", time.Hour)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	createdVersion := sess.Version

	if err := store.Renew(ctx, "renew-ok"); err != nil {
		t.Fatalf("renew: %v", err)
	}

	mirrored, ok := mirror.latest("renew-ok")
	if !ok {
		t.Fatal("renewed session must be mirrored")
	}
	if mirrored.Version <= createdVersion {
		t.Fatalf("mirror must hold the bumped version, got %d (created %d)", mirrored.Version, createdVersion)
	}
	if mirrored.State != model.StateActive {
		t.Fatalf("mirrored session must be active, got %s", mirrored.State)
	}
}

// TestBatchRenewPropagatesMirrorError anchors the fact that BatchRenew already
// reports mirror failures (it was not part of the swallowed-error regression)
// so that future changes keep that contract intact.
func TestBatchRenewPropagatesMirrorError(t *testing.T) {
	boom := newFailingMirror(errBatchMirrorDown)
	seedShards := shard.NewManager([]string{"node-a", "node-b"})
	seedStore, err := NewStore(StoreConfig{
		Shards:   seedShards,
		Clock:    ttl.NewClock(time.Hour),
		Versions: version.NewAllocator(1),
		Saver:    NewMemorySaver(),
		Backend:  NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if _, err := seedStore.Create(context.Background(), "batch-1", time.Hour); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	store, err := NewStore(StoreConfig{
		Shards:   shard.NewManager([]string{"node-a", "node-b"}),
		Clock:    seedStore.clock,
		Versions: version.NewAllocator(100),
		Saver:    NewMemorySaver(),
		Backend:  seedStore.backend,
		Mirror:   boom,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if sess, err := store.backend.Read(context.Background(), "batch-1"); err == nil {
		store.shards.Put(sess)
	}

	if _, err := store.BatchRenew(context.Background(), []string{"batch-1"}, time.Hour); err == nil {
		t.Fatal("BatchRenew must surface the mirror error, not swallow it")
	}
}
