package sync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sessionstore/internal/model"
	"sessionstore/internal/route"
	"sessionstore/internal/shard"
)

// faultMirror is a controllable MirrorBackend used to exercise mirror write
// failures and retries.  It records every Write attempt so tests can assert on
// retry behaviour, and can be programmed to fail the first N writes before
// succeeding, or to fail permanently.
type faultMirror struct {
	mu        sync.Mutex
	items     map[string]*model.Session
	failures  int // remaining forced failures
	attemptNo int // total Write attempts observed
	failErr   error
}

func newFaultMirror(failErr error, failures int) *faultMirror {
	return &faultMirror{
		items:    make(map[string]*model.Session),
		failures: failures,
		failErr:  failErr,
	}
}

func (f *faultMirror) Read(ctx context.Context, id string) (*model.Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sess, ok := f.items[id]
	if !ok {
		return nil, model.ErrNotFound
	}
	return sess.Clone(), nil
}

func (f *faultMirror) Write(ctx context.Context, sess *model.Session) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attemptNo++
	if f.failures > 0 {
		f.failures--
		return f.failErr
	}
	f.items[sess.ID] = sess.Clone()
	return nil
}

// attempts returns the number of Write calls observed so far.
func (f *faultMirror) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attemptNo
}

// resetAttempts zeros the attempt counter so a test can count only the writes
// it cares about.
func (f *faultMirror) resetAttempts() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attemptNo = 0
}

// has reports whether a snapshot for id was persisted.
func (f *faultMirror) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.items[id]
	return ok
}

func newTestManager(t *testing.T, backend MirrorBackend) (*Manager, *shard.Manager, *route.Route) {
	t.Helper()
	shards := shard.NewManager([]string{"node-a", "node-b"})
	router := route.NewRoute(shards)
	return NewManager(backend, router, shards), shards, router
}

func newMirrorSession(id string, version uint64) *model.Session {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sess := model.NewSession(id, "node-a", time.Hour, now)
	sess.State = model.StateActive
	sess.Version = version
	return sess
}

// TestMirrorSurfacesWriteError proves the original bug is fixed: a failed
// mirror write used to be discarded ("_ = m.backend.Write(...)") and the state
// machine advanced to consistent anyway.  Now the error is reported and the
// state rolls back to local so a failover is not lied to.
func TestMirrorSurfacesWriteError(t *testing.T) {
	boom := errors.New("disk full")
	mirror := newFaultMirror(boom, 100) // always fails
	manager, _, _ := newTestManager(t, mirror)

	err := manager.Mirror(context.Background(), newMirrorSession("fail-1", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("mirror error must wrap the backend cause, got %v", err)
	}
	if !IsMirrorError(err) {
		t.Fatalf("mirror error must be detectable via IsMirrorError, got %v", err)
	}
	if got := manager.StateOf("fail-1"); got != SyncLocal {
		t.Fatalf("failed mirror must roll back to local, got %s", got)
	}
	if mirror.has("fail-1") {
		t.Fatal("no snapshot should have been persisted on permanent failure")
	}
	if attempts := mirror.attempts(); attempts != retryAttempts {
		t.Fatalf("expected %d retry attempts, got %d", retryAttempts, attempts)
	}
}

// TestMirrorRetriesTransientFailure confirms a transient failure is retried and
// ultimately succeeds, leaving the session consistent.
func TestMirrorRetriesTransientFailure(t *testing.T) {
	boom := errors.New("transient hiccup")
	mirror := newFaultMirror(boom, 2) // fail twice, then succeed
	manager, _, _ := newTestManager(t, mirror)

	if err := manager.Mirror(context.Background(), newMirrorSession("retry-1", 1)); err != nil {
		t.Fatalf("mirror should recover after transient failures, got %v", err)
	}
	if got := manager.StateOf("retry-1"); got != SyncConsistent {
		t.Fatalf("recovered mirror must be consistent, got %s", got)
	}
	if !mirror.has("retry-1") {
		t.Fatal("snapshot must be persisted once writes succeed")
	}
	if attempts := mirror.attempts(); attempts != 3 {
		t.Fatalf("expected 3 attempts (2 fail + 1 ok), got %d", attempts)
	}
}

// TestMigrateSessionSurfacesMirrorError mirrors the failover path: a migration
// that the mirror never acknowledges must be reported and must not be marked
// consistent, otherwise a subsequent failover reads a stale owner from the
// mirror and loses the session.
func TestMigrateSessionSurfacesMirrorError(t *testing.T) {
	boom := errors.New("mirror unreachable")
	mirror := newFaultMirror(boom, 100)
	manager, shards, _ := newTestManager(t, mirror)

	// Seed the mirror with the original owner so migration can read it back.
	// The mirror is programmed to fail every write, so seed it via the
	// underlying store directly rather than through Write.
	orig := newMirrorSession("mig-1", 1)
	shards.Put(orig)
	mirror.mu.Lock()
	mirror.items["mig-1"] = orig.Clone()
	mirror.mu.Unlock()

	// Reset the attempt counter so only the migration write is counted.
	mirror.resetAttempts()

	err := manager.MigrateSession(context.Background(), "mig-1", "node-b")
	if !IsMirrorError(err) {
		t.Fatalf("migration mirror failure must be reported, got %v", err)
	}
	if got := manager.StateOf("mig-1"); got == SyncConsistent {
		t.Fatalf("migration with failed mirror must not be marked consistent, got %s", got)
	}
}

// TestReconnectNilRemote guards against a panic that would lose the recovered
// session during failover when a peer offers no snapshot.
func TestReconnectNilRemote(t *testing.T) {
	mirror := NewMemoryMirror()
	manager, _, _ := newTestManager(t, mirror)

	err := manager.Reconnect(context.Background(), "nil-1", nil)
	if err == nil {
		t.Fatal("reconnect with nil remote must error instead of panicking")
	}
}

// TestReconnectAdoptsNewerVersion simulates a failover: the local copy is
// behind, a remote peer offers a newer snapshot, and reconnect must adopt it
// so the session survives with the latest state instead of being rolled back.
func TestReconnectAdoptsNewerVersion(t *testing.T) {
	mirror := NewMemoryMirror()
	manager, _, _ := newTestManager(t, mirror)

	stale := newMirrorSession("rec-1", 3)
	if err := mirror.Write(context.Background(), stale); err != nil {
		t.Fatalf("seed stale mirror: %v", err)
	}

	fresh := newMirrorSession("rec-1", 9)
	fresh.Data["recovered"] = "yes"
	if err := manager.Reconnect(context.Background(), "rec-1", fresh); err != nil {
		t.Fatalf("reconnect should adopt newer remote, got %v", err)
	}
	got, err := mirror.Read(context.Background(), "rec-1")
	if err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}
	if got.Version != 9 {
		t.Fatalf("reconnect must adopt newer version 9, got %d", got.Version)
	}
	if got.Data["recovered"] != "yes" {
		t.Fatal("reconnect must adopt the recovered payload")
	}
	if got := manager.StateOf("rec-1"); got != SyncConsistent {
		t.Fatalf("reconnect must leave session consistent, got %s", got)
	}
}

// TestReconnectKeepsNewerLocal confirms the version guard is not inverted:
// a stale remote must never overwrite a newer local copy, which would lose a
// renewal that happened after the remote snapshot was taken.
func TestReconnectKeepsNewerLocal(t *testing.T) {
	mirror := NewMemoryMirror()
	manager, _, _ := newTestManager(t, mirror)

	local := newMirrorSession("keep-1", 7)
	local.Data["local"] = "winner"
	if err := mirror.Write(context.Background(), local); err != nil {
		t.Fatalf("seed local mirror: %v", err)
	}

	stale := newMirrorSession("keep-1", 2)
	if err := manager.Reconnect(context.Background(), "keep-1", stale); err != nil {
		t.Fatalf("reconnect should be a no-op for stale remote, got %v", err)
	}
	got, err := mirror.Read(context.Background(), "keep-1")
	if err != nil {
		t.Fatalf("read after stale reconnect: %v", err)
	}
	if got.Version != 7 {
		t.Fatalf("newer local must be preserved, got version %d", got.Version)
	}
	if got.Data["local"] != "winner" {
		t.Fatal("local payload must survive a stale remote reconnect")
	}
}
