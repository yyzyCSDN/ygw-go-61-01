package clean

import (
	"context"
	"sync"
	"testing"
)

type fakeStore struct {
	mu       sync.Mutex
	sessions map[string]bool
}

func (f *fakeStore) SessionIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.sessions))
	for id := range f.sessions {
		ids = append(ids, id)
	}
	return ids
}

func (f *fakeStore) Remove(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sessions, id)
	return nil
}

func TestScanSingleBatch(t *testing.T) {
	store := &fakeStore{sessions: map[string]bool{"e1": true, "e2": true, "e3": true}}
	scanner := NewScanner(store, func(id string) bool { return true }, 3)
	removed, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if removed != 3 {
		t.Fatalf("expected 3 removals, got %d", removed)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("all sessions must be removed, %d remain", len(store.sessions))
	}
}

func TestScanKeepsLiveSessions(t *testing.T) {
	store := &fakeStore{sessions: map[string]bool{"live": true, "dead": true}}
	scanner := NewScanner(store, func(id string) bool { return id == "dead" }, 2)
	removed, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removal, got %d", removed)
	}
	if !store.sessions["live"] {
		t.Fatal("live session must survive the scan")
	}
}
