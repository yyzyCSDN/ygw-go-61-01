package clean

import (
	"context"
	"sync"
	"testing"
)

type oracleStore struct {
	mu       sync.Mutex
	sessions map[string]bool
}

func (o *oracleStore) SessionIDs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	ids := make([]string, 0, len(o.sessions))
	for id := range o.sessions {
		ids = append(ids, id)
	}
	return ids
}

func (o *oracleStore) Remove(ctx context.Context, id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.sessions, id)
	return nil
}

func TestCleanupScanNoGap(t *testing.T) {
	store := &oracleStore{sessions: make(map[string]bool)}
	for index := 0; index < 25; index++ {
		store.sessions["expired-"+string(rune('a'+index/26))+string(rune('a'+index%26))] = true
	}
	scanner := NewScanner(store, func(id string) bool { return true }, 10)
	removed, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if removed != 25 {
		t.Fatalf("expected all 25 expired sessions to be removed, got %d", removed)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("%d expired sessions were skipped by the scan boundary", len(store.sessions))
	}
}
