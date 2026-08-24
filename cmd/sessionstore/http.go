package main

import (
	"net/http"
	"os"
)

// server binds the HTTP routes to the dependency graph.
type server struct {
	deps *Dependencies
	cfg  Config
}

// NewServer builds the HTTP mux for the session store.
func NewServer(deps *Dependencies) http.Handler {
	srv := &server{deps: deps, cfg: deps.Config}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", srv.handleIndex)
	mux.HandleFunc("GET /api/healthz", srv.handleHealth)
	mux.HandleFunc("GET /api/monitor", srv.handleMonitor)
	mux.HandleFunc("POST /api/sessions", srv.handleCreate)
	mux.HandleFunc("POST /api/sessions/{id}/renew", srv.handleRenew)
	mux.HandleFunc("GET /api/sessions/{id}", srv.handleGet)
	mux.HandleFunc("POST /api/sessions/{id}/evict", srv.handleEvict)
	mux.HandleFunc("POST /api/sessions/{id}/migrate", srv.handleMigrate)
	mux.HandleFunc("GET /api/sessions/{id}/saved", srv.handleSaved)
	mux.HandleFunc("POST /api/maintenance/rebalance", srv.handleRebalance)
	mux.HandleFunc("GET /api/sessions", srv.handleList)
	return mux
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	report := ProbeHealth(s.deps)
	writeJSON(w, http.StatusOK, report)
}

// readFile is a tiny indirection so the handler can be tested with an
// injected file system in unit tests.
var readFile = func(path string) ([]byte, error) {
	return os.ReadFile(path)
}
