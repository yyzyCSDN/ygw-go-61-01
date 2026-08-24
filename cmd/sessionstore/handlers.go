package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"sessionstore/internal/model"
)

// sessionRequest is the JSON body of a create request.
type sessionRequest struct {
	ID        string `json:"id"`
	TTLSecond int    `json:"ttl_seconds"`
}

// migrateRequest is the JSON body of a migrate request.
type migrateRequest struct {
	Node string `json:"node"`
}

// sessionView is the JSON representation returned to clients.
type sessionView struct {
	ID                string            `json:"id"`
	State             string            `json:"state"`
	OwnerNode         string            `json:"owner_node"`
	Version           uint64            `json:"version"`
	Data              map[string]string `json:"data"`
	CreatedAt         string            `json:"created_at"`
	LastActiveAt      string            `json:"last_active_at"`
	ExpireAt          string            `json:"expire_at"`
	TTLRemainingHours string            `json:"ttl_remaining"`
}

func (s *server) view(sess *model.Session) sessionView {
	remaining, ok := s.deps.Clock.Remaining(sess.ID)
	label := "unknown"
	if ok {
		label = remaining.Round(time.Second).String()
	}
	return sessionView{
		ID:                sess.ID,
		State:             sess.State.String(),
		OwnerNode:         sess.OwnerNode,
		Version:           sess.Version,
		Data:              sess.Data,
		CreatedAt:         sess.CreatedAt.Format(time.RFC3339),
		LastActiveAt:      sess.LastActiveAt.Format(time.RFC3339),
		ExpireAt:          sess.ExpireAt.Format(time.RFC3339),
		TTLRemainingHours: label,
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var request sessionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if strings.TrimSpace(request.ID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}
	policy := model.DefaultTTLPolicy()
	ttl := policy.Clamp(model.TTLFromSeconds(request.TTLSecond, s.cfg.DefaultTTL))
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	owner, err := s.deps.Router.Route(request.ID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	sess, err := s.deps.Store.Create(ctx, request.ID, ttl)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, model.ErrExists) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	s.deps.Router.BindSticky(request.ID, owner)
	writeJSON(w, http.StatusCreated, s.view(sess))
}

func (s *server) handleRenew(w http.ResponseWriter, r *http.Request) {
	id := pathSegment(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	if err := s.deps.Store.Renew(ctx, id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "renewed"})
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := pathSegment(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	owner, err := s.deps.Router.Route(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	sess, err := s.deps.Store.Get(ctx, id)
	if err != nil {
		status := http.StatusNotFound
		if errors.Is(err, model.ErrExpired) {
			status = http.StatusGone
		}
		writeJSON(w, status, map[string]string{"error": err.Error(), "owner": owner})
		return
	}
	writeJSON(w, http.StatusOK, s.view(sess))
}

func (s *server) handleEvict(w http.ResponseWriter, r *http.Request) {
	id := pathSegment(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	if err := s.deps.Store.Evict(ctx, id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "evicted"})
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	ids := s.deps.Store.SessionIDs()
	views := make([]sessionView, 0, len(ids))
	for _, id := range ids {
		sess, err := s.deps.Store.Get(ctx, id)
		if err == nil {
			views = append(views, s.view(sess))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": views, "count": len(views)})
}

func (s *server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	id := pathSegment(r, "id")
	var request migrateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadTimeout)
	defer cancel()
	if err := s.deps.Sync.MigrateSession(ctx, id, request.Node); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "migrated", "node": request.Node})
}

func (s *server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	moved := s.deps.Shards.Rebalance()
	rebound := s.deps.Router.Migrate(s.deps.Router.Snapshot())
	writeJSON(w, http.StatusOK, map[string]any{"moved": moved, "rebound": rebound})
}

func (s *server) handleSaved(w http.ResponseWriter, r *http.Request) {
	id := pathSegment(r, "id")
	sess, ok := s.deps.Saver.Snapshot(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no saved snapshot"})
		return
	}
	writeJSON(w, http.StatusOK, s.view(sess))
}

func (s *server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	stats := s.deps.Store.Stats()
	snapshot := model.Snapshot{
		TotalSessions: stats["created"] + stats["active"] + stats["stale"] + stats["expired"],
		Active:        stats["active"],
		Stale:         stats["stale"],
		Expired:       stats["expired"],
		SyncStates:    s.deps.Sync.Snapshot(),
	}
	for _, entry := range s.deps.Shards.Status() {
		node, found := nodeByID(s.deps.Nodes, entry.NodeID)
		state := "up"
		if found {
			state = node.State.String()
		}
		snapshot.Nodes = append(snapshot.Nodes, model.NodeInfo{
			ID:       entry.NodeID,
			Addr:     nodeAddr(s.deps.Nodes, entry.NodeID),
			State:    state,
			Sessions: entry.SessionCount,
		})
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func nodeByID(nodes []*model.Node, id string) (*model.Node, bool) {
	for _, node := range nodes {
		if node.ID == id {
			return node, true
		}
	}
	return nil, false
}

func nodeAddr(nodes []*model.Node, id string) string {
	if node, ok := nodeByID(nodes, id); ok {
		return node.Addr
	}
	return ""
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	path := s.cfg.WebRoot + "/monitor.html"
	content, err := readFile(path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "monitor page unavailable"})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func pathSegment(r *http.Request, name string) string {
	return r.PathValue(name)
}
