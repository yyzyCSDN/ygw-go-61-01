package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Addr != ":8080" {
		t.Fatalf("unexpected default addr %q", cfg.Addr)
	}
	if len(cfg.NodeIDs) != 3 {
		t.Fatalf("expected 3 default nodes, got %d", len(cfg.NodeIDs))
	}
	if cfg.CleanBatch <= 0 {
		t.Fatalf("clean batch must be positive, got %d", cfg.CleanBatch)
	}
}

func TestHTTPServerHealthz(t *testing.T) {
	deps, err := BuildDependencies(DefaultConfig())
	if err != nil {
		t.Fatalf("build dependencies: %v", err)
	}
	handler := NewServer(deps)
	request := httptest.NewRequest(http.MethodGet, "/api/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
}

func TestHTTPCreateAndGet(t *testing.T) {
	deps, err := BuildDependencies(DefaultConfig())
	if err != nil {
		t.Fatalf("build dependencies: %v", err)
	}
	handler := NewServer(deps)
	create := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"id":"http-1","ttl_seconds":3600}`))
	createRecorder := httptest.NewRecorder()
	handler.ServeHTTP(createRecorder, create)
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRecorder.Code, createRecorder.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/api/sessions/http-1", nil)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, get)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRecorder.Code, getRecorder.Body.String())
	}
}
