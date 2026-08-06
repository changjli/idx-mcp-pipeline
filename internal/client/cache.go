package client

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"
)

// cacheEntry holds a cached response with its creation time.
type cacheEntry struct {
	body    []byte
	status  int
	headers http.Header
	created time.Time
}

// StaleCache is a thread-safe in-memory stale-while-error cache.
type StaleCache struct {
	mu    sync.RWMutex
	items map[string]*cacheEntry
	ttl   time.Duration
}

// NewStaleCache creates a cache with the given TTL.
func NewStaleCache(ttl time.Duration) *StaleCache {
	return &StaleCache{
		items: make(map[string]*cacheEntry),
		ttl:   ttl,
	}
}

// Get returns a cached response and whether it is still fresh.
// If the entry is past TTL, it is still returned as stale (for error fallback).
func (c *StaleCache) Get(key string) (body []byte, status int, headers http.Header, fresh bool) {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		return nil, 0, nil, false
	}

	fresh = time.Since(entry.created) < c.ttl
	return entry.body, entry.status, entry.headers.Clone(), fresh
}

// Set stores a response body + metadata in the cache.
func (c *StaleCache) Set(key string, status int, headers http.Header, body []byte) {
	entry := &cacheEntry{
		body:    body,
		status:  status,
		headers: headers.Clone(),
		created: time.Now(),
	}

	c.mu.Lock()
	c.items[key] = entry
	c.mu.Unlock()
}

// ReplayBody returns a reader for the cached body (for serving stale responses).
func ReplayBody(body []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(body))
}
