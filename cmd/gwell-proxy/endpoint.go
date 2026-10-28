package main

// endpoint.go — P2P server endpoint cache (REE-1054, 4.6.1-node.17).
//
// The pinned proxy re-stamped the cache time and seven-day TTL every time
// it reused a cached endpoint, so the hourly token refresh and every
// post-error pass kept a stale address alive forever. Now:
//
//   - The endpoint ages from real discovery only (discoveredAt). Saving a
//     refreshed token updates cachedAt (the token time) and nothing else.
//   - Each failed certification/health attempt (a session that ended
//     without delivering video) counts against the endpoint; after
//     endpointFailLimit the next attempt runs a fresh cloud discovery.
//   - A session that delivers video clears the count.
//   - Discovery is the cloud routing exchange only (unicast to the P2P
//     list/server addresses). No LAN broadcast, multicast or mDNS (REE-959).

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// endpointTTL: how long a discovered P2P server address is used before
	// it is discovered again, counted from the discovery itself.
	endpointTTL = 24 * time.Hour
	// endpointFailLimit: failed attempts on a cached endpoint before it is
	// invalidated and cloud routing refreshed.
	endpointFailLimit = 3
)

// tokenCache persists credentials and the discovered endpoint across
// restarts. cachedAt/ttlSeconds keep their names for old files; a file
// without discoveredAt has an endpoint of unknown age and is rediscovered.
type tokenCache struct {
	AccessID       string `json:"accessId"`
	AccessToken    string `json:"accessToken"`
	ServerAddr     string `json:"serverAddr"`
	CachedAt       int64  `json:"cachedAt"`               // unix s: token last saved
	TTLSeconds     int64  `json:"ttlSeconds"`             // endpoint lifetime from discoveredAt
	DiscoveredAt   int64  `json:"discoveredAt,omitempty"` // unix s: last real discovery
	FailedAttempts int    `json:"failedAttempts,omitempty"`
}

// endpointUsable reports whether the cached server address may be used
// without a fresh discovery.
func (tc *tokenCache) endpointUsable(now time.Time) bool {
	if tc == nil || tc.ServerAddr == "" || tc.DiscoveredAt == 0 {
		return false
	}
	if tc.FailedAttempts >= endpointFailLimit {
		return false
	}
	ttl := time.Duration(tc.TTLSeconds) * time.Second
	if ttl <= 0 || ttl > endpointTTL {
		ttl = endpointTTL
	}
	age := now.Sub(time.Unix(tc.DiscoveredAt, 0))
	return age >= 0 && age < ttl
}

// cacheMu serializes read-modify-write of the cache file across camera
// goroutines and the token refresh loop.
var cacheMu sync.Mutex

// cacheFilePath resolves to $STATE_DIR/gwell/token_cache.json when
// STATE_DIR is set (our bridge's convention), otherwise ./data/.
func cacheFilePath() string {
	if d := os.Getenv("STATE_DIR"); d != "" {
		return filepath.Join(d, "gwell", "token_cache.json")
	}
	return "data/token_cache.json"
}

func loadCache() *tokenCache {
	data, err := os.ReadFile(cacheFilePath())
	if err != nil {
		return nil
	}
	var tc tokenCache
	if err := json.Unmarshal(data, &tc); err != nil {
		return nil
	}
	return &tc
}

func saveCache(tc *tokenCache) {
	path := cacheFilePath()
	dir := filepath.Dir(path)
	os.MkdirAll(dir, 0755)
	data, err := json.MarshalIndent(tc, "", "  ")
	if err != nil {
		log.Printf("[cache] Failed to marshal: %v", err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("[cache] Failed to write %s: %v", path, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("[cache] Failed to write %s: %v", path, err)
		return
	}
}

// updateCache applies fn to the cache file under cacheMu. fn gets a
// non-nil cache (empty when there was none) and returns false to skip
// saving.
func updateCache(fn func(tc *tokenCache) bool) *tokenCache {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	tc := loadCache()
	if tc == nil {
		tc = &tokenCache{}
	}
	if fn(tc) {
		saveCache(tc)
	}
	return tc
}

// noteEndpointFailure counts a failed attempt against the cached endpoint.
// Returns the count and whether the endpoint is now invalidated.
func noteEndpointFailure(reason string) (int, bool) {
	var n int
	updateCache(func(tc *tokenCache) bool {
		if tc.ServerAddr == "" {
			return false
		}
		tc.FailedAttempts++
		n = tc.FailedAttempts
		return true
	})
	invalid := n >= endpointFailLimit
	if invalid {
		log.Printf("[cache] P2P endpoint failed %d attempts (%s): next attempt refreshes cloud routing", n, reason)
	}
	return n, invalid
}

// noteEndpointHealthy clears the failure count after video arrived.
func noteEndpointHealthy() {
	updateCache(func(tc *tokenCache) bool {
		if tc.FailedAttempts == 0 {
			return false
		}
		tc.FailedAttempts = 0
		return true
	})
}
