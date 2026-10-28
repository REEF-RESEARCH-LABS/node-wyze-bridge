package webui

import (
	"encoding/json"
	"os"
	"strings"
	"time"
)

// gwellStatusStaleAfter: gwell-proxy rewrites its status file at least
// every 5 s; older than this means the proxy is not running.
const gwellStatusStaleAfter = 30 * time.Second

// gwellStatuses reads gwell-proxy's status file (REE-1054) and returns
// each camera's entry keyed by upper-case MAC, each with "proxy_updated_at"
// (the file's own time) and "stale" added. Missing or unreadable file:
// nil, and cameras simply carry no "gwell" key.
func (s *Server) gwellStatuses() map[string]map[string]interface{} {
	if s.cfg == nil {
		return nil
	}
	raw, err := os.ReadFile(s.cfg.GwellStatusFile())
	if err != nil {
		return nil
	}
	var file struct {
		UpdatedAt time.Time                         `json:"updated_at"`
		Cameras   map[string]map[string]interface{} `json:"cameras"`
	}
	if json.Unmarshal(raw, &file) != nil {
		return nil
	}
	stale := file.UpdatedAt.IsZero() || time.Since(file.UpdatedAt) > gwellStatusStaleAfter
	out := make(map[string]map[string]interface{}, len(file.Cameras))
	for id, st := range file.Cameras {
		if st == nil {
			continue
		}
		st["proxy_updated_at"] = file.UpdatedAt
		st["stale"] = stale
		out[strings.ToUpper(id)] = st
	}
	return out
}

// withGwellStatus adds gwell-proxy's lifecycle status as "gwell" to a
// camera's JSON when the proxy reports that camera (matched by MAC).
// Existing keys are untouched.
func withGwellStatus(cam map[string]interface{}, statuses map[string]map[string]interface{}) map[string]interface{} {
	if len(statuses) == 0 {
		return cam
	}
	if mac, ok := cam["mac"].(string); ok && mac != "" {
		if st, ok := statuses[strings.ToUpper(mac)]; ok {
			cam["gwell"] = st
		}
	}
	return cam
}
