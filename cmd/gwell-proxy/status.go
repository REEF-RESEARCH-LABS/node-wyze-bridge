package main

// status.go — per-camera lifecycle status (REE-1054, 4.6.1-node.17).
//
// Written as JSON to --status-file (the bridge passes
// $STATE_DIR/gwell/status.json and serves it per camera on /api/cameras as
// "gwell"). Times are RFC 3339 UTC strings; a measurement that has not
// happened is null, never zero. Durations carry their unit in the name.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Proxy-level stages around the session's own (gwell.Stage*).
const (
	stageStarting          = "starting"
	stageAuthorizing       = "authorizing"        // fetching the camera's Mars credential from the bridge
	stageWaitingTurn       = "waiting_turn"       // waiting for the one-at-a-time P2P setup slot
	stageWaitingRetry      = "waiting_retry"      // quick retry or cooldown before the next attempt
	stageCredentialRefused = "credential_refused" // bridge says the credential was refused; held
	stageStopped           = "stopped"
)

type endpointStatus struct {
	DiscoveredAt   *time.Time `json:"discovered_at"`
	FailedAttempts int        `json:"failed_attempts"`
}

type cameraStatus struct {
	CameraID            string         `json:"camera_id"`
	Stream              string         `json:"stream,omitempty"`
	Stage               string         `json:"stage"`
	StageSince          time.Time      `json:"stage_since"`
	Generation          uint64         `json:"generation"`
	Route               string         `json:"route,omitempty"` // lan | relay | trial
	LastError           string         `json:"last_error,omitempty"`
	LastErrorKind       string         `json:"last_error_kind,omitempty"`
	LastErrorAt         *time.Time     `json:"last_error_at"`
	LastVideoAt         *time.Time     `json:"last_video_at"`
	NextRetryAt         *time.Time     `json:"next_retry_at"`
	RetryMode           string         `json:"retry_mode,omitempty"` // retry | cooldown | credential_hold
	ConsecutiveFailures int            `json:"consecutive_failures"`
	LastControl         string         `json:"last_control,omitempty"`
	LateAccepts         int            `json:"late_accepts"`
	BytesIn             uint64         `json:"bytes_in"`
	BytesOut            uint64         `json:"bytes_out"`
	DroppedChunks       uint64         `json:"dropped_chunks"`
	PublisherRestarts   uint64         `json:"publisher_restarts"`
	StuckWrites         uint64         `json:"stuck_writes"`
	Endpoint            endpointStatus `json:"endpoint"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

type statusFile struct {
	Version   int                      `json:"version"`
	PID       int                      `json:"pid"`
	UpdatedAt time.Time                `json:"updated_at"`
	Cameras   map[string]*cameraStatus `json:"cameras"`
}

// statusBoard holds every camera's status and writes the file. Updates
// only take a short mutex; the file is written by its own goroutine.
type statusBoard struct {
	path    string
	mu      sync.Mutex
	cameras map[string]*cameraStatus
	dirty   chan struct{}
}

func newStatusBoard(path string) *statusBoard {
	b := &statusBoard{path: path, cameras: map[string]*cameraStatus{}, dirty: make(chan struct{}, 1)}
	if path != "" {
		go b.writer()
	}
	return b
}

func ptrTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

// update applies fn to a camera's status.
func (b *statusBoard) update(id string, fn func(st *cameraStatus)) {
	if b == nil {
		return
	}
	b.mu.Lock()
	st := b.cameras[id]
	if st == nil {
		st = &cameraStatus{CameraID: id, Stage: stageStarting, StageSince: time.Now().UTC()}
		b.cameras[id] = st
	}
	prev := st.Stage
	fn(st)
	now := time.Now().UTC()
	if st.Stage != prev {
		st.StageSince = now
	}
	st.UpdatedAt = now
	b.mu.Unlock()
	select {
	case b.dirty <- struct{}{}:
	default:
	}
}

func (b *statusBoard) setStage(id, stage string) {
	b.update(id, func(st *cameraStatus) { st.Stage = stage })
}

// snapshot returns a deep copy (for tests and the writer).
func (b *statusBoard) snapshot() statusFile {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := statusFile{Version: 1, PID: os.Getpid(), UpdatedAt: time.Now().UTC(), Cameras: map[string]*cameraStatus{}}
	for id, st := range b.cameras {
		c := *st
		out.Cameras[id] = &c
	}
	return out
}

// writer writes the file at most every 500 ms when something changed, and
// every 5 s regardless so updated_at shows the proxy is alive.
func (b *statusBoard) writer() {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-b.dirty:
		case <-tick.C:
		}
		b.writeOnce()
		time.Sleep(500 * time.Millisecond)
	}
}

func (b *statusBoard) writeOnce() {
	data, err := json.MarshalIndent(b.snapshot(), "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(b.path), 0755); err != nil {
		log.Printf("[status] %v", err)
		return
	}
	tmp := b.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[status] %v", err)
		return
	}
	if err := os.Rename(tmp, b.path); err != nil {
		log.Printf("[status] %v", err)
	}
}
