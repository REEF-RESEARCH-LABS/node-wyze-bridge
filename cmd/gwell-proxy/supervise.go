package main

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// Supervisor errors, classified by runCamera for status and retry.
var (
	errDeadman        = errors.New("deadman timeout")
	errPublisherStart = errors.New("publisher would not restart")
)

// sessionHandle is the part of a camera session the supervisor needs.
type sessionHandle interface {
	Close() error
}

type superviseConfig struct {
	camera  string
	tick    time.Duration
	deadman time.Duration // max interval without camera video (first video included)
	// stuckAfter: a publisher write running this long is treated as a dead
	// publisher even if closing it has not taken effect.
	stuckAfter   time.Duration
	newPublisher func() (publisher, error)
	onTick       func() // status refresh; must not block
	onRestart    func() // a publisher was replaced
}

// supervise is the per-session monitor: stop handling, publisher health and
// the deadman switch. It reads only atomics and the publisher pointer, never
// waits on a publisher write, and so keeps running when downstream output
// is blocked (REE-1054).
func supervise(sess sessionHandle, errCh <-chan error, stopCh <-chan struct{}, tracker *writeTracker, cfg superviseConfig) error {
	ticker := time.NewTicker(cfg.tick)
	defer ticker.Stop()
	for {
		select {
		case err := <-errCh:
			return fmt.Errorf("session ended: %w", err)

		case <-stopCh:
			sess.Close()
			return nil

		case <-ticker.C:
			now := time.Now()
			if cfg.onTick != nil {
				cfg.onTick()
			}
			// Publisher health. When ffmpeg dies (go2rtc restarted or refused
			// it) or a write overran its deadline, a new publisher is started
			// and the camera session kept: a full reconnect costs a new P2P
			// handshake, a minute or more on a Window Cam.
			old := tracker.current()
			stuck := tracker.stuckOnCurrent(now, cfg.stuckAfter)
			if old == nil || !old.Alive() || stuck {
				fresh, err := cfg.newPublisher()
				if err != nil {
					sess.Close()
					return fmt.Errorf("%w: %v", errPublisherStart, err)
				}
				if prev := tracker.swap(fresh); prev != nil {
					go prev.Close() // never wait on a publisher here
				}
				if cfg.onRestart != nil {
					cfg.onRestart()
				}
				log.Printf("[%s] publisher replaced (dead=%v stuck=%v); camera session kept", cfg.camera, old == nil || !old.Alive(), stuck)
			}

			// Deadman: no camera video for too long. Measured on what the
			// camera delivers, not on what the publisher accepted, so a
			// downstream stall never tears down a healthy camera session.
			if since := now.Sub(tracker.deadmanBaseline()); since > cfg.deadman {
				sess.Close()
				return fmt.Errorf("%w: no stream data for %s", errDeadman, cfg.deadman)
			}
		}
	}
}
