package main

// retry.go — per-camera retry and route trial (REE-1054, 4.6.1-node.17).
//
// Transient failures get a few quick retries, then an expiring cooldown
// followed by one half-open attempt; a failed half-open attempt earns a
// longer cooldown up to a ceiling. Nothing latches: the ceiling still
// retries. A genuine credential refusal from the bridge (HTTP 401/403 from
// the shim's token endpoint) is honoured with a long hold instead of a
// session attempt, and is checked again when the hold expires.

import (
	"errors"
	"time"

	"github.com/IDisposable/docker-wyze-bridge/internal/gwell/upstream/gwell"
)

var (
	retryQuickDelays   = []time.Duration{3 * time.Second, 10 * time.Second, 30 * time.Second}
	retryCooldownFirst = 2 * time.Minute
	retryCooldownMax   = 15 * time.Minute
	credentialHold     = 15 * time.Minute
)

// Error kinds, as reported in status last_error_kind.
const (
	kindTransient         = "transient"
	kindCredentialRefused = "credential_refused"
	kindTerminalControl   = "terminal_control"
	kindDeadman           = "deadman"
	kindPublisher         = "publisher"
)

func classifyError(err error) string {
	var tc *gwell.TerminalControlError
	switch {
	case err == nil:
		return ""
	case isCredentialRefused(err):
		return kindCredentialRefused
	case errors.As(err, &tc):
		return kindTerminalControl
	case errors.Is(err, errDeadman):
		return kindDeadman
	case errors.Is(err, errPublisherStart):
		return kindPublisher
	default:
		return kindTransient
	}
}

// retryPolicy decides the wait before the next attempt. Not safe for
// concurrent use; one per camera goroutine.
type retryPolicy struct {
	failures  int // consecutive failed attempts
	cooldowns int // cooldowns served since the last success
}

// success resets the policy: the next failure starts with a quick retry.
func (r *retryPolicy) success() {
	r.failures, r.cooldowns = 0, 0
}

// fail records a failed attempt and returns the wait before the next one
// and the retry mode: "retry", "cooldown" (the attempt after it is the
// half-open probe) or "credential_hold".
func (r *retryPolicy) fail(kind string) (time.Duration, string) {
	if kind == kindCredentialRefused {
		return credentialHold, "credential_hold"
	}
	r.failures++
	if r.failures <= len(retryQuickDelays) {
		return retryQuickDelays[r.failures-1], "retry"
	}
	d := retryCooldownFirst << r.cooldowns
	if d <= 0 || d > retryCooldownMax {
		d = retryCooldownMax
	} else {
		r.cooldowns++
	}
	return d, "cooldown"
}

// lanFailuresBeforeRelayTrial: consecutive sessions on the LAN route that
// produced no video before one attempt goes relay-only.
const lanFailuresBeforeRelayTrial = 2

// routeChooser keeps repeated sessions from choosing the same unsuccessful
// route. After lanFailuresBeforeRelayTrial LAN sessions without video, one
// attempt runs without the camera's LAN address (cloud relay only); the
// attempt after that tries LAN again. Unicast only either way.
type routeChooser struct {
	lanFailures int
}

func (c *routeChooser) useLAN(haveLAN bool) bool {
	return haveLAN && c.lanFailures < lanFailuresBeforeRelayTrial
}

func (c *routeChooser) result(usedLAN, gotVideo bool) {
	switch {
	case !usedLAN:
		c.lanFailures = 0 // relay had its turn; LAN is tried again next
	case gotVideo:
		c.lanFailures = 0
	default:
		c.lanFailures++
	}
}
