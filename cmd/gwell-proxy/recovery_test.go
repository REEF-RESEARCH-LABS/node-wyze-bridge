package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IDisposable/docker-wyze-bridge/internal/gwell/upstream/gwell"
)

func withStateDir(t *testing.T) {
	t.Helper()
	t.Setenv("STATE_DIR", t.TempDir())
}

// A token refresh reusing the cached endpoint must not reset its age.
func TestEndpointAgesFromDiscoveryNotTokenRefresh(t *testing.T) {
	withStateDir(t)
	discovered := time.Now().Add(-endpointTTL + time.Minute)
	saveCache(&tokenCache{AccessID: "a", AccessToken: "t", ServerAddr: "3.13.212.24:28800",
		CachedAt: discovered.Unix(), TTLSeconds: int64(endpointTTL.Seconds()), DiscoveredAt: discovered.Unix()})

	// Token-only refresh (the hourly loop): never discovers.
	cred := &shimAccessCredential{AccessID: "a2", AccessToken: "t2"}
	updateCache(func(tc *tokenCache) bool {
		tc.AccessID, tc.AccessToken, tc.CachedAt = cred.AccessID, cred.AccessToken, time.Now().Unix()
		return true
	})
	tc := loadCache()
	if tc.DiscoveredAt != discovered.Unix() {
		t.Fatalf("token refresh moved discoveredAt %d -> %d", discovered.Unix(), tc.DiscoveredAt)
	}
	if tc.AccessToken != "t2" {
		t.Fatal("token not saved")
	}
	if !tc.endpointUsable(time.Now()) {
		t.Fatal("endpoint should still be usable inside its TTL")
	}
	if tc.endpointUsable(time.Now().Add(2 * time.Minute)) {
		t.Fatal("endpoint still usable after its discovery TTL; token refreshes extended it")
	}
}

func TestLegacyCacheWithoutDiscoveryTimeIsRediscovered(t *testing.T) {
	// A pinned-bridge file: cachedAt re-stamped on every reuse, 7-day TTL.
	tc := &tokenCache{AccessID: "a", AccessToken: "t", ServerAddr: "3.13.212.24:28800",
		CachedAt: time.Now().Unix(), TTLSeconds: int64((7 * 24 * time.Hour).Seconds())}
	if tc.endpointUsable(time.Now()) {
		t.Fatal("endpoint of unknown age reused")
	}
}

// Bounded failed certification/health attempts invalidate the endpoint;
// video clears the count.
func TestEndpointInvalidatedAfterFailedAttempts(t *testing.T) {
	withStateDir(t)
	now := time.Now()
	saveCache(&tokenCache{AccessID: "a", AccessToken: "t", ServerAddr: "3.13.212.24:28800",
		CachedAt: now.Unix(), TTLSeconds: int64(endpointTTL.Seconds()), DiscoveredAt: now.Unix()})
	for i := 1; i < endpointFailLimit; i++ {
		if n, invalid := noteEndpointFailure("certify"); n != i || invalid {
			t.Fatalf("attempt %d: n=%d invalid=%v", i, n, invalid)
		}
		if !loadCache().endpointUsable(now) {
			t.Fatalf("invalidated after %d failures", i)
		}
	}
	noteEndpointHealthy()
	if loadCache().FailedAttempts != 0 {
		t.Fatal("video did not clear the failure count")
	}
	for i := 0; i < endpointFailLimit; i++ {
		noteEndpointFailure("certify")
	}
	if loadCache().endpointUsable(now) {
		t.Fatalf("endpoint still used after %d failed attempts", endpointFailLimit)
	}
}

// Quick retries, then cooldowns that grow to a ceiling and keep retrying:
// never a permanent latch. Success starts over.
func TestRetryNeverLatchesAndSuccessResets(t *testing.T) {
	var r retryPolicy
	var modes []string
	var total time.Duration
	for i := 0; i < 50; i++ {
		d, mode := r.fail(kindTransient)
		if d <= 0 || d > retryCooldownMax {
			t.Fatalf("attempt %d: delay %s", i, d)
		}
		total += d
		modes = append(modes, mode)
	}
	for i := range retryQuickDelays {
		if modes[i] != "retry" {
			t.Fatalf("attempt %d mode %s, want retry", i, modes[i])
		}
	}
	if modes[len(retryQuickDelays)] != "cooldown" || modes[49] != "cooldown" {
		t.Fatalf("modes %v", modes)
	}
	if d, _ := r.fail(kindTransient); d != retryCooldownMax {
		t.Fatalf("ceiling %s", d)
	}
	r.success()
	if d, mode := r.fail(kindDeadman); d != retryQuickDelays[0] || mode != "retry" {
		t.Fatalf("after success: %s %s", d, mode)
	}
}

func TestCredentialRefusalIsHeldNotHammered(t *testing.T) {
	var r retryPolicy
	refused := &shimStatusError{Path: "/Camera/CameraToken", Code: 403, Body: "credential refused"}
	if !isCredentialRefused(refused) || classifyError(refused) != kindCredentialRefused {
		t.Fatal("403 not classified as refusal")
	}
	if isCredentialRefused(&shimStatusError{Code: 502}) || isCredentialRefused(&shimStatusError{Code: 503}) {
		t.Fatal("transient shim errors classified as refusal")
	}
	d, mode := r.fail(kindCredentialRefused)
	if d != credentialHold || mode != "credential_hold" || r.failures != 0 {
		t.Fatalf("refusal: %s %s failures=%d", d, mode, r.failures)
	}
}

func TestClassifyTerminalAndDeadman(t *testing.T) {
	term := fmt.Errorf("session ended: %w", &gwell.TerminalControlError{Action: gwell.AVActionCLOSE})
	if classifyError(term) != kindTerminalControl {
		t.Fatal(classifyError(term))
	}
	if classifyError(fmt.Errorf("%w: x", errDeadman)) != kindDeadman {
		t.Fatal("deadman")
	}
}

// Repeated sessions must not keep choosing a LAN route that never delivered.
func TestRouteChooserGivesTheRelayATurn(t *testing.T) {
	var c routeChooser
	var seq []bool
	for i := 0; i < 6; i++ {
		lan := c.useLAN(true)
		seq = append(seq, lan)
		c.result(lan, false)
	}
	want := []bool{true, true, false, true, true, false}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("route sequence %v, want %v", seq, want)
		}
	}
	c.result(true, true)
	if !c.useLAN(true) || c.useLAN(false) {
		t.Fatal("LAN success not honoured / LAN chosen without an address")
	}
}

func TestStatusFileShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gwell", "status.json")
	b := &statusBoard{path: path, cameras: map[string]*cameraStatus{}, dirty: make(chan struct{}, 1)}
	b.update("D03F27AABBCC", func(st *cameraStatus) {
		st.Stream = "window-cam"
		st.Stage = "forwarding"
		st.Generation = 3
		st.Route = "lan"
	})
	b.writeOnce()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f map[string]any
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	cam := f["cameras"].(map[string]any)["D03F27AABBCC"].(map[string]any)
	for _, k := range []string{"stage", "generation", "last_error_at", "last_video_at", "next_retry_at", "endpoint", "stage_since"} {
		if _, ok := cam[k]; !ok {
			t.Fatalf("status missing %q: %s", k, raw)
		}
	}
	if cam["last_video_at"] != nil || cam["next_retry_at"] != nil {
		t.Fatal("unmeasured times must be null, not zero")
	}
	if !strings.Contains(string(raw), `"stage": "forwarding"`) {
		t.Fatalf("%s", raw)
	}
}
