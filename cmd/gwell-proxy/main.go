// Package main is the Wyze Gwell P2P proxy — the bridge-side sidecar
// that speaks Wyze's Gwell/IoTVideo protocol to GW_* cameras (Doorbell
// Pro, OG, OG 3X, Doorbell Duo) and publishes H.264 via RTSP PUSH to
// our loopback go2rtc.
//
// Ported (with import path swaps) from the `cmd/gwell-proxy/main.go`
// of wlatic/hacky-wyze-gwell at SHA bilbaraski/hacky-wyze-gwell@c930a32
// (that fork has the cmd/ directory upstream's main branch is missing
// at pinned SHA 9c1b99f8 — see internal/gwell/upstream/README.md).
//
// Architectural divergence from the upstream fork:
//   - pkg/wyze (upstream's client for a companion Python wyze-api
//     service) is replaced by a local HTTP client in wyzeshim.go that
//     hits our bridge's internal shim endpoints. Our bridge owns the
//     Wyze cloud auth — the shim just re-exposes it in the shape the
//     proxy expects so the port below stays verbatim.
//   - Default API URL points at our bridge's localhost shim, not a
//     separate macvlan-addressed wyze-api container.
//   - Default RTSP target is go2rtc on :8554 (RTSP PUSH compatible
//     with mediamtx's dialect).
//   - cacheFile respects STATE_DIR so token_cache.json persists
//     alongside wyze-bridge.state.json.
//
// v2 upstream notes preserved:
// - Serialized P2P session setup via connectMu to prevent cross-session interference
// - Fresh discovery on reconnect when both cameras are down
// - Proper 15s stagger between all session setups, not just initial start
// - DeviceName set to cameraID for correct device targeting
//
// Node One 4.6.1-node.17 (REE-1054, Window Cam session survives):
//   - publish.go: video publication never blocks the session goroutine or
//     the supervisor; bounded queue, write deadline, drop to keyframe.
//   - supervise.go: stop handling, publisher replacement and the deadman
//     run independently of output backpressure.
//   - endpoint.go: P2P endpoints age from real discovery only and are
//     invalidated after endpointFailLimit failed attempts.
//   - retry.go: quick retries, then expiring cooldown + half-open attempt;
//     credential refusal held; LAN/relay route rotation.
//   - status.go: per-camera stage/generation/errors in --status-file.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IDisposable/docker-wyze-bridge/internal/gwell/upstream/gwell"
	"github.com/IDisposable/docker-wyze-bridge/internal/gwell/upstream/stream"
)

const (
	tokenRefreshInterval = 1 * time.Hour
	cameraStagger        = 15 * time.Second
	// superviseTick is how often the supervisor checks the publisher and
	// the deadman and refreshes the status.
	superviseTick = 5 * time.Second
)

// connectMu serializes P2P session setup across all cameras.
// The GWell P2P server gets confused when multiple sessions from the same
// account do InitInfoMsg simultaneously — frames leak between sessions,
// causing "bad count" decryption failures. Only one camera should be in
// the connect→certify→initInfo→subscribe→calling phase at a time.
var connectMu sync.Mutex

// Per-second arrival timing (2026-10-09): the dump shows the
// camera's frames are clean, so stalls are about WHEN they arrive. Every
// second: bytes, writes, Annex B frame starts, and the longest gap between
// two writes.
type arrivalMeter struct {
	mu     sync.Mutex
	start  time.Time
	last   time.Time
	bytes  int
	writes int
	frames int
	maxGap time.Duration
	camera string
}

func (a *arrivalMeter) note(p []byte) {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.start.IsZero() {
		a.start = now
	}
	if !a.last.IsZero() {
		if gap := now.Sub(a.last); gap > a.maxGap {
			a.maxGap = gap
		}
	}
	a.last = now
	a.bytes += len(p)
	a.writes++
	for i := 0; i+4 <= len(p); i++ {
		if p[i] == 0 && p[i+1] == 0 && p[i+2] == 1 {
			if t := p[i+3] & 0x1f; t == 1 || t == 5 {
				a.frames++
			}
		}
	}
	if now.Sub(a.start) >= time.Second {
		log.Printf("[%s] arrival: %dB %d writes %d frames, longest gap %dms", a.camera, a.bytes, a.writes, a.frames, a.maxGap.Milliseconds())
		a.start, a.bytes, a.writes, a.frames, a.maxGap = now, 0, 0, 0, 0
	}
}

// shared state protected by sharedMu
var (
	sharedMu      sync.Mutex
	sharedToken   *gwell.AccessToken
	sharedServer  string
	sharedDevices []gwell.DeviceInfo
)

type proxyOptions struct {
	rtspHost       string
	rtspPort       int
	dumpDir        string
	ffmpegLogLevel string
	deadmanTimeout time.Duration
}

func main() {
	// Configuration flows from the parent wyze-bridge process as CLI
	// flags — the bridge already knows every value it needs to hand us
	// (its own webui host+port for the shim URL, its managed go2rtc's
	// RTSP listener). No env-var fallback: if anyone runs this binary
	// standalone they're opting into setting these explicitly.
	apiURL := flag.String("shim-url", "", "URL of the bridge's wyze-shim, e.g. http://127.0.0.1:5080/internal/wyze")
	rtspHost := flag.String("rtsp-host", "127.0.0.1", "host of the RTSP server we PUSH to (go2rtc)")
	rtspPort := flag.Int("rtsp-port", 8554, "port of the RTSP server we PUSH to")
	dumpDir := flag.String("dump-h264", "", "if set, tee raw H.264 bytes per-camera into <dir>/<cam>-<unix-ms>.h264 for offline analysis")
	ffmpegLogLevel := flag.String("ffmpeg-loglevel", "warning", "ffmpeg -loglevel (quiet/panic/fatal/error/warning/info/verbose/debug/trace); debug spams thousands of lines/second")
	deadmanTimeout := flag.Duration("deadman-timeout", 2*time.Minute, "max no-data interval before forcing reconnect")
	statusFilePath := flag.String("status-file", "", "if set, write per-camera lifecycle status JSON here")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("[main] Wyze GWell P2P proxy starting")

	if *apiURL == "" {
		log.Fatalf("[main] --shim-url is required")
	}

	log.Printf("[main] shim=%s RTSP=%s:%d", *apiURL, *rtspHost, *rtspPort)

	client := newWyzeShimClient(*apiURL)
	status := newStatusBoard(*statusFilePath)
	opts := proxyOptions{
		rtspHost:       *rtspHost,
		rtspPort:       *rtspPort,
		dumpDir:        *dumpDir,
		ffmpegLogLevel: *ffmpegLogLevel,
		deadmanTimeout: *deadmanTimeout,
	}

	// Poll the shim until at least one Gwell camera is registered.
	// Two independent reasons the list can be empty temporarily:
	//   1. Bridge is still doing its first Wyze discovery pass
	//   2. User has GWELL_ENABLED=true but no GW_* cameras on their
	//      account yet (e.g. they're buying one tomorrow)
	// Don't fatal in either case — the bridge's supervision loop
	// treats fatal-exit as a crash and restart-storms. Patient
	// polling is the right semantics.
	log.Println("[main] Waiting for Gwell cameras via shim...")
	// The shim is on loopback and answers at once, so the first looks come
	// quickly (2 s, doubling to 30 s): a fixed 30 s wait cost a Window Cam
	// up to half a minute on every start.
	var cameraIDs []string
	wait := 2 * time.Second
	for {
		ids, err := client.GetCameraList()
		if err != nil {
			log.Printf("[main] shim GetCameraList: %v (retrying in %s)", err, wait)
		} else if len(ids) == 0 {
			log.Printf("[main] shim reports 0 Gwell cameras (retrying in %s)", wait)
		} else {
			cameraIDs = ids
			log.Printf("[main] Found %d camera(s): %v", len(ids), ids)
			break
		}
		time.Sleep(wait)
		if wait *= 2; wait > 30*time.Second {
			wait = 30 * time.Second
		}
	}
	for _, id := range cameraIDs {
		status.setStage(id, stageStarting)
	}

	// Initial routing. A failure is no longer fatal: each camera's loop
	// retries it with backoff (exiting made the bridge restart us in a loop).
	if cred, err := client.GetCameraToken(cameraIDs[0]); err != nil {
		log.Printf("[main] Initial token fetch failed: %v (camera loops will retry)", err)
	} else if err := applyRouting(cred, true); err != nil {
		log.Printf("[main] Initial discovery failed: %v (camera loops will retry)", err)
	}

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Start per-camera goroutines with stagger
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i, camID := range cameraIDs {
		if i > 0 {
			log.Printf("[main] Staggering camera start (%s)...", cameraStagger)
			time.Sleep(cameraStagger)
		}

		wg.Add(1)
		go func(cameraID string) {
			defer wg.Done()
			runCamera(client, cameraID, opts, status, stopCh)
		}(camID)
	}

	// Token refresh loop. Refreshes the credential only: it never resets
	// the endpoint's discovery age and never runs a discovery while
	// cameras stream (a second InitInfo on the same account disturbs
	// live sessions). Rediscovery happens before a camera's next attempt.
	go func() {
		ticker := time.NewTicker(tokenRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("[main] Refreshing token...")
				cred, err := client.GetCameraToken(cameraIDs[0])
				if err == nil {
					err = applyRouting(cred, false)
				}
				if err != nil {
					log.Printf("[main] Token refresh failed: %v", err)
				} else {
					log.Println("[main] Token refreshed (will apply on next reconnect)")
				}
			case <-stopCh:
				return
			}
		}
	}()

	// Wait for signal
	sig := <-sigCh
	log.Printf("[main] Received signal %v, shutting down...", sig)
	close(stopCh)
	wg.Wait()
	log.Println("[main] Shutdown complete")
}

// applyRouting installs a fresh credential and makes sure a usable P2P
// endpoint is known. The cached endpoint is used while it is younger than
// endpointTTL (counted from real discovery) and has not failed
// endpointFailLimit attempts; otherwise, when allowDiscover is set, a fresh
// cloud discovery runs. Saving a new credential never re-stamps the
// endpoint's age.
func applyRouting(cred *shimAccessCredential, allowDiscover bool) error {
	token, err := gwell.ParseAccessToken(cred.AccessID, cred.AccessToken)
	if err != nil {
		return fmt.Errorf("parse token: %w", err)
	}
	now := time.Now()

	cacheMu.Lock()
	cache := loadCache()
	cacheMu.Unlock()

	if cache.endpointUsable(now) || !allowDiscover {
		sharedMu.Lock()
		sharedToken = token
		if cache.endpointUsable(now) {
			sharedServer = cache.ServerAddr
		}
		// Keep existing devices if we have them
		sharedMu.Unlock()
		updateCache(func(tc *tokenCache) bool {
			tc.AccessID, tc.AccessToken, tc.CachedAt = cred.AccessID, cred.AccessToken, now.Unix()
			return true
		})
		if cache.endpointUsable(now) {
			log.Printf("[main] Using cached P2P server address %s (discovered %s ago, %d failed attempts)",
				cache.ServerAddr, now.Sub(time.Unix(cache.DiscoveredAt, 0)).Round(time.Second), cache.FailedAttempts)
		}
		return nil
	}

	log.Println("[main] Running device discovery (cloud routing; no LAN broadcast)...")
	result, err := gwell.DiscoverDevices(token)
	if err != nil {
		sharedMu.Lock()
		sharedToken = token
		sharedMu.Unlock()
		return fmt.Errorf("discovery: %w", err)
	}
	log.Printf("[main] Discovery complete: server=%s, %d device(s)", result.ServerAddr, len(result.Devices))

	sharedMu.Lock()
	sharedToken = token
	sharedServer = result.ServerAddr
	sharedDevices = result.Devices
	sharedMu.Unlock()

	updateCache(func(tc *tokenCache) bool {
		*tc = tokenCache{
			AccessID:     cred.AccessID,
			AccessToken:  cred.AccessToken,
			ServerAddr:   result.ServerAddr,
			CachedAt:     now.Unix(),
			TTLSeconds:   int64(endpointTTL.Seconds()),
			DiscoveredAt: now.Unix(),
		}
		return true
	})
	return nil
}

// getSharedState returns a snapshot of the current shared state.
func getSharedState() (*gwell.AccessToken, string, []gwell.DeviceInfo) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	return sharedToken, sharedServer, sharedDevices
}

func endpointSnapshot() endpointStatus {
	cacheMu.Lock()
	tc := loadCache()
	cacheMu.Unlock()
	if tc == nil {
		return endpointStatus{}
	}
	var at time.Time
	if tc.DiscoveredAt != 0 {
		at = time.Unix(tc.DiscoveredAt, 0)
	}
	return endpointStatus{DiscoveredAt: ptrTime(at), FailedAttempts: tc.FailedAttempts}
}

// runCamera is the reconnect loop for a single camera. Each pass is one
// session generation.
func runCamera(client *wyzeShimClient, cameraID string, opts proxyOptions, status *statusBoard, stopCh chan struct{}) {
	var retry retryPolicy
	var routes routeChooser
	var generation uint64

	// wait sleeps before the next attempt, reporting when it will be.
	wait := func(d time.Duration, mode string, err error) bool {
		next := time.Now().Add(d)
		status.update(cameraID, func(st *cameraStatus) {
			if mode == "credential_hold" {
				st.Stage = stageCredentialRefused
			} else {
				st.Stage = stageWaitingRetry
			}
			st.NextRetryAt = ptrTime(next)
			st.RetryMode = mode
			st.ConsecutiveFailures = retry.failures
			if err != nil {
				st.LastError = err.Error()
				st.LastErrorKind = classifyError(err)
				st.LastErrorAt = ptrTime(time.Now())
			}
			st.Endpoint = endpointSnapshot()
		})
		log.Printf("[%s] next attempt in %s (%s)", cameraID, d, mode)
		select {
		case <-stopCh:
			return false
		case <-time.After(d):
			status.update(cameraID, func(st *cameraStatus) { st.NextRetryAt = nil })
			return true
		}
	}

	for {
		select {
		case <-stopCh:
			log.Printf("[%s] Stop signal received", cameraID)
			status.setStage(cameraID, stageStopped)
			return
		default:
		}

		// Fresh credential on each attempt; refresh routing when the
		// cached endpoint is too old or has failed too often.
		status.setStage(cameraID, stageAuthorizing)
		cred, err := client.GetCameraToken(cameraID)
		if err != nil && isCredentialRefused(err) {
			log.Printf("[%s] Credential refused by Wyze: %v", cameraID, err)
			d, mode := retry.fail(kindCredentialRefused)
			if !wait(d, mode, err) {
				status.setStage(cameraID, stageStopped)
				return
			}
			continue
		}
		if err != nil {
			log.Printf("[%s] Token fetch failed: %v, using shared token", cameraID, err)
		} else if rerr := applyRouting(cred, true); rerr != nil {
			log.Printf("[%s] Routing refresh failed: %v", cameraID, rerr)
		}
		if token, _, _ := getSharedState(); token == nil {
			if err == nil {
				err = fmt.Errorf("no camera credential yet")
			}
			d, mode := retry.fail(kindTransient)
			if !wait(d, mode, err) {
				status.setStage(cameraID, stageStopped)
				return
			}
			continue
		}

		generation++
		res := streamCamera(client, cameraID, generation, opts, &routes, status, stopCh)
		routes.result(res.usedLAN, res.gotVideo)
		if res.gotVideo {
			retry.success()
			noteEndpointHealthy()
		} else if res.err != nil && res.sessionRan {
			noteEndpointFailure(res.err.Error())
		}
		if res.err == nil {
			status.setStage(cameraID, stageStopped)
			return
		}
		log.Printf("[%s] Stream error: %v", cameraID, res.err)
		log.Printf("[%s] reconnect reason: %s", cameraID, classifyError(res.err))
		d, mode := retry.fail(classifyError(res.err))
		if !wait(d, mode, res.err) {
			status.setStage(cameraID, stageStopped)
			return
		}
		log.Printf("[%s] Reconnecting...", cameraID)
	}
}

type sessionResult struct {
	err        error
	gotVideo   bool
	usedLAN    bool
	sessionRan bool // a P2P session was attempted (counts against the endpoint)
}

// streamCamera runs a single streaming session (one generation) for one
// camera. It acquires connectMu to serialize the P2P handshake phase.
func streamCamera(client *wyzeShimClient, cameraID string, generation uint64, opts proxyOptions,
	routes *routeChooser, status *statusBoard, stopCh chan struct{}) (res sessionResult) {

	// Get device info for stream name and LAN IP
	info, err := client.GetDeviceInfo(cameraID)
	if err != nil {
		res.err = fmt.Errorf("get device info: %w", err)
		return
	}

	streamName := info.StreamName
	if streamName == "" {
		streamName = cameraID
	}
	streamPath := streamName
	// Bridge convention: stream paths land at rtsp://go2rtc:8554/<name>
	// directly (no "live/" prefix that upstream mediamtx convention used).
	// If your go2rtc is configured to expect a prefix, set GWELL_STREAM_PREFIX.
	if prefix := os.Getenv("GWELL_STREAM_PREFIX"); prefix != "" && !strings.HasPrefix(streamPath, prefix) {
		streamPath = prefix + streamPath
	}

	// Route trial across sessions: after repeated LAN sessions without
	// video, one attempt goes through the cloud relay only.
	lanIP := info.LanIP
	if !routes.useLAN(lanIP != "") {
		if lanIP != "" {
			log.Printf("[%s] LAN route failed %d sessions without video: this attempt is relay only", cameraID, routes.lanFailures)
		}
		lanIP = ""
	}
	res.usedLAN = lanIP != ""

	log.Printf("[%s] Starting stream: %s (LAN IP: %s) generation %d", cameraID, streamPath, lanIP, generation)
	status.update(cameraID, func(st *cameraStatus) {
		st.Stream = streamName
		st.Generation = generation
		st.Route = ""
		st.LastControl = ""
		st.LateAccepts = 0
		st.Endpoint = endpointSnapshot()
	})

	// Start ffmpeg publisher
	fps := gwellFPS(cameraID)
	log.Printf("[%s] stamping video at %d fps", cameraID, fps)
	newPublisher := func() (publisher, error) {
		return stream.StartFFmpegPublisherFPS(streamPath, opts.rtspHost, opts.rtspPort, opts.ffmpegLogLevel, fps)
	}
	ffmpeg, err := newPublisher()
	if err != nil {
		res.err = fmt.Errorf("%w: start ffmpeg: %v", errPublisherStart, err)
		return
	}

	// writeTracker queues bytes for ffmpeg (never blocking the session),
	// optionally teeing them to --dump-h264 for offline ffprobe
	// inspection, while tracking camera-arrival time for the deadman
	// switch and byte counts for the session-end summary.
	tracker := newWriteTracker(ffmpeg)
	if os.Getenv("GWELL_ARRIVAL_LOG") != "" {
		tracker.meter = &arrivalMeter{camera: cameraID}
	}
	defer func() {
		// Close the publisher first: it unblocks any write in flight, so
		// the writer goroutine can stop at once.
		if p := tracker.current(); p != nil {
			p.Close()
		}
		tracker.Close()
		in := tracker.bytesIn.Load()
		out := tracker.bytesOut.Load()
		res.gotVideo = in > 0
		// Session-end summary — one log line tells you whether the session
		// produced any video without having to `stat` the dump file.
		log.Printf("[%s] session end: generation=%d bytesIn=%d bytesOut=%d (delta=%d) dropped=%d", cameraID, generation, in, out, int64(in)-int64(out), tracker.dropped.Load())
	}()

	// Optional raw-H.264 dump for offline ffprobe inspection. One file
	// per session, timestamped so successive reconnects don't clobber
	// each other — analyze any of them to confirm the camera stream is
	// well-formed before we blame ffmpeg/go2rtc downstream.
	if opts.dumpDir != "" {
		if err := os.MkdirAll(opts.dumpDir, 0755); err != nil {
			log.Printf("[%s] dump: mkdir %s: %v (continuing without dump)", cameraID, opts.dumpDir, err)
		} else {
			path := filepath.Join(opts.dumpDir, fmt.Sprintf("%s-%d.h264", cameraID, time.Now().UnixMilli()))
			f, err := os.Create(path)
			if err != nil {
				log.Printf("[%s] dump: create %s: %v (continuing without dump)", cameraID, path, err)
			} else {
				log.Printf("[%s] dump: writing raw H.264 to %s", cameraID, path)
				tracker.mu.Lock()
				tracker.dump = f
				tracker.mu.Unlock()
			}
		}
	}

	// === SERIALIZED SECTION ===
	// Acquire the connect mutex so only one camera is doing the P2P
	// handshake at a time. This prevents cross-session interference
	// on the P2P server.
	status.setStage(cameraID, stageWaitingTurn)
	log.Printf("[%s] Waiting for connect lock...", cameraID)
	connectMu.Lock()
	log.Printf("[%s] Acquired connect lock, starting P2P session", cameraID)

	token, serverAddr, devices := getSharedState()

	sess := gwell.NewSession(gwell.SessionConfig{
		Token:       token,
		ServerAddr:  serverAddr,
		CameraLanIP: lanIP,
		DeviceName:  cameraID,
		H264Writer:  tracker,
		Devices:     devices,
	})
	// The first-video deadline starts with the real attempt.
	tracker.sessionStart.Store(time.Now().UnixNano())

	// Run the session — it will go through connect, certify, initInfo,
	// subscribe, calling, and then enter streamLoop. We release the
	// lock after a delay to let the handshake complete before the next
	// camera tries.
	errCh := make(chan error, 1)
	res.sessionRan = true
	go func() {
		err := sess.Run(cameraID)
		if err == nil {
			err = fmt.Errorf("stream loop ended")
		}
		errCh <- err
	}()

	// Release the lock after the stagger delay so the next camera can go
	go func() {
		time.Sleep(cameraStagger)
		connectMu.Unlock()
		log.Printf("[%s] Released connect lock", cameraID)
	}()
	// === END SERIALIZED SECTION ===

	var restarts uint64
	refresh := func() {
		status.update(cameraID, func(st *cameraStatus) {
			if stage := sess.Stage(); stage != "" {
				st.Stage = stage
			}
			st.Route = sess.Route()
			st.LastControl = sess.LastControl()
			st.LateAccepts = sess.LateAccepts()
			if t := tracker.lastIngressTime(); !t.IsZero() {
				st.LastVideoAt = ptrTime(t)
			}
			st.BytesIn = tracker.bytesIn.Load()
			st.BytesOut = tracker.bytesOut.Load()
			st.DroppedChunks = tracker.dropped.Load()
			st.StuckWrites = tracker.stuck.Load()
			st.PublisherRestarts = restarts
		})
	}

	// Monitor: deadman switch + publisher health + stop signal. Runs
	// independently of publisher backpressure.
	res.err = supervise(sess, errCh, stopCh, tracker, superviseConfig{
		camera:       cameraID,
		tick:         superviseTick,
		deadman:      opts.deadmanTimeout,
		stuckAfter:   2 * publishWriteTimeout,
		newPublisher: newPublisher,
		onTick:       refresh,
		onRestart:    func() { restarts++ },
	})
	refresh()
	return res
}

// gwellFPS is the frame rate a Gwell camera sends, by model (the device ID
// starts with the model code). GWELL_FPS overrides it for every camera.
// Window Cam measured at 20 fps (2026-10-09); Doorbell Pro and OG keep
// upstream's 15.
func gwellFPS(cameraID string) int {
	if v, err := strconv.Atoi(os.Getenv("GWELL_FPS")); err == nil && v > 0 && v <= 60 {
		return v
	}
	switch {
	case strings.HasPrefix(cameraID, "GW_WC"):
		return 20
	default:
		return 15
	}
}
