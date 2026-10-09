package stream

import (
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// FFmpegPublisher pipes raw H.264 Annex B data to ffmpeg, which publishes
// the stream via RTSP PUSH (ANNOUNCE/RECORD). The target is any RTSP
// server that accepts publishes — upstream targets mediamtx; our bridge
// points it at go2rtc on loopback. Both speak the same RTSP dialect.
type FFmpegPublisher struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	mu    sync.Mutex
	done  chan struct{}
}

// StartFFmpegPublisher spawns an ffmpeg process that reads raw H.264 from
// stdin and publishes it via RTSP PUSH to rtsp://<rtspHost>:<rtspPort>/<streamPath>.
//
// logLevel maps directly to ffmpeg's -loglevel flag (quiet/panic/fatal/
// error/warning/info/verbose/debug/trace). Empty string defaults to
// "warning" — the debug setting produces dozens of lines per frame
// which drowns out everything else in the bridge log; flip to debug
// only when diagnosing ffmpeg itself.
func StartFFmpegPublisher(streamPath, rtspHost string, rtspPort int, logLevel string) (*FFmpegPublisher, error) {
	return StartFFmpegPublisherFPS(streamPath, rtspHost, rtspPort, logLevel, 15)
}

// StartFFmpegPublisherFPS is StartFFmpegPublisher with the camera's real
// frame rate. The rate is how ffmpeg stamps raw H.264 (it has no
// timestamps of its own): stamped slower than it arrives, the stream's
// clock falls behind, players pile up video and jump ahead, and a jump
// shows black until the next keyframe. Measured 2026-10-09: a
// Window Cam (GW_WC, fw 1.3.0.63) sends 1080p at 20 fps with a keyframe
// every 80 frames; stamped at 15 it played ~5 s, froze, went black.
func StartFFmpegPublisherFPS(streamPath, rtspHost string, rtspPort int, logLevel string, fps int) (*FFmpegPublisher, error) {
	if logLevel == "" {
		logLevel = "info"
	}
	if fps <= 0 {
		fps = 15
	}
	rtspURL := fmt.Sprintf("rtsp://%s:%d/%s", rtspHost, rtspPort, streamPath)
	log.Printf("[ffmpeg] Publishing to %s (loglevel=%s)", rtspURL, logLevel)

	// Timestamp dance for raw H.264 Annex B → RTSP PUSH, go2rtc-compatible:
	//
	//   -use_wallclock_as_timestamps 1  — initial PTS source since
	//     raw H.264 has no container timestamps. Alone, this produces
	//     wallclock-based 90kHz RTP ticks that go2rtc rejects.
	//   -fflags +genpts+igndts          — let ffmpeg's bitstream layer
	//     synthesize monotonic PTS (ignoring DTS which is also absent);
	//     works in concert with +use_wallclock_as_timestamps rather than
	//     as a replacement.
	//   -r 15                           — input framerate hint.
	//     Doorbell Pro encodes at ~15fps; without this ffmpeg can't
	//     compute frame duration for the RTP muxer and emits
	//     "Timestamps are unset in a packet" errors. 15 is a safe
	//     default for GW_* cameras; variable-rate streams still work
	//     because the RTP timestamps stay monotonic.
	//   -bsf:v dump_extra               — force SPS/PPS to appear
	//     in-band in the first keyframe so go2rtc can build the SDP
	//     without a separate sprop-parameter-sets negotiation.
	//   -avoid_negative_ts make_zero    — rebase at stream start so
	//     the RTP sender's first packet has timestamp 0 rather than
	//     a gigantic wallclock-derived value that looks like a rollover
	//     to consumers.
	// Copy mode: remux the camera's H.264 bitstream into RTSP without
	// decoding or re-encoding. This avoids the CPU cost of libx264 and
	// prevents ffmpeg's decoder from choking on residual IoTVideo framing
	// bytes at GOP boundaries. The browser/go2rtc decoder is typically
	// more tolerant of occasional corruption than ffmpeg's full
	// decode→encode pipeline.
	//
	// Timestamp handling for raw H.264 → RTSP copy:
	//   -use_wallclock_as_timestamps 1 + -fflags +genpts+igndts
	//     assigns monotonic PTS from wallclock, ignoring absent DTS.
	//   -r 15 hints the frame rate so the RTP muxer can compute
	//     correct timestamp increments.
	//   -bsf:v dump_extra forces SPS/PPS into the first keyframe
	//     so go2rtc can build the SDP without sprop-parameter-sets.
	//   -avoid_negative_ts make_zero rebases the first RTP timestamp
	//     to 0 rather than a large wallclock-derived value.
	//
	// 2026-10-09: no -use_wallclock_as_timestamps. With it AND a
	// fixed -r, frames that arrive in bursts got clock stamps that ran
	// backwards ("Non-monotonic DTS" every few seconds, hidden below),
	// and players stalled or drew green until the next keyframe. Stamping
	// by frame count at the camera's own rate cannot go backwards.
	cmd := exec.Command("ffmpeg",
		"-loglevel", logLevel,
		"-fflags", "+genpts+igndts+nobuffer",
		"-r", strconv.Itoa(fps),
		"-f", "h264",
		"-i", "pipe:0",
		"-c:v", "copy",
		// Access unit delimiters (2026-10-09): without them
		// go2rtc's web-player path (MSE/HLS/MP4) merged ~6% of a Window
		// Cam's frames into their neighbours (decoder: "Frame num
		// change", decode_slice_header error), and browsers froze or drew
		// green until the next keyframe. Same recording with AUDs: every
		// frame, no errors. RTSP consumers never showed it.
		"-bsf:v", "h264_metadata=aud=insert,dump_extra",
		"-avoid_negative_ts", "make_zero",
		"-f", "rtsp",
		"-rtsp_transport", "tcp",
		rtspURL,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg stdin pipe: %w", err)
	}

	// Send ffmpeg stderr to our log
	cmd.Stderr = &logWriter{prefix: "[ffmpeg]"}

	if err := cmd.Start(); err != nil {
		stdin.Close()
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}

	p := &FFmpegPublisher{
		cmd:   cmd,
		stdin: stdin,
		done:  make(chan struct{}),
	}

	go func() {
		err := cmd.Wait()
		log.Printf("[ffmpeg] Process exited: %v", err)
		close(p.done)
	}()

	log.Printf("[ffmpeg] Started (pid %d)", cmd.Process.Pid)
	return p, nil
}

// Write sends raw H.264 Annex B data to ffmpeg's stdin.
func (p *FFmpegPublisher) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.done:
		return 0, fmt.Errorf("ffmpeg process has exited")
	default:
	}

	return p.stdin.Write(data)
}

// Alive returns true if the ffmpeg process is still running.
func (p *FFmpegPublisher) Alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Close terminates the ffmpeg process.
func (p *FFmpegPublisher) Close() error {
	p.stdin.Close()
	select {
	case <-p.done:
		return nil
	default:
		return p.cmd.Process.Kill()
	}
}

// logWriter adapts log.Printf to io.Writer for ffmpeg stderr.
type logWriter struct {
	prefix string
}

func (w *logWriter) Write(p []byte) (int, error) {
	s := strings.TrimSpace(string(p))
	// Count DTS warnings instead of dropping them silently: they mean the
	// stream's timestamps ran backwards, which players show as stalls.
	if strings.Contains(s, "Non-monotonic DTS") || strings.Contains(s, "changing to") {
		n := dtsWarnings.Add(1)
		if n == 1 || n%50 == 0 {
			log.Printf("%s non-monotonic DTS warnings so far: %d", w.prefix, n)
		}
		return len(p), nil
	}
	// ffmpeg writes its "[rtsp @ 0x..]" context and the message in
	// separate writes; a bare context prefix carries nothing.
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") && strings.Contains(s, " @ 0x") {
		return len(p), nil
	}
	log.Printf("%s %s", w.prefix, s)
	return len(p), nil
}

var dtsWarnings atomic.Uint64
