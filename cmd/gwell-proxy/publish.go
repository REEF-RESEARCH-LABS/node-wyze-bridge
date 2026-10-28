package main

// publish.go — the path from the camera session to the ffmpeg publisher.
//
// REE-1054 (4.6.1-node.17): video publication must never block the session
// goroutine that also runs heartbeats, meter probes, INITREQ renewals and
// stop handling, nor the supervisor that runs the deadman and publisher
// checks. So:
//
//   - Write (called by the session) only copies the chunk into a bounded
//     queue. It never waits on the publisher.
//   - When the queue is full, chunks are dropped until the next keyframe
//     (SPS or IDR), so the publisher resumes on something decodable instead
//     of accumulating stale video.
//   - One writer goroutine drains the queue into the current publisher.
//     Each write has a deadline; a write that overruns it closes that
//     publisher (which unblocks the write), and the supervisor starts a
//     fresh one primed with the last SPS/PPS. The camera session is kept.
//   - pubMu only guards the publisher pointer; it is never held across a
//     write, so the supervisor can always read or swap the publisher.

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// publishQueueChunks bounds the session→publisher queue. A Window Cam
	// sends 20 fps; this is several seconds of video.
	publishQueueChunks = 256
	// publishWriteTimeout bounds one write into the publisher.
	publishWriteTimeout = 5 * time.Second
)

// publisher is what the tracker writes into: an ffmpeg publisher, or a
// fake in tests.
type publisher interface {
	Write([]byte) (int, error)
	Alive() bool
	Close() error
}

// writeTracker feeds the publisher and keeps the timings the supervisor
// and the status file need.
type writeTracker struct {
	pubMu  sync.Mutex
	pub    publisher // replaced by swap when it dies
	pubGen uint64    // bumped by swap; the writer primes a new publisher

	mu     sync.Mutex     // guards params and dump
	params [][]byte       // the last SPS and PPS seen, start codes included
	dump   io.WriteCloser // nil when --dump-h264 isn't set

	sessionStart atomic.Int64  // unix nano; deadman baseline before first video
	lastIngress  atomic.Int64  // unix nano of the last chunk from the camera
	lastWrite    atomic.Int64  // unix nano of the last byte into the publisher
	inflight     atomic.Int64  // unix nano a publisher write began, 0 when idle
	inflightGen  atomic.Uint64 // pubGen of the publisher being written
	bytesIn      atomic.Uint64 // raw bytes received from the P2P pipeline
	bytesOut     atomic.Uint64 // bytes actually forwarded to ffmpeg
	dropped      atomic.Uint64 // chunks dropped (queue overflow, waiting for keyframe)
	stuck        atomic.Uint64 // publishers closed for overrunning the write deadline
	enqueued     atomic.Uint64
	handled      atomic.Uint64
	meter        *arrivalMeter // per-second timing, when GWELL_ARRIVAL_LOG is set

	dropping     bool // set and read by Write only (the session goroutine)
	queue        chan []byte
	stop         chan struct{}
	done         chan struct{}
	closeOnce    sync.Once
	writeTimeout time.Duration
	primedGen    uint64 // writer goroutine only
}

func newWriteTracker(pub publisher) *writeTracker {
	return newWriteTrackerWith(pub, publishQueueChunks, publishWriteTimeout)
}

func newWriteTrackerWith(pub publisher, queueLen int, writeTimeout time.Duration) *writeTracker {
	w := &writeTracker{
		pub:          pub,
		queue:        make(chan []byte, queueLen),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		writeTimeout: writeTimeout,
	}
	w.sessionStart.Store(time.Now().UnixNano())
	go w.run()
	return w
}

// Write is called by the camera session. It never blocks on the publisher.
func (w *writeTracker) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.bytesIn.Add(uint64(len(p)))
	w.lastIngress.Store(time.Now().UnixNano())
	if w.meter != nil {
		w.meter.note(p)
	}
	w.mu.Lock()
	if w.dump != nil {
		_, _ = w.dump.Write(p)
	}
	w.keepParams(p)
	w.mu.Unlock()

	if w.dropping {
		if !hasKeyframe(p) {
			w.dropped.Add(1)
			return len(p), nil
		}
		w.dropping = false
	}
	chunk := append([]byte(nil), p...)
	select {
	case w.queue <- chunk:
		w.enqueued.Add(1)
	default:
		// Publisher is behind: drop this and everything up to the next
		// keyframe rather than wait.
		w.dropping = true
		w.dropped.Add(1)
	}
	return len(p), nil
}

// run drains the queue into the current publisher.
func (w *writeTracker) run() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		case p := <-w.queue:
			w.deliver(p)
			w.handled.Add(1)
		}
	}
}

func (w *writeTracker) deliver(p []byte) {
	w.pubMu.Lock()
	pub, gen := w.pub, w.pubGen
	w.pubMu.Unlock()
	if pub == nil {
		return
	}
	if gen != w.primedGen {
		// A fresh publisher: ffmpeg cannot decode a frame of an H.264
		// stream it joins mid-way until it has seen both SPS and PPS.
		w.primedGen = gen
		w.mu.Lock()
		params := w.params
		w.mu.Unlock()
		for _, unit := range params {
			if _, err := w.writeOne(pub, gen, unit); err != nil {
				return
			}
		}
	}
	if n, _ := w.writeOne(pub, gen, p); n > 0 {
		w.lastWrite.Store(time.Now().UnixNano())
		w.bytesOut.Add(uint64(n))
	}
}

// writeOne is one publisher write with a deadline. Overrunning it closes
// the publisher, which unblocks the write; the supervisor replaces it.
func (w *writeTracker) writeOne(pub publisher, gen uint64, p []byte) (int, error) {
	w.inflightGen.Store(gen)
	w.inflight.Store(time.Now().UnixNano())
	timer := time.AfterFunc(w.writeTimeout, func() {
		w.stuck.Add(1)
		_ = pub.Close()
	})
	n, err := pub.Write(p)
	timer.Stop()
	w.inflight.Store(0)
	return n, err
}

// inflightAge is how long the current publisher write has been running,
// or 0 when none is.
func (w *writeTracker) inflightAge(now time.Time) time.Duration {
	if ns := w.inflight.Load(); ns != 0 {
		return now.Sub(time.Unix(0, ns))
	}
	return 0
}

// stuckOnCurrent reports a write into the current publisher running longer
// than d. A write still stuck in an already replaced publisher does not
// count, so one blocked write cannot trigger a replacement every tick.
func (w *writeTracker) stuckOnCurrent(now time.Time, d time.Duration) bool {
	if d <= 0 || w.inflightAge(now) <= d {
		return false
	}
	w.pubMu.Lock()
	gen := w.pubGen
	w.pubMu.Unlock()
	return w.inflightGen.Load() == gen
}

// keepParams remembers the stream's latest SPS and PPS, so a new publisher
// can be primed with them. Caller holds mu.
func (w *writeTracker) keepParams(p []byte) {
	var sps, pps []byte
	for _, nal := range annexBUnits(p) {
		switch nalType(nal) {
		case 7:
			sps = nal
		case 8:
			pps = nal
		}
	}
	if sps != nil && pps != nil {
		w.params = [][]byte{append([]byte(nil), sps...), append([]byte(nil), pps...)}
	}
}

func nalType(nal []byte) byte {
	return nal[len(nal)-len(bytes.TrimLeft(nal, "\x00"))+1] & 0x1f
}

// hasKeyframe reports an SPS or IDR slice in an Annex B chunk: a point a
// decoder can resume from.
func hasKeyframe(p []byte) bool {
	for _, nal := range annexBUnits(p) {
		if t := nalType(nal); t == 5 || t == 7 {
			return true
		}
	}
	return false
}

// annexBUnits splits an Annex B buffer into NAL units, each with its start
// code. Units cut off by the end of the buffer come back as they are.
func annexBUnits(p []byte) [][]byte {
	var starts []int
	for i := 0; i+3 <= len(p); i++ {
		if p[i] == 0 && p[i+1] == 0 && p[i+2] == 1 {
			s := i
			if s > 0 && p[s-1] == 0 {
				s--
			}
			starts = append(starts, s)
			i += 2
		}
	}
	units := make([][]byte, 0, len(starts))
	for k, s := range starts {
		end := len(p)
		if k+1 < len(starts) {
			end = starts[k+1]
		}
		if head := len(p[s:end]) - len(bytes.TrimLeft(p[s:end], "\x00")); end-s > head+1 {
			units = append(units, p[s:end])
		}
	}
	return units
}

// swap puts a new publisher in place of a dead one and returns the old one
// for the caller to close. The writer primes the new one with the last SPS
// and PPS before its next chunk. Never blocks on a write.
func (w *writeTracker) swap(fresh publisher) publisher {
	w.pubMu.Lock()
	defer w.pubMu.Unlock()
	old := w.pub
	w.pub = fresh
	w.pubGen++
	return old
}

func (w *writeTracker) current() publisher {
	w.pubMu.Lock()
	defer w.pubMu.Unlock()
	return w.pub
}

func unixNano(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}

func (w *writeTracker) lastWriteTime() time.Time   { return unixNano(w.lastWrite.Load()) }
func (w *writeTracker) lastIngressTime() time.Time { return unixNano(w.lastIngress.Load()) }

// deadmanBaseline is the last camera video, or the session start before any.
func (w *writeTracker) deadmanBaseline() time.Time {
	if t := w.lastIngressTime(); !t.IsZero() {
		return t
	}
	return unixNano(w.sessionStart.Load())
}

// waitIdle waits until every queued chunk has been handled. For tests and
// orderly shutdown; false on timeout.
func (w *writeTracker) waitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if w.handled.Load() == w.enqueued.Load() && w.inflight.Load() == 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// Close stops the writer goroutine and releases the dump file. It does not
// close the publisher (the caller owns it). Safe to call repeatedly.
func (w *writeTracker) Close() error {
	w.closeOnce.Do(func() { close(w.stop) })
	select {
	case <-w.done:
	case <-time.After(w.writeTimeout + time.Second):
		// The in-flight write's deadline closes its publisher; don't wait.
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dump != nil {
		err := w.dump.Close()
		w.dump = nil
		return err
	}
	return nil
}
