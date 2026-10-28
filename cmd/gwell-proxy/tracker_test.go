package main

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakePub struct {
	mu     sync.Mutex
	writes [][]byte
	alive  bool
	closed bool
}

func (f *fakePub) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (f *fakePub) Alive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive
}
func (f *fakePub) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed, f.alive = true, false
	return nil
}
func (f *fakePub) got() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.writes...)
}

var (
	sps = []byte{0, 0, 0, 1, 0x67, 0x64, 0x00, 0x29}
	pps = []byte{0, 0, 0, 1, 0x68, 0xee, 0x3c, 0x80}
	idr = []byte{0, 0, 1, 0x65, 0x88, 0x84}
	sli = []byte{0, 0, 1, 0x41, 0x9a, 0x02}
)

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func idle(t *testing.T, w *writeTracker) {
	t.Helper()
	if !w.waitIdle(2 * time.Second) {
		t.Fatal("writer did not drain")
	}
}

// The go2rtc a camera publishes into restarted: the new publisher must get
// the stream's SPS and PPS before anything else, or ffmpeg cannot decode
// a frame until the camera happens to resend them.
func TestSwapPrimesTheNewPublisherWithTheLastSPSAndPPS(t *testing.T) {
	first := &fakePub{alive: true}
	w := newWriteTracker(first)
	defer w.Close()
	if _, err := w.Write(cat(sps, pps, idr)); err != nil {
		t.Fatal(err)
	}
	w.Write(sli)
	idle(t, w)
	first.Close()
	fresh := &fakePub{alive: true}
	old := w.swap(fresh)
	if old != first || w.current() != fresh {
		t.Fatal("swap did not replace the publisher")
	}
	w.Write(sli)
	idle(t, w)
	got := fresh.got()
	if len(got) != 3 || !bytes.Equal(got[0], sps) || !bytes.Equal(got[1], pps) || !bytes.Equal(got[2], sli) {
		t.Fatalf("new publisher got %x", got)
	}
	if len(first.got()) != 2 {
		t.Fatalf("old publisher kept receiving: %d writes", len(first.got()))
	}
}

func TestParamsFollowTheStreamAndIgnoreHalfSets(t *testing.T) {
	w := newWriteTracker(&fakePub{alive: true})
	defer w.Close()
	w.Write(cat(sps, idr)) // an SPS alone does not replace a full set
	if w.params != nil {
		t.Fatal("kept an SPS without a PPS")
	}
	newSPS := []byte{0, 0, 1, 0x67, 0x4d, 0x00, 0x1f}
	w.Write(cat(sps, pps, idr))
	w.Write(cat(newSPS, pps, idr))
	w.mu.Lock()
	defer w.mu.Unlock()
	if !bytes.Equal(w.params[0], newSPS) || !bytes.Equal(w.params[1], pps) {
		t.Fatalf("params %x", w.params)
	}
}

func TestAnnexBUnitsSplitsOnBothStartCodes(t *testing.T) {
	units := annexBUnits(cat(sps, pps, idr, []byte{0, 0, 1}))
	if len(units) != 3 || !bytes.Equal(units[0], sps) || !bytes.Equal(units[2], idr) {
		t.Fatalf("units %x", units)
	}
}

// stallPub is a publisher that stops reading: every Write blocks until the
// publisher is closed, like ffmpeg's stdin when go2rtc stops consuming.
type stallPub struct {
	once    sync.Once
	closed  chan struct{}
	entered chan struct{}
}

func newStallPub() *stallPub {
	return &stallPub{closed: make(chan struct{}), entered: make(chan struct{}, 1)}
}
func (s *stallPub) Write(p []byte) (int, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.closed
	return 0, errors.New("publisher closed")
}
func (s *stallPub) Alive() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}
func (s *stallPub) Close() error { s.once.Do(func() { close(s.closed) }); return nil }

type fakeSession struct {
	mu     sync.Mutex
	closed bool
}

func (f *fakeSession) Close() error { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }
func (f *fakeSession) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// REE-1054 P1, blocked-writer proof. The publisher stops reading while the
// camera keeps sending. The session loop (which in gwell.Session also runs
// heartbeats, meter probes and INITREQ renewals between Writes) and the
// supervisor must keep their cadence, the stuck publisher must be replaced
// with a fresh one primed with SPS/PPS, video must reach the new publisher,
// and stop must be honoured promptly.
func TestBlockedPublisherNeverStallsControlOrSupervisor(t *testing.T) {
	const (
		loopEvery    = 5 * time.Millisecond  // session loop iteration (heartbeat scheduler)
		tick         = 20 * time.Millisecond // supervisor tick
		writeTimeout = 100 * time.Millisecond
		runFor       = 800 * time.Millisecond
		maxLoopGap   = 50 * time.Millisecond
		maxTickGap   = 100 * time.Millisecond
	)
	stalled := newStallPub()
	w := newWriteTrackerWith(stalled, 16, writeTimeout)
	defer w.Close()

	var freshMu sync.Mutex
	var fresh []*fakePub
	newPub := func() (publisher, error) {
		p := &fakePub{alive: true}
		freshMu.Lock()
		fresh = append(fresh, p)
		freshMu.Unlock()
		return p, nil
	}

	// Session loop: one Write per iteration, timing every iteration.
	stopLoop := make(chan struct{})
	loopDone := make(chan time.Duration)
	go func() {
		var maxGap time.Duration
		last := time.Now()
		for i := 0; ; i++ {
			select {
			case <-stopLoop:
				loopDone <- maxGap
				return
			case <-time.After(loopEvery):
			}
			chunk := sli
			if i%10 == 0 {
				chunk = cat(sps, pps, idr)
			}
			w.Write(chunk)
			now := time.Now()
			if gap := now.Sub(last); gap > maxGap {
				maxGap = gap
			}
			last = now
		}
	}()
	select {
	case <-stalled.entered:
	case <-time.After(time.Second):
		t.Fatal("writer never reached the publisher")
	}

	var tickMu sync.Mutex
	var ticks []time.Time
	sess := &fakeSession{}
	errCh := make(chan error)
	stopCh := make(chan struct{})
	supDone := make(chan error, 1)
	restarts := 0
	go func() {
		supDone <- supervise(sess, errCh, stopCh, w, superviseConfig{
			camera:       "GW_WC_TEST",
			tick:         tick,
			deadman:      10 * time.Second,
			stuckAfter:   2 * writeTimeout,
			newPublisher: newPub,
			onTick: func() {
				tickMu.Lock()
				ticks = append(ticks, time.Now())
				tickMu.Unlock()
			},
			onRestart: func() { restarts++ },
		})
	}()

	time.Sleep(runFor)

	// Stop is honoured promptly while the camera is still sending.
	stopAt := time.Now()
	close(stopCh)
	select {
	case err := <-supDone:
		if err != nil {
			t.Fatalf("supervise on stop: %v", err)
		}
	case <-time.After(5 * tick):
		t.Fatal("stop not honoured: supervisor blocked")
	}
	stopLatency := time.Since(stopAt)
	close(stopLoop)
	loopGap := <-loopDone

	if !sess.isClosed() {
		t.Fatal("stop did not close the session")
	}
	if loopGap > maxLoopGap {
		t.Fatalf("session loop stalled %s behind a blocked publisher (limit %s)", loopGap, maxLoopGap)
	}
	tickMu.Lock()
	var tickGap time.Duration
	for i := 1; i < len(ticks); i++ {
		if g := ticks[i].Sub(ticks[i-1]); g > tickGap {
			tickGap = g
		}
	}
	nTicks := len(ticks)
	tickMu.Unlock()
	if nTicks < int(runFor/tick)/2 || tickGap > maxTickGap {
		t.Fatalf("supervisor ticks=%d max gap=%s: monitor was blocked", nTicks, tickGap)
	}
	if stalled.Alive() {
		t.Fatal("stuck publisher was not closed by the write deadline")
	}
	if w.stuck.Load() == 0 {
		t.Fatal("stuck write not counted")
	}
	freshMu.Lock()
	defer freshMu.Unlock()
	if len(fresh) != 1 || restarts != 1 {
		t.Fatalf("publisher replacements=%d restarts=%d, want exactly 1", len(fresh), restarts)
	}
	got := fresh[0].got()
	if len(got) < 3 || !bytes.Equal(got[0], sps) || !bytes.Equal(got[1], pps) {
		t.Fatalf("replacement publisher not primed / fed: %d writes", len(got))
	}
	t.Logf("loop max gap %s, supervisor %d ticks max gap %s, stop latency %s, dropped %d chunks, replacement got %d writes",
		loopGap, nTicks, tickGap, stopLatency, w.dropped.Load(), len(got))
}

// A publisher that never returns and is never closed in time still cannot
// hold the session: overflow drops video, and resumes on a keyframe.
func TestQueueOverflowDropsToTheNextKeyframe(t *testing.T) {
	stalled := newStallPub()
	w := newWriteTrackerWith(stalled, 4, time.Hour)
	defer func() { stalled.Close(); w.Close() }()

	w.Write(cat(sps, pps, idr)) // taken by the writer, which blocks in Write
	<-stalled.entered
	start := time.Now()
	for i := 0; i < 100; i++ {
		w.Write(sli)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("Write blocked for %s with a stalled publisher", d)
	}
	if w.dropped.Load() == 0 || !w.dropping {
		t.Fatal("overflow did not drop")
	}
	w.Write(sli)
	if !w.dropping {
		t.Fatal("resumed on a non-keyframe")
	}
	// Drain the queue by swapping in a reader, then send a keyframe.
	fresh := &fakePub{alive: true}
	w.swap(fresh).Close()
	idle(t, w)
	w.Write(cat(idr))
	idle(t, w)
	if w.dropping {
		t.Fatal("keyframe did not end the drop")
	}
	got := fresh.got()
	if last := got[len(got)-1]; !bytes.Equal(last, idr) {
		t.Fatalf("last delivered %x, want the keyframe", last)
	}
}

// The deadman follows what the camera delivers, not what the publisher
// accepted: a blocked publisher does not end a healthy camera session.
func TestDeadmanUsesCameraArrivalNotPublisherWrites(t *testing.T) {
	stalled := newStallPub()
	w := newWriteTrackerWith(stalled, 4, time.Hour)
	defer func() { stalled.Close(); w.Close() }()
	w.sessionStart.Store(time.Now().Add(-time.Hour).UnixNano())
	w.Write(sli)
	if since := time.Since(w.deadmanBaseline()); since > time.Second {
		t.Fatalf("deadman baseline %s old after fresh camera video", since)
	}
	if !w.lastWriteTime().IsZero() {
		t.Fatal("publisher write recorded although the publisher accepted nothing")
	}
}
