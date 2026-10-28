package main

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakePub struct {
	writes [][]byte
	alive  bool
	closed bool
}

func (f *fakePub) Write(p []byte) (int, error) {
	f.writes = append(f.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (f *fakePub) Alive() bool  { return f.alive }
func (f *fakePub) Close() error { f.closed = true; return nil }

var (
	sps = []byte{0, 0, 0, 1, 0x67, 0x64, 0x00, 0x29}
	pps = []byte{0, 0, 0, 1, 0x68, 0xee, 0x3c, 0x80}
	idr = []byte{0, 0, 1, 0x65, 0x88, 0x84}
	sli = []byte{0, 0, 1, 0x41, 0x9a, 0x02}
)

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// The go2rtc a camera publishes into restarted: the new publisher must get
// the stream's SPS and PPS before anything else, or ffmpeg cannot decode
// a frame until the camera happens to resend them.
func TestSwapPrimesTheNewPublisherWithTheLastSPSAndPPS(t *testing.T) {
	first := &fakePub{alive: true}
	w := &writeTracker{pub: first}
	if _, err := w.Write(cat(sps, pps, idr)); err != nil {
		t.Fatal(err)
	}
	w.Write(sli)
	first.alive = false
	fresh := &fakePub{alive: true}
	old := w.swap(fresh)
	if old != first || w.current() != fresh {
		t.Fatal("swap did not replace the publisher")
	}
	w.Write(sli)
	if len(fresh.writes) != 3 || !bytes.Equal(fresh.writes[0], sps) || !bytes.Equal(fresh.writes[1], pps) || !bytes.Equal(fresh.writes[2], sli) {
		t.Fatalf("new publisher got %x", fresh.writes)
	}
	if len(first.writes) != 2 {
		t.Fatalf("old publisher kept receiving: %d writes", len(first.writes))
	}
}

func TestParamsFollowTheStreamAndIgnoreHalfSets(t *testing.T) {
	w := &writeTracker{pub: &fakePub{alive: true}}
	w.Write(cat(sps, idr)) // an SPS alone does not replace a full set
	if w.params != nil {
		t.Fatal("kept an SPS without a PPS")
	}
	newSPS := []byte{0, 0, 1, 0x67, 0x4d, 0x00, 0x1f}
	w.Write(cat(sps, pps, idr))
	w.Write(cat(newSPS, pps, idr))
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

// A publisher that stops reading (REE-1054): its Write blocks until it is
// closed, as a pipe into a stuck ffmpeg does.
type blockedPub struct {
	gate   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

func newBlockedPub() *blockedPub { return &blockedPub{gate: make(chan struct{})} }
func (b *blockedPub) Write(p []byte) (int, error) {
	<-b.gate
	return 0, errors.New("closed")
}
func (b *blockedPub) Alive() bool { return !b.closed.Load() }
func (b *blockedPub) Close() error {
	b.once.Do(func() { b.closed.Store(true); close(b.gate) })
	return nil
}

type safePub struct {
	mu     sync.Mutex
	writes [][]byte
}

func (f *safePub) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (f *safePub) Alive() bool  { return true }
func (f *safePub) Close() error { return nil }
func (f *safePub) all() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.writes...)
}

func within(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// The camera session's loop writes frames and sends its heartbeats; the
// monitor asks for the publisher. Neither may wait on a publisher that
// stopped reading.
func TestABlockedPublisherNeverHoldsUpTheSessionOrTheMonitor(t *testing.T) {
	stuck := newBlockedPub()
	w := newWriteTracker(stuck)
	defer w.Close()
	within(t, 2*time.Second, "the session's writes", func() {
		w.Write(cat(sps, pps, idr))
		for i := 0; i < 3*publishQueue; i++ {
			w.Write(sli)
		}
	})
	within(t, time.Second, "the monitor's current()", func() { _ = w.current() })
	if w.dropped.Load() == 0 {
		t.Fatal("a full queue dropped nothing")
	}
	if w.lastInTime().IsZero() {
		t.Fatal("camera input not timed")
	}
	// The write in progress is seen as stuck once it has lasted publishStuck.
	w.writing.Store(time.Now().Add(-publishStuck - time.Second).UnixNano())
	if !w.stuck() {
		t.Fatal("a write held past publishStuck is not stuck")
	}
	// Replacing the publisher ends the stuck write; the new one starts at a keyframe.
	fresh := &safePub{}
	within(t, time.Second, "swap", func() { w.swap(fresh).Close() })
	// What was queued for the stuck one is stale: wait for it to drain.
	deadline := time.Now().Add(2 * time.Second)
	for len(w.queue) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	before := len(fresh.all())
	w.Write(sli)
	w.Write(cat(idr))
	w.Write(sli)
	for len(fresh.all()) < before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got := fresh.all()
	if len(got) < 2 || !bytes.Equal(got[0], sps) || !bytes.Equal(got[1], pps) {
		t.Fatalf("new publisher was not primed: %x", got)
	}
	tail := got[before:]
	if len(tail) != 2 || !bytes.Equal(tail[0], idr) || !bytes.Equal(tail[1], sli) {
		t.Fatalf("new publisher did not start at the keyframe: %x", tail)
	}
}

// A cached server address ages from its real discovery, never from the
// token refresh that reused it (REE-1054).
func TestACachedAddressAgesFromItsDiscovery(t *testing.T) {
	now := time.Now().Unix()
	fresh := tokenCache{AccessID: "a", AccessToken: "t", ServerAddr: "s", CachedAt: now, DiscoveredAt: now - 3600}
	if !fresh.isValid() {
		t.Fatal("an hour-old discovery is still good")
	}
	old := fresh
	old.DiscoveredAt = now - 8*24*3600
	if old.isValid() {
		t.Fatal("an eight-day-old discovery kept alive by a fresh CachedAt")
	}
	before := fresh
	before.DiscoveredAt = 0
	if before.isValid() {
		t.Fatal("a cache from before has no known discovery time")
	}
}
