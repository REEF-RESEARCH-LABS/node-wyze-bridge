package main

import (
	"bytes"
	"testing"
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
