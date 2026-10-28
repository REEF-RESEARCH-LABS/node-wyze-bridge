package gwell

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

const (
	testConvData = 0x1234ABCD & 0x7FFFFFFF
	testConvCtrl = 0x1234ABCD | 0x80000000
)

// captureKCP builds a camera-side KCP sender whose output segments are
// captured one flush at a time.
func captureKCP(conv uint32) (*KCPConn, *[][]byte) {
	var out [][]byte
	k := NewKCPConn(conv, func(data []byte, size int) {
		out = append(out, append([]byte(nil), data[:size]...))
	})
	k.NoDelay(1, 10, 2, 1)
	k.Update(1)
	return k, &out
}

func ackUna(t *testing.T, ack []byte) (conv, sn, una uint32) {
	t.Helper()
	if len(ack) != 24 || ack[4] != 82 {
		t.Fatalf("not a KCP ACK: %x", ack)
	}
	return binary.LittleEndian.Uint32(ack[0:4]), binary.LittleEndian.Uint32(ack[12:16]), binary.LittleEndian.Uint32(ack[16:20])
}

// REE-1054 comparison §6, candidate ACK-sequence defect. Control and data
// sequences diverge (data far ahead), and one control segment is lost.
// The supplementary ACK for the next control segment must carry the
// control conversation's una. The pinned rule (data una for every ACK)
// tells the camera the lost control segment arrived, the camera drops it
// from its send buffer, and it is never retransmitted.
func TestDirectAckUsesTheControlConversationsUnaWhenSequencesDiverge(t *testing.T) {
	cam, out := captureKCP(testConvCtrl)
	for _, msg := range [][]byte{[]byte("ctrl-0"), []byte("ctrl-1 (lost)"), []byte("ctrl-2")} {
		cam.Send(msg)
		cam.Flush()
	}
	if len(*out) != 3 {
		t.Fatalf("expected one output per control segment, got %d", len(*out))
	}

	viewerCtrl := NewKCPConn(testConvCtrl, func([]byte, int) {})
	viewerData := NewKCPConn(testConvData, func([]byte, int) {})
	// Data conversation is far ahead: 900 segments already delivered.
	for sn := uint32(0); sn < 900; sn++ {
		viewerData.Input(BuildKCPPushSegment(testConvData, sn, 1, []byte{0}))
		viewerData.Recv()
	}
	viewerCtrl.Input((*out)[0])
	viewerCtrl.Input((*out)[2]) // segment 1 lost
	if got := viewerCtrl.RcvNxt(); got != 1 {
		t.Fatalf("viewer ctrl rcvNxt=%d, want 1", got)
	}
	if got := viewerData.RcvNxt(); got != 900 {
		t.Fatalf("viewer data rcvNxt=%d, want 900", got)
	}

	// The pinned rule, reproduced: data una on a control ACK.
	_, _, legacyUna := ackUna(t, BuildKCPAckSegment(testConvCtrl, 2, 1, viewerData.RcvNxt()))
	if legacyUna != 900 {
		t.Fatalf("legacy una=%d", legacyUna)
	}
	legacyCam, legacyOut := captureKCP(testConvCtrl)
	for _, msg := range [][]byte{[]byte("ctrl-0"), []byte("ctrl-1 (lost)"), []byte("ctrl-2")} {
		legacyCam.Send(msg)
		legacyCam.Flush()
	}
	_ = legacyOut
	legacyCam.Input(BuildKCPAckSegment(testConvCtrl, 2, 1, legacyUna))
	if st := legacyCam.Stats(); st.SndBufLen != 0 {
		t.Fatalf("expected the legacy ACK to clear the camera's send buffer (the defect), got %d left", st.SndBufLen)
	}

	// The fix: the ACK built for the control segment carries ctrl una.
	acks := DirectAcks((*out)[2], testConvData, testConvCtrl, viewerData.RcvNxt(), viewerCtrl.RcvNxt())
	if len(acks) != 1 {
		t.Fatalf("got %d ACKs, want 1", len(acks))
	}
	conv, sn, una := ackUna(t, acks[0])
	if conv != testConvCtrl || sn != 2 || una != 1 {
		t.Fatalf("ACK conv=%08X sn=%d una=%d, want ctrl sn=2 una=1", conv, sn, una)
	}
	cam.Input(acks[0])
	st := cam.Stats()
	if st.SndBufLen != 1 || st.SndUna != 1 {
		t.Fatalf("camera must keep the lost segment for retransmission: sndBuf=%d sndUna=%d", st.SndBufLen, st.SndUna)
	}
}

func TestDirectAcksKeepDataUnaForDataAndHandleMixedFrames(t *testing.T) {
	frame := append(BuildKCPPushSegment(testConvData, 41, 7, []byte("video")),
		BuildKCPPushSegment(testConvCtrl, 3, 8, []byte("ctl"))...)
	frame = append(frame, BuildKCPAckSegment(testConvData, 9, 9, 9)...) // ACKs are not acknowledged
	acks := DirectAcks(frame, testConvData, testConvCtrl, 42, 4)
	if len(acks) != 2 {
		t.Fatalf("got %d ACKs, want 2", len(acks))
	}
	if c, sn, una := ackUna(t, acks[0]); c != testConvData || sn != 41 || una != 42 {
		t.Fatalf("data ACK conv=%08X sn=%d una=%d", c, sn, una)
	}
	if c, sn, una := ackUna(t, acks[1]); c != testConvCtrl || sn != 3 || una != 4 {
		t.Fatalf("ctrl ACK conv=%08X sn=%d una=%d", c, sn, una)
	}
}

func streamingSession() *Session {
	s := NewSession(SessionConfig{DeviceName: "GW_WC_TEST"})
	s.state = StateStreaming
	return s
}

// REJECT/STOP/CLOSE after established media end the generation once video
// stops; they are never just logged with the session left streaming.
func TestTerminalControlEndsTheGenerationWhenVideoStops(t *testing.T) {
	for _, action := range []uint32{AVActionREJECT, AVActionSTOP, AVActionCLOSE} {
		s := streamingSession()
		s.firstVideo = true
		s.streamDataBytes = 1000
		s.noteControl(action, 7)
		if s.Stage() != StageTerminating {
			t.Fatalf("%s: stage %q, want terminating", avActionName(action), s.Stage())
		}
		if err := s.checkTerminal(s.terminalAt.Add(terminalConfirmWindow / 2)); err != nil {
			t.Fatalf("%s: ended before the confirm window: %v", avActionName(action), err)
		}
		err := s.checkTerminal(s.terminalAt.Add(terminalConfirmWindow))
		var tc *TerminalControlError
		if !errors.As(err, &tc) || tc.Action != action || tc.Reason != 7 {
			t.Fatalf("%s: got %v, want TerminalControlError", avActionName(action), err)
		}
		if s.LastControl() == "" {
			t.Fatalf("%s: last control not recorded", avActionName(action))
		}
	}
}

func TestTerminalControlIgnoredWhileVideoKeepsArriving(t *testing.T) {
	s := streamingSession()
	s.firstVideo = true
	s.streamDataBytes = 1000
	s.noteControl(AVActionREJECT, 2)
	s.streamDataBytes = 5000 // video kept coming
	if err := s.checkTerminal(s.terminalAt.Add(terminalConfirmWindow)); err != nil {
		t.Fatalf("ended a stream that is still delivering video: %v", err)
	}
	if s.terminal != nil || s.Stage() != StageForwarding {
		t.Fatalf("terminal not cleared (stage %q)", s.Stage())
	}
}

// Replies queued during the INITREQ burst drain right after START, before
// any video: they are not terminal for the stream that follows.
func TestControlBeforeFirstVideoIsNotTerminal(t *testing.T) {
	s := streamingSession()
	s.noteControl(AVActionREJECT, 1)
	s.noteControl(AVActionACCEPT, 0)
	if s.terminal != nil || s.LateAccepts() != 0 {
		t.Fatal("pre-video control treated as established-stream control")
	}
	s.firstVideo = true
	s.noteControl(AVActionACCEPT, 0)
	if s.LateAccepts() != 1 || s.terminal != nil {
		t.Fatalf("late ACCEPT: accepts=%d terminal=%v", s.LateAccepts(), s.terminal)
	}
}

func lanAddr(port int) *net.UDPAddr { return &net.UDPAddr{IP: net.IPv4(10, 10, 10, 50), Port: port} }

// Unanswered LAN candidates must not suppress the relay; the trial is
// bounded; return traffic locks the LAN route; silence unlocks it.
func TestRouteTrialNeedsReturnTrafficAndLetsTheRelayIn(t *testing.T) {
	s := NewSession(SessionConfig{DeviceName: "GW_WC_TEST"})
	s.lanMTPAddrs = []*net.UDPAddr{lanAddr(6789), lanAddr(32761)} // candidates from config, never heard
	s.udpRelayTargets = []UDPRelayTarget{{Addr: &net.UDPAddr{IP: net.IPv4(3, 13, 212, 24), Port: 28800}}}
	start := time.Now()
	s.lanTrialStart = start

	lan, relay := s.routeTargets()
	if len(lan) != 2 || !relay || s.Route() != "trial" {
		t.Fatalf("unanswered LAN: lan=%d relay=%v route=%s; want both paths fed", len(lan), relay, s.Route())
	}

	s.checkRoute(start.Add(lanTrialTimeout / 2))
	if s.Route() != "trial" {
		t.Fatal("trial ended early")
	}
	s.checkRoute(start.Add(lanTrialTimeout + time.Second))
	if lan, relay := s.routeTargets(); len(lan) != 0 || !relay || s.Route() != "relay" {
		t.Fatalf("after trial: lan=%d relay=%v route=%s; want relay only", len(lan), relay, s.Route())
	}

	// The camera answers on LAN: lock it, stop feeding the relay.
	s.noteLanReturn(lanAddr(32761))
	if lan, relay := s.routeTargets(); len(lan) != 1 || relay || lan[0].Port != 32761 || s.Route() != "lan" {
		t.Fatalf("after return traffic: lan=%v relay=%v route=%s", lan, relay, s.Route())
	}

	// The learned route goes silent: unlock so the relay is tried again.
	s.checkRoute(s.lastLanRecv.Add(routeSilenceTimeout + time.Second))
	if lan, relay := s.routeTargets(); s.Route() == "lan" || !relay || len(lan) == 0 {
		t.Fatalf("silent LAN route stayed sticky: lan=%d relay=%v route=%s", len(lan), relay, s.Route())
	}
}

func TestRouteWithoutRelayKeepsLanCandidates(t *testing.T) {
	s := NewSession(SessionConfig{DeviceName: "GW_WC_TEST"})
	s.lanMTPAddrs = []*net.UDPAddr{lanAddr(6789)}
	s.lanTrialStart = time.Now()
	s.checkRoute(time.Now().Add(time.Hour))
	if lan, _ := s.routeTargets(); len(lan) != 1 {
		t.Fatal("abandoned the only path")
	}
}

func TestInitReqBytesUnchangedByDivergenceConstants(t *testing.T) {
	key := make([]byte, 32)
	b := BuildAVStreamCtlINITREQ(0xCAFEBABE, 1, 1, 0, key)
	if len(b) != 76 {
		t.Fatalf("len %d", len(b))
	}
	for _, off := range []int{0x40, 0x41, 0x44, 0x45, 0x46, 0x47} {
		if b[off] != 0 {
			t.Fatalf("byte 0x%X = %d; wire constants must stay unchanged", off, b[off])
		}
	}
}
