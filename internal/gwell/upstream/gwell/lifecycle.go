package gwell

// lifecycle.go — Node One additions (REE-1054, 4.6.1-node.17), written from
// the Window Cam recovery brief and native-session comparison prose only.
//
//   - Typed session stages, readable from another goroutine (Stage).
//   - Terminal media-control messages (REJECT/STOP/CLOSE) after streaming
//     starts end this session generation instead of being only logged.
//   - Route trial: LAN candidates are guesses until the camera answers from
//     one. Until then the relay paths are fed too, an unanswered LAN trial
//     is bounded, and a LAN route that goes silent is unlocked so the relay
//     can carry the session. Unicast only: no broadcast, multicast or mDNS.
//   - Supplementary direct KCP ACKs carry the una of the conversation they
//     acknowledge.

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
)

// Session stages, as reported by Session.Stage. The proxy adds its own
// stages around these (authorizing, waiting_retry, ...).
const (
	StageRegistering       = "registering"         // P2P server connect, certify, InitInfo
	StageSubscribing       = "subscribing"         // network detect, subscribe, CALLING
	StageNegotiating       = "negotiating"         // transport set up, INITREQ until ACCEPT
	StageWaitingFirstVideo = "waiting_first_video" // START sent, no H.264 yet
	StageForwarding        = "forwarding"          // H.264 reached the writer
	StageTerminating       = "terminating"         // terminal control message seen, confirming
	StageEnded             = "ended"
)

// AVSTREAMCTL actions seen on the control conversation.
const (
	AVActionINITREQ = 1
	AVActionACCEPT  = 2
	AVActionREJECT  = 3
	AVActionSTOP    = 4
	AVActionCLOSE   = 5
	AVActionSTART   = 6
)

func avActionName(a uint32) string {
	switch a {
	case AVActionINITREQ:
		return "INITREQ"
	case AVActionACCEPT:
		return "ACCEPT"
	case AVActionREJECT:
		return "REJECT"
	case AVActionSTOP:
		return "STOP"
	case AVActionCLOSE:
		return "CLOSE"
	case AVActionSTART:
		return "START"
	}
	return fmt.Sprintf("0x%X", a)
}

// Timing for the route trial and terminal-message handling. These are
// Node-side supervision bounds, not wire values.
const (
	// lanTrialTimeout bounds how long unanswered LAN candidates are tried
	// once streaming setup begins, when a relay path exists.
	lanTrialTimeout = 10 * time.Second
	// routeSilenceTimeout unlocks a learned LAN route that has carried
	// nothing back for this long (the camera answers meter probes every
	// few seconds on a live route).
	routeSilenceTimeout = 10 * time.Second
	// terminalConfirmWindow: after REJECT/STOP/CLOSE on an established
	// stream, the generation is torn down unless video keeps arriving for
	// this long. The bridge's periodic INITREQ (an unverified keepalive,
	// left unchanged) could draw a reply that is not meant for the live
	// stream; video that keeps flowing shows the stream is still alive.
	terminalConfirmWindow = 3 * time.Second
)

// TerminalControlError ends a session generation after the camera sent
// REJECT, STOP or CLOSE on an established stream and video stopped.
type TerminalControlError struct {
	Action uint32
	Reason uint32
}

func (e *TerminalControlError) Error() string {
	return fmt.Sprintf("camera sent %s (reason %d); session generation ended", avActionName(e.Action), e.Reason)
}

// Stage reports the session's current lifecycle stage. Safe from any goroutine.
func (s *Session) Stage() string {
	if v, ok := s.stage.Load().(string); ok {
		return v
	}
	return ""
}

func (s *Session) setStage(stage string) {
	if s.Stage() == stage {
		return
	}
	s.stage.Store(stage)
	log.Printf("%s stage: %s", s.prefix, stage)
}

// LastControl is the last AVSTREAMCTL action seen after streaming started,
// e.g. "REJECT reason=7", or "" when none.
func (s *Session) LastControl() string {
	if v, ok := s.lastControl.Load().(string); ok {
		return v
	}
	return ""
}

// LateAccepts counts ACCEPT messages seen after streaming started. They are
// recorded, not acted on: whether a later ACCEPT opens a new media
// generation that needs its own START is not established (comparison §2),
// and the bridge's periodic INITREQ may draw them.
func (s *Session) LateAccepts() int { return int(s.lateAccepts.Load()) }

// noteControl handles one AVSTREAMCTL message from the control conversation.
// Called from the stream goroutine only.
func (s *Session) noteControl(action, reason uint32) {
	// Only on established media: control replies queued during the
	// INITREQ burst (one per duplicate INITREQ) drain right after START
	// and say nothing about the live stream. Before first video the
	// first-data deadline in the proxy covers a refused setup.
	if s.state != StateStreaming || !s.firstVideo {
		return
	}
	s.lastControl.Store(fmt.Sprintf("%s reason=%d", avActionName(action), reason))
	switch action {
	case AVActionACCEPT:
		s.lateAccepts.Add(1)
	case AVActionREJECT, AVActionSTOP, AVActionCLOSE:
		if s.terminal == nil {
			s.terminal = &TerminalControlError{Action: action, Reason: reason}
			s.terminalAt = time.Now()
			s.terminalBytes = s.streamDataBytes
			s.setStage(StageTerminating)
			log.Printf("%s %s after streaming started: ending this generation unless video keeps arriving for %s",
				s.prefix, avActionName(action), terminalConfirmWindow)
		}
	}
}

// checkTerminal decides a pending terminal message once its confirm window
// has passed. Returns the error that ends the generation, or nil.
func (s *Session) checkTerminal(now time.Time) error {
	if s.terminal == nil || now.Sub(s.terminalAt) < terminalConfirmWindow {
		return nil
	}
	if s.streamDataBytes > s.terminalBytes {
		log.Printf("%s %s ignored: video kept arriving", s.prefix, avActionName(s.terminal.Action))
		s.terminal = nil
		if s.firstVideo {
			s.setStage(StageForwarding)
		} else {
			s.setStage(StageWaitingFirstVideo)
		}
		return nil
	}
	return s.terminal
}

// Route reports the media route: "lan" (locked on an address the camera
// answered from), "relay" (no LAN candidates, or the LAN trial ended), or
// "trial" (LAN candidates still unanswered, relay fed too).
func (s *Session) Route() string {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	switch {
	case s.bestLanAddr != nil:
		return "lan"
	case len(s.lanMTPAddrs) == 0 || s.lanAbandoned:
		return "relay"
	default:
		return "trial"
	}
}

// noteLanReturn records traffic that came back from the camera on a LAN
// address: it verifies the LAN route and locks it as the best address.
func (s *Session) noteLanReturn(addr *net.UDPAddr) {
	if addr == nil || !isCameraLanIP(addr.IP) {
		return
	}
	s.addLanMTPAddr(addr)
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	s.lanVerified = true
	s.lanAbandoned = false
	s.lastLanRecv = time.Now()
	if s.bestLanAddr == nil {
		s.bestLanAddr = &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port}
		log.Printf("%s locked bestLanAddr: %s", s.prefix, s.bestLanAddr)
	}
}

// hasRelayPath reports whether any relay path exists. Caller holds routeMu
// or runs before the KCP goroutine starts.
func (s *Session) hasRelayPath() bool {
	return len(s.udpRelayTargets) > 0 || s.tcpRelay != nil
}

// routeTargets returns the LAN addresses to send to and whether the relay
// paths should be fed as well.
func (s *Session) routeTargets() (lan []*net.UDPAddr, relay bool) {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	if s.bestLanAddr != nil {
		return []*net.UDPAddr{s.bestLanAddr}, false
	}
	if len(s.lanMTPAddrs) > 0 && !s.lanAbandoned {
		lan = append([]*net.UDPAddr(nil), s.lanMTPAddrs...)
		// Unanswered candidates must not suppress the relay.
		return lan, !s.lanVerified
	}
	return nil, true
}

// checkRoute bounds the LAN trial and unlocks a LAN route gone silent.
// Called from the stream goroutine.
func (s *Session) checkRoute(now time.Time) {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	if s.bestLanAddr != nil {
		if now.Sub(s.lastLanRecv) > routeSilenceTimeout {
			log.Printf("%s LAN route %s silent for %s: feeding the relay as well", s.prefix, s.bestLanAddr, routeSilenceTimeout)
			s.bestLanAddr = nil
			s.lanVerified = false
			s.lanTrialStart = now
		}
		return
	}
	if !s.lanVerified && !s.lanAbandoned && len(s.lanMTPAddrs) > 0 && s.hasRelayPath() &&
		!s.lanTrialStart.IsZero() && now.Sub(s.lanTrialStart) > lanTrialTimeout {
		s.lanAbandoned = true
		log.Printf("%s LAN candidates unanswered for %s: relay only", s.prefix, lanTrialTimeout)
	}
}

// DirectAcks builds the supplementary KCP ACK segments for every PUSH (cmd
// 81) in kcp, the KCP portion of one MTP frame. Each ACK carries the una
// (next expected sequence) of the conversation it acknowledges: the data
// conversation's for data, the control conversation's for control. The
// pinned bridge used the data conversation's una for both (REE-1054
// comparison §6), which tells the camera control segments it never
// delivered were received once the two sequences diverge.
func DirectAcks(kcp []byte, convData, convCtrl, dataRcvNxt, ctrlRcvNxt uint32) [][]byte {
	var acks [][]byte
	off := 0
	for off+24 <= len(kcp) {
		segLen := binary.LittleEndian.Uint32(kcp[off+20 : off+24])
		if kcp[off+4] == 81 {
			conv := binary.LittleEndian.Uint32(kcp[off : off+4])
			ts := binary.LittleEndian.Uint32(kcp[off+8 : off+12])
			sn := binary.LittleEndian.Uint32(kcp[off+12 : off+16])
			var una uint32
			if conv == convCtrl {
				una = ctrlRcvNxt
			} else {
				// Data, and (as before) any other conversation.
				una = dataRcvNxt
			}
			acks = append(acks, BuildKCPAckSegment(conv, sn, ts, una))
		}
		if segLen > 65536 {
			break
		}
		off += 24 + int(segLen)
	}
	return acks
}

// bestLan returns the locked LAN address, or nil. For logging.
func (s *Session) bestLan() *net.UDPAddr {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	return s.bestLanAddr
}
