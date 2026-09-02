package oracleconnector

import (
	"context"
	"errors"
	"fmt"
)

// ttcMessage is the minimal QMigration-owned message boundary layered on top
// of Oracle Net/TNS DATA. Oracle TTC message payloads begin with a message code;
// keeping framing separate from authentication lets negotiation/auth/query
// codecs evolve without coupling them to TCP/TCPS or listener redirect logic.
type ttcMessage struct {
	Code    byte
	Payload []byte
}

type ttcStream struct {
	session *tnsDataSession
}

func newTTCStream(s *tnsDataSession) (*ttcStream, error) {
	if s == nil || s.conn == nil {
		return nil, errors.New("Oracle TTC stream requires an accepted TNS DATA session")
	}
	return &ttcStream{session: s}, nil
}

func (s *ttcStream) WriteMessage(ctx context.Context, m ttcMessage) error {
	if s == nil || s.session == nil {
		return errors.New("Oracle TTC stream is closed")
	}
	if m.Code == 0 {
		return errors.New("Oracle TTC message code 0 is invalid")
	}
	body := make([]byte, 1+len(m.Payload))
	body[0] = m.Code
	copy(body[1:], m.Payload)
	return s.session.WriteData(ctx, 0, body)
}

func (s *ttcStream) ReadMessage(ctx context.Context) (ttcMessage, error) {
	if s == nil || s.session == nil {
		return ttcMessage{}, errors.New("Oracle TTC stream is closed")
	}
	flags, body, err := s.session.ReadData(ctx)
	if err != nil {
		return ttcMessage{}, err
	}
	if flags != 0 {
		return ttcMessage{}, fmt.Errorf("Oracle TTC DATA flags 0x%x are not supported during negotiation", flags)
	}
	if len(body) == 0 || body[0] == 0 {
		return ttcMessage{}, errors.New("truncated Oracle TTC message")
	}
	return ttcMessage{Code: body[0], Payload: append([]byte(nil), body[1:]...)}, nil
}

// ttcPhase makes the Oracle native session state explicit. Experimental query
// codecs still stop short of production metadata/full-read capability; the state
// machine prevents later dictionary/full-load code from bypassing validated
// transport, negotiation and authentication phases.
type ttcPhase uint8

const (
	ttcPhaseTransport ttcPhase = iota + 1
	ttcPhaseProtocol
	ttcPhaseDataType
	ttcPhaseAuthenticated
	ttcPhaseReady
)

type ttcState struct{ phase ttcPhase }

func newTTCState() *ttcState { return &ttcState{phase: ttcPhaseTransport} }
func (s *ttcState) Advance(next ttcPhase) error {
	if s == nil {
		return errors.New("nil Oracle TTC state")
	}
	if next != s.phase+1 {
		return fmt.Errorf("invalid Oracle TTC phase transition %d -> %d", s.phase, next)
	}
	s.phase = next
	return nil
}
func (s *ttcState) Ready() bool { return s != nil && s.phase == ttcPhaseReady }
