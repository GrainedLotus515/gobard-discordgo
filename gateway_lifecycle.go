package discordgo

import (
	"context"
	"errors"
)

// ErrSessionClosed means the terminal Shutdown barrier has begun. Use a new
// Session for subsequent connections; Close alone remains reconnectable.
var ErrSessionClosed = errors.New("discord session shut down")

func (s *Session) beginGatewayWork() (context.Context, bool) {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	if s.gatewayClosed {
		return nil, false
	}
	if s.gatewayContext == nil {
		s.gatewayContext, s.gatewayCancel = context.WithCancel(context.Background())
	}
	s.gatewayWorkers.Add(1)
	return s.gatewayContext, true
}

func (s *Session) gatewayGo(work func()) {
	if _, ok := s.beginGatewayWork(); ok {
		go func() { defer s.gatewayWorkers.Done(); work() }()
	}
}

// Shutdown permanently closes Gateway admission, cancels opening/reconnect,
// closes the socket and joins Gateway workers and dispatched event callbacks.
// Call it from the owner, never from an event callback. Callbacks must return
// cooperatively; caller-created goroutines and legacy VoiceConnections are not
// owned here. GoBard uses a separate voice library. Repeated calls join the same
// shutdown. Close is retained as a nonjoining disconnect usable by workers.
func (s *Session) Shutdown() error {
	s.shutdownOnce.Do(func() {
		s.gatewayMu.Lock()
		s.gatewayClosed = true
		if s.gatewayCancel != nil {
			s.gatewayCancel()
		}
		s.gatewayMu.Unlock()
		s.shutdownError = s.Close()
		s.gatewayWorkers.Wait()
	})
	return s.shutdownError
}
