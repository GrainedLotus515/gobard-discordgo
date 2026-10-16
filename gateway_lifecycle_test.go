package discordgo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestShutdownCancelsAndJoinsOpeningHandshake(t *testing.T) {
	accepted, serverDone := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer close(serverDone)
		close(accepted)
		_, _, _ = conn.ReadMessage() // Deliberately never send Hello.
	}))
	defer server.Close()
	s, err := New("Bot fixture")
	if err != nil {
		t.Fatal(err)
	}
	s.gateway = "ws" + strings.TrimPrefix(server.URL, "http")
	opened := make(chan error, 1)
	go func() { opened <- s.Open() }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("not connected")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Shutdown() }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not interrupt handshake")
	}
	select {
	case err := <-opened:
		if err == nil {
			t.Fatal("canceled opening succeeded")
		}
	default:
		t.Fatal("shutdown did not join Open")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("socket retained")
	}
	if err := s.Open(); !errors.Is(err, ErrSessionClosed) {
		t.Fatal(err)
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownWaitsForAcceptedEventAndRejectsNewWork(t *testing.T) {
	s, err := New("Bot fixture")
	if err != nil {
		t.Fatal(err)
	}
	started, finish, completed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.AddHandler(func(_ *Session, _ *MessageCreate) { close(started); <-finish; close(completed) })
	s.handleEvent(messageCreateEventType, &MessageCreate{Message: &Message{ID: "1"}})
	<-started
	closed := make(chan struct{})
	go func() { _ = s.Shutdown(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("shutdown returned with callback running")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("callback not joined")
	}
	select {
	case <-completed:
	default:
		t.Fatal("callback not finished")
	}
	if _, ok := s.beginGatewayWork(); ok {
		s.gatewayWorkers.Done()
		t.Fatal("work admitted after shutdown")
	}
}

func TestOpenCallerCancellationDoesNotWaitForHello(t *testing.T) {
	accepted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	s, _ := New("Bot fixture")
	s.gateway = "ws" + strings.TrimPrefix(server.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan error, 1)
	go func() { opened <- s.OpenContext(ctx) }()
	<-accepted
	cancel()
	select {
	case err := <-opened:
		if err == nil {
			t.Fatal("canceled open succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled open stuck")
	}
	_ = s.Shutdown()
}
